#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DEPUTAT — бот для отчётов, конкурсов и выплат."""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from functools import partial

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, InputMediaPhoto,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

# ══════════════════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════════════════

BOT_TOKEN   = os.environ.get("BOT_TOKEN", "8724153136:AAFhD24OvSoepxott4H-9WodBJAd-1rUh7U")
OWNER_IDS   = [6693142204, 5711452887]
DATA_FILE   = "data.json"
DATABASE_URL = os.environ.get("DATABASE_URL")

PAYMENTS = {"high": 350_000, "medium": 200_000}
MSK = timedelta(hours=3)

# ══════════════════════════════════════════════════════════
# СОСТОЯНИЯ (user_data["st"])
# ══════════════════════════════════════════════════════════
ST_REPORT_P1  = "rp1"
ST_REPORT_P2  = "rp2"
ST_REPORT_NICK = "rn"
ST_REPORT_CD  = "rcd"

ST_EV_NAME    = "en"
ST_EV_COUNT   = "eco"
ST_EV_PRIZE   = "epr"

ST_ESET_VAL   = "esv"

ST_NORMA_COUNT = "nc"

# ══════════════════════════════════════════════════════════
# ДАННЫЕ
# ══════════════════════════════════════════════════════════

DATA: dict = {
    "users": {},
    "reports": {},
    "report_counter": 1,
    "admins": set(),
    "active_cd": {"nick": None, "minute": None, "expires_at": None},
    "maintenance": {"active": False, "reason": ""},
    "event": {
        "active": False, "name": "", "class": "",
        "required": 0, "prize": 0, "created_at": None,
    },
    # ID рабочей группы (туда бот шлёт выговоры и кикает)
    "group_id": None,
    # Дневная норма строек
    "daily_norm": {"class": None, "count": 0},
    # Дневной лимит за оплачиваемые стройки (0 = без лимита)
    "daily_limit": {"high": 0, "medium": 0},
    # Дата последней вечерней проверки (чтобы не выдавать выговоры дважды)
    "last_check_date": None,
}

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════
# БАЗА ДАННЫХ  (всё через run_in_executor — не блокирует)
# ══════════════════════════════════════════════════════════

def _db_conn():
    try:
        return psycopg2.connect(DATABASE_URL, sslmode="require")
    except Exception:
        return psycopg2.connect(DATABASE_URL, sslmode="disable")


def _sync_init_db():
    conn = _db_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_data (
            id INTEGER PRIMARY KEY DEFAULT 1,
            payload JSONB NOT NULL
        )
    """)
    conn.commit(); cur.close(); conn.close()


def _sync_save(payload_str: str):
    conn = _db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO bot_data (id,payload) VALUES (1,%s) "
        "ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload",
        (payload_str,)
    )
    conn.commit(); cur.close(); conn.close()


def _sync_load() -> dict | None:
    conn = _db_conn()
    cur = conn.cursor()
    cur.execute("SELECT payload FROM bot_data WHERE id=1")
    row = cur.fetchone(); cur.close(); conn.close()
    if not row: return None
    return row[0] if isinstance(row[0], dict) else json.loads(row[0])


async def _run(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(fn, *args))


# ══════════════════════════════════════════════════════════
# СЕРИАЛИЗАЦИЯ
# ══════════════════════════════════════════════════════════

def _to_json() -> dict:
    reports = {}
    for rid, r in DATA["reports"].items():
        rc = dict(r)
        if isinstance(rc.get("at"), datetime):
            rc["at"] = rc["at"].isoformat()
        if "msg_ids" in rc:
            rc["msg_ids"] = {str(k): v for k, v in rc["msg_ids"].items()}
        reports[str(rid)] = rc

    ac = DATA["active_cd"]
    ev = DATA["event"]
    # Сериализация пользователей с datetime sleep_until
    users_out = {}
    for k, v in DATA["users"].items():
        u = dict(v)
        if isinstance(u.get("sleep_until"), datetime):
            u["sleep_until"] = u["sleep_until"].isoformat()
        users_out[str(k)] = u
    return {
        "users": users_out,
        "reports": reports,
        "report_counter": DATA["report_counter"],
        "admins": list(DATA["admins"]),
        "active_cd": {
            "nick": ac.get("nick"), "minute": ac.get("minute"),
            "expires_at": ac["expires_at"].isoformat() if ac.get("expires_at") else None,
        },
        "maintenance": DATA["maintenance"],
        "event": {
            "active": ev.get("active", False), "name": ev.get("name", ""),
            "class": ev.get("class", ""), "required": ev.get("required", 0),
            "prize": ev.get("prize", 0),
            "created_at": ev["created_at"].isoformat() if ev.get("created_at") else None,
        },
        "group_id": DATA.get("group_id"),
        "daily_norm": DATA.get("daily_norm", {"class": None, "count": 0}),
        "daily_limit": DATA.get("daily_limit", {"high": 0, "medium": 0}),
        "last_check_date": DATA.get("last_check_date"),
    }


def _from_json(saved: dict):
    DATA["users"] = {}
    for k, v in saved.get("users", {}).items():
        uid = int(k); v["id"] = int(v["id"]); v["telegram_id"] = int(v["telegram_id"])
        # Гарантируем новые поля (для совместимости со старыми данными)
        v.setdefault("warnings", 0)
        v.setdefault("sleep_until", None)
        v.setdefault("daily_count", {"high": 0, "medium": 0, "date": None})
        if isinstance(v.get("sleep_until"), str):
            try: v["sleep_until"] = datetime.fromisoformat(v["sleep_until"])
            except Exception: v["sleep_until"] = None
        DATA["users"][uid] = v

    DATA["reports"] = {}
    for rid, r in saved.get("reports", {}).items():
        r["id"] = int(r["id"]); r["user_id"] = int(r["user_id"])
        if r.get("at"):
            try: r["at"] = datetime.fromisoformat(r["at"])
            except Exception: r["at"] = datetime.now()
        if "msg_ids" in r:
            r["msg_ids"] = {int(k2): v2 for k2, v2 in r["msg_ids"].items()}
        DATA["reports"][int(rid)] = r

    DATA["report_counter"] = saved.get("report_counter", 1)
    DATA["admins"] = set(saved.get("admins", []))

    ac = saved.get("active_cd", {})
    DATA["active_cd"] = {
        "nick": ac.get("nick"), "minute": ac.get("minute"),
        "expires_at": datetime.fromisoformat(ac["expires_at"]) if ac.get("expires_at") else None,
    }
    DATA["maintenance"] = saved.get("maintenance", {"active": False, "reason": ""})

    ev = saved.get("event", {})
    DATA["event"] = {
        "active": ev.get("active", False), "name": ev.get("name", ""),
        "class": ev.get("class", ""), "required": ev.get("required", 0),
        "prize": ev.get("prize", 0),
        "created_at": datetime.fromisoformat(ev["created_at"]) if ev.get("created_at") else None,
    }

    DATA["group_id"] = saved.get("group_id")
    DATA["daily_norm"] = saved.get("daily_norm") or {"class": None, "count": 0}
    DATA["daily_limit"] = saved.get("daily_limit") or {"high": 0, "medium": 0}
    DATA["last_check_date"] = saved.get("last_check_date")


# ══════════════════════════════════════════════════════════
# SAVE / LOAD
# ══════════════════════════════════════════════════════════

async def save_data():
    try:
        payload = _to_json()
        if DATABASE_URL and psycopg2:
            await _run(_sync_save, json.dumps(payload, ensure_ascii=False))
        else:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения: {e}")


def load_data():
    try:
        if DATABASE_URL and psycopg2:
            _sync_init_db()
            saved = _sync_load()
            if not saved:
                logger.info("DB: нет данных, стартуем с нуля.")
                return
        else:
            if not os.path.exists(DATA_FILE): return
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
        _from_json(saved)
        logger.info(f"Загружено: {len(DATA['users'])} польз., {len(DATA['reports'])} отчётов.")
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")


# ══════════════════════════════════════════════════════════
# УТИЛИТЫ
# ══════════════════════════════════════════════════════════

def is_owner(uid: int) -> bool: return uid in OWNER_IDS
def is_admin(uid: int) -> bool: return uid in OWNER_IDS or uid in DATA["admins"]
def is_maintenance() -> bool: return DATA["maintenance"].get("active", False)

def all_privileged() -> list:
    return list(OWNER_IDS) + [u for u in DATA["admins"] if u not in OWNER_IDS]

def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def now_msk() -> datetime:
    return datetime.utcnow() + MSK

def cd_expiry(cd_min: int) -> datetime:
    now = now_msk()
    base = now.replace(second=0, microsecond=0)
    if base.minute < cd_min:
        return base.replace(minute=cd_min)
    nxt = (base + timedelta(hours=1)).replace(minute=cd_min)
    return nxt

def is_cd_active() -> bool:
    exp = DATA["active_cd"].get("expires_at")
    return bool(exp) and now_msk() < exp

def find_user_by_username(uname: str):
    uname = uname.lstrip("@").lower()
    for u in DATA["users"].values():
        if u.get("username") and u["username"].lower() == uname:
            return u
    return None

def resolve_user(arg: str):
    """Найти пользователя по @username или ID."""
    arg = arg.strip()
    if arg.startswith("@"):
        return find_user_by_username(arg[1:])
    try:
        tid = int(arg)
        return DATA["users"].get(tid)
    except ValueError:
        return find_user_by_username(arg)

def today_msk_str() -> str:
    return now_msk().strftime("%Y-%m-%d")

def is_sleeping(user: dict) -> bool:
    su = user.get("sleep_until")
    return bool(su) and isinstance(su, datetime) and now_msk() < su

def reset_daily_count_if_needed(user: dict):
    today = today_msk_str()
    dc = user.get("daily_count") or {"high": 0, "medium": 0, "date": None}
    if dc.get("date") != today:
        user["daily_count"] = {"high": 0, "medium": 0, "date": today}

def count_today_approved(uid: int, cls: str) -> int:
    """Сколько одобренных отчётов класса cls пользователь сделал за сегодня (МСК)."""
    today = now_msk().date()
    return sum(
        1 for r in DATA["reports"].values()
        if r["user_id"] == uid and r["status"] == "approved" and r["class"] == cls
        and isinstance(r.get("at"), datetime) and r["at"].date() == today
    )

def count_event_reports(uid: int) -> int:
    ev = DATA["event"]
    if not ev.get("active"): return 0
    cls = ev.get("class"); start = ev.get("created_at")
    return sum(
        1 for r in DATA["reports"].values()
        if r["user_id"] == uid and r["status"] == "approved"
        and r["class"] == cls
        and (start is None or (isinstance(r["at"], datetime) and r["at"] >= start))
    )

def main_kbd(uid: int) -> ReplyKeyboardMarkup:
    rows = [
        ["📝 Отправить отчет", "💰 Баланс"],
        ["📊 История строек", "⏰ КД", "📋 Последний отчет"],
    ]
    if DATA["event"].get("active"):
        rows.append(["🏆 Конкурс"])
    if is_owner(uid):
        rows.append(["👑 Панель владельца", "👥 Пользователи", "💸 Выплаты"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def event_text() -> str:
    ev = DATA["event"]
    cls = "Высокий" if ev.get("class") == "high" else "Средний"
    dt = ev["created_at"].strftime("%d.%m.%Y %H:%M") if ev.get("created_at") else "—"
    return (
        f"🏆 <b>КОНКУРС: {ev.get('name','—')}</b>\n\n"
        f"🏗 Класс: <b>{cls}</b>\n"
        f"🎯 Цель: <b>{ev.get('required',0)}</b> строек\n"
        f"💰 Приз: <b>{fmt(ev.get('prize',0))} ₽</b>\n"
        f"📅 Начат: {dt}"
    )

def st_clear(ctx: ContextTypes.DEFAULT_TYPE):
    for k in ("st","rp1","rp2","nick","cls","pay","cd","ev_name","ev_cls","ev_cnt","eset_field"):
        ctx.user_data.pop(k, None)

async def broadcast(ctx, text: str, skip: int = None):
    targets = [u for u in DATA["users"].values()
               if u["has_access"] and not u.get("blocked") and u["id"] != skip]
    tasks = []
    for u in targets:
        tasks.append(ctx.bot.send_message(
            u["id"], text, parse_mode=ParseMode.HTML,
            reply_markup=main_kbd(u["id"]),
        ))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception): logger.warning(f"broadcast err: {r}")

async def log_action(ctx, actor_id: int, text: str):
    actor = DATA["users"].get(actor_id)
    name = actor["full_name"] if actor else str(actor_id)
    role = "👑 Владелец" if is_owner(actor_id) else "👮 Админ"
    msg = f"📣 <b>{role} {name}:</b>\n{text}"
    tasks = []
    for uid in all_privileged():
        if uid != actor_id:
            tasks.append(ctx.bot.send_message(uid, msg, parse_mode=ParseMode.HTML))
    await asyncio.gather(*tasks, return_exceptions=True)


# ══════════════════════════════════════════════════════════
# КОНКУРС — завершение с победителем
# ══════════════════════════════════════════════════════════

async def end_event_win(ctx, winner_uid: int):
    ev = DATA["event"]
    winner = DATA["users"].get(winner_uid, {})
    ev_name = ev.get("name", "")
    prize = ev.get("prize", 0)
    cls_name = "Высокий" if ev.get("class") == "high" else "Средний"
    nick = winner.get("nick") or winner.get("full_name", str(winner_uid))

    DATA["users"][winner_uid]["balance"] += prize
    DATA["event"] = {"active": False, "name": "", "class": "",
                     "required": 0, "prize": 0, "created_at": None}
    await save_data()

    text = (
        f"🏆 <b>КОНКУРС ЗАВЕРШЁН!</b>\n\n"
        f"🎉 Победитель: <b>{nick}</b>\n"
        f"📌 Конкурс: <b>{ev_name}</b>\n"
        f"🏗 Класс: <b>{cls_name}</b>\n"
        f"💰 Приз: <b>{fmt(prize)} ₽</b> начислен!\n\n"
        f"🎊 Поздравляем победителя!"
    )
    await broadcast(ctx, text)


# ══════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    uid = u.id
    if is_owner(uid) and uid not in DATA["users"]:
        DATA["users"][uid] = {
            "id": uid, "telegram_id": uid, "username": u.username,
            "full_name": u.full_name, "has_access": True, "balance": 0,
            "total_reports": 0, "bank_account": None, "nick": None, "blocked": False,
        }
        await save_data()

    user = DATA["users"].get(uid)
    st_clear(ctx)

    if user and user["has_access"] and not user.get("blocked"):
        await update.message.reply_text(
            f"👋 Привет, <b>{user['full_name']}</b>!\n"
            f"💼 Баланс: <b>{fmt(user['balance'])} ₽</b>\n"
            f"🏗 Строек: <b>{user['total_reports']}</b>",
            reply_markup=main_kbd(uid), parse_mode=ParseMode.HTML,
        )
    elif user:
        await update.message.reply_text("⏳ Запрос на рассмотрении.")
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔑 Получить доступ", callback_data="req_access")
        ]])
        await update.message.reply_text(
            "🏗️ <b>DEPUTAT — Система отчётов</b>\n\nНажми кнопку ниже:",
            reply_markup=kb, parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
# ЕДИНЫЙ ОБРАБОТЧИК ВСЕХ CALLBACK-КНОПОК
# ══════════════════════════════════════════════════════════

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    uid = q.from_user.id

    # Отвечаем немедленно — убирает «часики» на кнопке
    try: await q.answer()
    except Exception: pass

    try:
        # ── Запрос доступа ──────────────────────────────
        if data == "req_access":
            u = q.from_user
            if is_maintenance() and not is_owner(uid):
                await q.message.reply_text(f"🔧 Техработы: {DATA['maintenance']['reason']}")
                return
            if uid in DATA["users"]:
                await q.message.reply_text("⏳ Запрос уже отправлен.")
                return
            DATA["users"][uid] = {
                "id": uid, "telegram_id": uid, "username": u.username,
                "full_name": u.full_name, "has_access": False, "balance": 0,
                "total_reports": 0, "bank_account": None, "nick": None, "blocked": False,
            }
            await save_data()
            tasks = []
            for oid in all_privileged():
                btn = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ Выдать доступ", callback_data=f"grant_{uid}")
                ]])
                tasks.append(ctx.bot.send_message(
                    oid,
                    f"🔔 <b>Запрос на доступ!</b>\n{u.full_name} | <code>{uid}</code>",
                    reply_markup=btn, parse_mode=ParseMode.HTML,
                ))
            await asyncio.gather(*tasks, return_exceptions=True)
            await q.message.reply_text("✅ Запрос отправлен!")
            return

        # ── Выдача доступа ──────────────────────────────
        if data.startswith("grant_"):
            if not is_admin(uid): return
            tid = int(data.split("_", 1)[1])
            user = DATA["users"].get(tid)
            if not user: return
            if user["has_access"]:
                await q.answer("⚠️ Доступ уже выдан!", show_alert=True)
                return
            user["has_access"] = True
            await save_data()
            await q.message.edit_text(
                f"✅ Доступ выдан <code>{tid}</code>",
                parse_mode=ParseMode.HTML,
            )
            try: await ctx.bot.send_message(
                tid, "✅ <b>Доступ одобрен!</b>\nВведите <b>номер счёта</b>:",
                parse_mode=ParseMode.HTML
            )
            except Exception: pass
            await log_action(ctx, uid, f"✅ Выдал доступ {user['full_name']} (<code>{tid}</code>)")
            return

        # ── Одобрение отчёта ────────────────────────────
        if data.startswith("appr_"):
            if not is_admin(uid): return
            rep_id = int(data.split("_", 1)[1])
            rep = DATA["reports"].get(rep_id)
            if not rep:
                await q.answer("❌ Отчёт не найден.", show_alert=True); return
            if rep["status"] == "approved":
                await q.answer("⚠️ Уже одобрен!", show_alert=True); return

            rep["status"] = "approved"; rep["approved_by"] = uid
            ruid = rep["user_id"]

            # ── ПРОВЕРКА ДНЕВНОГО ЛИМИТА ──
            cls = rep["class"]
            limit = DATA.get("daily_limit", {}).get(cls, 0)
            already = count_today_approved(ruid, cls)  # без текущего (этот ещё не учтён)
            limit_hit = limit > 0 and already >= limit
            actual_pay = 0 if limit_hit else rep["pay"]
            rep["pay"] = actual_pay  # сохраняем фактическую выплату

            DATA["users"][ruid]["balance"] += actual_pay
            DATA["users"][ruid]["total_reports"] += 1

            cls_name = "Высокий" if cls == "high" else "Средний"
            limit_note = ""
            if limit_hit:
                limit_note = f"\n⚠️ <b>Лимит превышен</b> ({limit}/день) — выплата 0 ₽"
            txt = (
                f"📋 <b>ОТЧЁТ #{rep_id}</b>\n"
                f"👤 {rep['nick']}\n🏗 {cls_name}\n💰 {fmt(actual_pay)} ₽\n"
                f"⏰ КД: мин. {rep['cd']}\n✅ <b>ОДОБРЕНО</b> — {q.from_user.full_name}{limit_note}"
            )
            for oid, ids in rep.get("msg_ids", {}).items():
                btn_id = ids.get("btn") if isinstance(ids, dict) else None
                if btn_id:
                    try:
                        await ctx.bot.edit_message_text(
                            txt, chat_id=oid, message_id=btn_id, parse_mode=ParseMode.HTML
                        )
                    except Exception: pass
            await save_data()
            user_msg = (
                f"🎉 Отчёт #{rep_id} одобрен! +{fmt(actual_pay)} ₽"
                if not limit_hit else
                f"⚠️ Отчёт #{rep_id} одобрен, но <b>дневной лимит ({limit}) превышен</b> — выплата 0 ₽."
            )
            try: await ctx.bot.send_message(ruid, user_msg, parse_mode=ParseMode.HTML)
            except Exception: pass
            await log_action(ctx, uid, f"✅ Одобрил #{rep_id} {rep['nick']} +{fmt(actual_pay)} ₽" + (" (лимит)" if limit_hit else ""))

            ev = DATA["event"]
            if ev.get("active") and rep["class"] == ev.get("class"):
                if count_event_reports(ruid) >= ev.get("required", 0):
                    await end_event_win(ctx, ruid)
            return

        # ── Управление пользователями ───────────────────
        if data.startswith("revoke_") or data.startswith("block_"):
            if not is_owner(uid): return
            action, tid = data.split("_", 1)[0], int(data.split("_", 1)[1])
            user = DATA["users"].get(tid)
            if not user: return
            if is_owner(tid):
                await q.answer("⚠️ Нельзя тронуть владельца.", show_alert=True); return
            full_name = user["full_name"]
            if action == "revoke":
                # Отозвать доступ — удаляем пользователя из бота
                DATA["users"].pop(tid, None)
                DATA["admins"].discard(tid)
                await save_data()
                await q.message.edit_text(q.message.text + "\n\n❌ Доступ отозван — удалён", parse_mode=ParseMode.HTML)
                try: await ctx.bot.send_message(tid, "❌ Ваш доступ отозван.")
                except Exception: pass
                await log_action(ctx, uid, f"❌ Отозвал доступ {full_name}")
            elif action == "block":
                # Кикнуть из группы и удалить
                gid = DATA.get("group_id")
                if gid:
                    try:
                        await ctx.bot.ban_chat_member(gid, tid)
                        await ctx.bot.unban_chat_member(gid, tid)
                    except Exception as e: logger.warning(f"kick group err: {e}")
                DATA["users"].pop(tid, None)
                DATA["admins"].discard(tid)
                await save_data()
                await q.message.edit_text(q.message.text + "\n\n🚫 Кикнут и удалён", parse_mode=ParseMode.HTML)
                try: await ctx.bot.send_message(tid, "🚫 Вы исключены.")
                except Exception: pass
                await log_action(ctx, uid, f"🚫 Кикнул {full_name}")
            return

        # ── Выплаты ─────────────────────────────────────
        if data == "pay_all":
            if not is_owner(uid): return
            names, count = [], 0
            for u in DATA["users"].values():
                if u["balance"] > 0:
                    names.append(f"{u['full_name']} ({fmt(u['balance'])} ₽)")
                    u["balance"] = 0; count += 1
                    try: await ctx.bot.send_message(u["id"], "✅ Зарплата выдана! Баланс обнулён.")
                    except Exception: pass
            await save_data()
            await q.message.edit_text(f"✅ ЗП выдана всем! {count} чел.", parse_mode=ParseMode.HTML)
            await log_action(ctx, uid, f"💸 ЗП всем ({count} чел.): {', '.join(names)}")
            return

        if data.startswith("pay_"):
            if not is_owner(uid): return
            tid = int(data.split("_", 1)[1])
            user = DATA["users"].get(tid)
            if user and user["balance"] > 0:
                paid = user["balance"]; user["balance"] = 0
                await save_data()
                await q.message.edit_text(q.message.text + "\n\n✅ <b>ЗП выдана!</b>", parse_mode=ParseMode.HTML)
                try: await ctx.bot.send_message(tid, "✅ Зарплата выдана! Баланс обнулён.")
                except Exception: pass
                await log_action(ctx, uid, f"💸 ЗП {user['full_name']}: {fmt(paid)} ₽")
            else:
                await q.answer("⚠️ Баланс уже 0.", show_alert=True)
            return

        # ── /estop подтверждение ─────────────────────────
        if data == "estop_yes":
            if not is_owner(uid): return
            ev_name = DATA["event"].get("name", "")
            DATA["event"] = {"active": False, "name": "", "class": "",
                             "required": 0, "prize": 0, "created_at": None}
            await save_data()
            await q.message.edit_text(f"🛑 Конкурс <b>{ev_name}</b> остановлен.", parse_mode=ParseMode.HTML)
            await broadcast(ctx, f"🛑 Конкурс <b>{ev_name}</b> остановлен.")
            await log_action(ctx, uid, f"🛑 Остановил конкурс: {ev_name}")
            return

        if data == "estop_no":
            await q.message.edit_text("❌ Отменено.")
            return

        # ── /vipeall подтверждение ───────────────────────
        if data == "vipeall_yes":
            if not is_owner(uid): return
            for u in DATA["users"].values():
                u["balance"] = 0; u["total_reports"] = 0
            DATA["reports"] = {}; DATA["report_counter"] = 1
            await save_data()
            await q.message.edit_text("✅ <b>Полный сброс выполнен!</b>", parse_mode=ParseMode.HTML)
            await log_action(ctx, uid, "🗑 Полный сброс /vipeall")
            return

        if data == "vipeall_no":
            await q.message.edit_text("❌ Отменено.")
            return

        # ── Отчёт: ник сохранённый ──────────────────────
        if data == "nick_saved":
            user = DATA["users"].get(uid)
            if not user or ctx.user_data.get("st") != ST_REPORT_NICK: return
            nick = user.get("nick", "")
            ctx.user_data["nick"] = nick
            ctx.user_data["st"] = None
            await q.message.edit_text(f"✅ Ник: <b>{nick}</b>", parse_mode=ParseMode.HTML)
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🏗 Высокий — 350 000 ₽", callback_data="rc_high"),
                InlineKeyboardButton("🏗 Средний — 200 000 ₽", callback_data="rc_medium"),
            ]])
            await q.message.reply_text("🏗 Шаг 4/5 — класс стройки:", reply_markup=kb)
            return

        # ── Отчёт: класс ────────────────────────────────
        if data in ("rc_high", "rc_medium"):
            user = DATA["users"].get(uid)
            if not user: return
            cls = "high" if data == "rc_high" else "medium"
            ctx.user_data["cls"] = cls
            ctx.user_data["pay"] = PAYMENTS[cls]
            cls_name = "Высокий" if cls == "high" else "Средний"
            await q.message.edit_text(
                f"✅ Класс: <b>{cls_name}</b> — {fmt(PAYMENTS[cls])} ₽",
                parse_mode=ParseMode.HTML
            )
            ctx.user_data["st"] = ST_REPORT_CD
            await q.message.reply_text("⏰ Шаг 5/5 — введите <b>минуту КД</b> (0–59):", parse_mode=ParseMode.HTML)
            return

        # ── Создание конкурса: класс ─────────────────────
        if data in ("evclass_high", "evclass_medium"):
            if not is_owner(uid): return
            cls = "high" if data == "evclass_high" else "medium"
            ctx.user_data["ev_cls"] = cls
            cls_name = "Высокий" if cls == "high" else "Средний"
            await q.message.edit_text(
                f"✅ Класс: <b>{cls_name}</b>\n\nШаг 3/4 — напишите <b>количество строек</b>:",
                parse_mode=ParseMode.HTML,
            )
            ctx.user_data["st"] = ST_EV_COUNT
            return

        # ── Редактирование конкурса: выбор поля ─────────
        if data.startswith("eset_"):
            if not is_owner(uid): return
            field = data[5:]
            if field == "cancel":
                ctx.user_data.pop("st", None); ctx.user_data.pop("eset_field", None)
                await q.message.edit_text("❌ Редактирование отменено.")
                return
            if field == "class":
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🏗 Высокий", callback_data="esetv_high"),
                    InlineKeyboardButton("🏗 Средний", callback_data="esetv_medium"),
                ]])
                await q.message.edit_text("Выберите новый класс:", reply_markup=kb)
                ctx.user_data["eset_field"] = "class"
                ctx.user_data["st"] = ST_ESET_VAL
            else:
                labels = {"name": "название", "count": "количество строек (число)", "prize": "приз (число)"}
                ctx.user_data["eset_field"] = field
                ctx.user_data["st"] = ST_ESET_VAL
                await q.message.edit_text(f"✏️ Введите новое {labels.get(field, 'значение')}:")
            return

        # ── /setnorma: выбор класса ──────────────────────
        if data in ("norma_high", "norma_medium"):
            if not is_owner(uid): return
            cls = "high" if data == "norma_high" else "medium"
            ctx.user_data["norma_cls"] = cls
            ctx.user_data["st"] = ST_NORMA_COUNT
            cn = "Высокий" if cls == "high" else "Средний"
            await q.message.edit_text(
                f"✅ Класс: <b>{cn}</b>\n\nСколько строек должно быть в день?",
                parse_mode=ParseMode.HTML,
            )
            return

        if data == "norma_reset":
            if not is_owner(uid): return
            DATA["daily_norm"] = {"class": None, "count": 0}
            await save_data()
            await q.message.edit_text("🗑 Дневная норма сброшена.")
            await log_action(ctx, uid, "🗑 Сбросил дневную норму")
            return

        # ── Редактирование конкурса: значение класса ─────
        if data in ("esetv_high", "esetv_medium"):
            if not is_owner(uid): return
            cls = "high" if data == "esetv_high" else "medium"
            DATA["event"]["class"] = cls
            await save_data()
            cls_name = "Высокий" if cls == "high" else "Средний"
            ctx.user_data.pop("st", None); ctx.user_data.pop("eset_field", None)
            await q.message.edit_text(f"✅ Класс изменён: <b>{cls_name}</b>", parse_mode=ParseMode.HTML)
            return

    except Exception as e:
        logger.error(f"on_callback error [{data}]: {e}", exc_info=True)
        try: await q.answer("❌ Ошибка. Попробуйте ещё раз.", show_alert=True)
        except Exception: pass


# ══════════════════════════════════════════════════════════
# ЕДИНЫЙ ОБРАБОТЧИК ВСЕХ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════

async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg: return
    chat = update.effective_chat
    # В группах бот реагирует ТОЛЬКО на команды (а они идут через CommandHandler)
    if chat and chat.type in ("group", "supergroup"):
        return
    uid = update.effective_user.id
    user = DATA["users"].get(uid)
    st = ctx.user_data.get("st")

    if is_maintenance() and not is_owner(uid):
        await msg.reply_text(f"🔧 Техработы: {DATA['maintenance']['reason']}")
        return

    # ── ФАЗА ОТЧЁТА: фото 1 ─────────────────────────────
    if st == ST_REPORT_P1:
        if not msg.photo:
            await msg.reply_text("❗ Отправьте фото (не файл).")
            return
        ctx.user_data["rp1"] = msg.photo[-1].file_id
        ctx.user_data["st"] = ST_REPORT_P2
        await msg.reply_text("📸 Шаг 2/5 — фото <b>ОКОНЧАНИЯ</b> стройки:", parse_mode=ParseMode.HTML)
        return

    # ── ФАЗА ОТЧЁТА: фото 2 ─────────────────────────────
    if st == ST_REPORT_P2:
        if not msg.photo:
            await msg.reply_text("❗ Отправьте фото (не файл).")
            return
        ctx.user_data["rp2"] = msg.photo[-1].file_id
        ctx.user_data["st"] = ST_REPORT_NICK
        saved_nick = user.get("nick") if user else None
        if saved_nick:
            ctx.user_data["nick"] = saved_nick
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ {saved_nick}", callback_data="nick_saved")
            ]])
            await msg.reply_text(
                f"👤 Шаг 3/5 — ваш ник: <b>{saved_nick}</b>\n"
                "Нажмите кнопку или напишите другой:",
                reply_markup=kb, parse_mode=ParseMode.HTML,
            )
        else:
            await msg.reply_text("👤 Шаг 3/5 — напишите ваш <b>ник</b>:", parse_mode=ParseMode.HTML)
        return

    # ── ФАЗА ОТЧЁТА: ник текстом ────────────────────────
    if st == ST_REPORT_NICK and msg.text:
        nick = msg.text.strip()
        ctx.user_data["nick"] = nick
        ctx.user_data["st"] = None
        if user: user["nick"] = nick; await save_data()
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🏗 Высокий — 350 000 ₽", callback_data="rc_high"),
            InlineKeyboardButton("🏗 Средний — 200 000 ₽", callback_data="rc_medium"),
        ]])
        await msg.reply_text(
            f"✅ Ник: <b>{nick}</b>\n\n🏗 Шаг 4/5 — класс стройки:",
            reply_markup=kb, parse_mode=ParseMode.HTML,
        )
        return

    # ── ФАЗА ОТЧЁТА: КД ─────────────────────────────────
    if st == ST_REPORT_CD and msg.text:
        try:
            cd_min = int(msg.text.strip())
            if not 0 <= cd_min <= 59: raise ValueError
        except ValueError:
            await msg.reply_text("❌ Число от 0 до 59:")
            return

        if not user:
            await msg.reply_text("❌ Нет доступа.")
            ctx.user_data.clear(); return

        expires_at = cd_expiry(cd_min)
        nick = ctx.user_data.get("nick", user.get("nick", user["full_name"]))
        DATA["active_cd"] = {"nick": nick, "minute": cd_min, "expires_at": expires_at}

        rep_id = DATA["report_counter"]
        DATA["reports"][rep_id] = {
            "id": rep_id, "user_id": uid, "nick": nick,
            "class": ctx.user_data["cls"], "pay": ctx.user_data["pay"],
            "cd": cd_min, "p1": ctx.user_data["rp1"], "p2": ctx.user_data["rp2"],
            "status": "pending", "at": now_msk(), "msg_ids": {},
        }
        DATA["report_counter"] += 1

        cls_name = "Высокий" if ctx.user_data["cls"] == "high" else "Средний"
        txt = (
            f"📋 <b>ОТЧЁТ #{rep_id}</b>\n"
            f"👤 {nick}\n🏗 {cls_name}\n💰 {fmt(ctx.user_data['pay'])} ₽\n"
            f"💳 {user.get('bank_account') or '—'}\n"
            f"⏰ КД до {expires_at.strftime('%H:%M')} МСК (мин. {cd_min})"
        )
        btn = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ ОДОБРИТЬ", callback_data=f"appr_{rep_id}")
        ]])
        media = [
            InputMediaPhoto(ctx.user_data["rp1"], caption="📸 Начало"),
            InputMediaPhoto(ctx.user_data["rp2"], caption="📸 Конец"),
        ]
        for oid in all_privileged():
            try:
                sm = await ctx.bot.send_media_group(oid, media)
                sb = await ctx.bot.send_message(oid, txt, reply_markup=btn, parse_mode=ParseMode.HTML)
                DATA["reports"][rep_id]["msg_ids"][oid] = {
                    "media": [m.message_id for m in sm], "btn": sb.message_id
                }
            except Exception as e:
                logger.warning(f"Ошибка отправки отчёта {oid}: {e}")

        await save_data()
        await msg.reply_text(
            f"✅ Отчёт #{rep_id} отправлен!\n⏰ КД до <b>{expires_at.strftime('%H:%M')} МСК</b>",
            reply_markup=main_kbd(uid), parse_mode=ParseMode.HTML,
        )
        st_clear(ctx)
        return

    # ── СОЗДАНИЕ КОНКУРСА: название ──────────────────────
    if st == ST_EV_NAME and msg.text:
        ctx.user_data["ev_name"] = msg.text.strip()
        ctx.user_data["st"] = None
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🏗 Высокий", callback_data="evclass_high"),
            InlineKeyboardButton("🏗 Средний", callback_data="evclass_medium"),
        ]])
        await msg.reply_text(
            f"✅ Название: <b>{ctx.user_data['ev_name']}</b>\n\nШаг 2/4 — класс строек:",
            reply_markup=kb, parse_mode=ParseMode.HTML,
        )
        return

    # ── СОЗДАНИЕ КОНКУРСА: количество ───────────────────
    if st == ST_EV_COUNT and msg.text:
        try:
            cnt = int(msg.text.strip())
            if cnt <= 0: raise ValueError
        except ValueError:
            await msg.reply_text("❌ Введите положительное число:"); return
        ctx.user_data["ev_cnt"] = cnt
        ctx.user_data["st"] = ST_EV_PRIZE
        await msg.reply_text(
            f"✅ Цель: <b>{cnt} строек</b>\n\nШаг 4/4 — напишите <b>сумму приза</b> (₽):",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── СОЗДАНИЕ КОНКУРСА: приз ──────────────────────────
    if st == ST_EV_PRIZE and msg.text:
        try:
            prize = int(msg.text.strip())
            if prize <= 0: raise ValueError
        except ValueError:
            await msg.reply_text("❌ Введите положительное число:"); return

        ev_name = ctx.user_data.get("ev_name", "")
        ev_cls  = ctx.user_data.get("ev_cls", "high")
        ev_cnt  = ctx.user_data.get("ev_cnt", 1)
        cls_name = "Высокий" if ev_cls == "high" else "Средний"

        DATA["event"] = {
            "active": True, "name": ev_name, "class": ev_cls,
            "required": ev_cnt, "prize": prize, "created_at": now_msk(),
        }
        await save_data()

        announce = (
            f"🏆 <b>НАЧАЛСЯ КОНКУРС!</b>\n\n"
            f"📌 <b>{ev_name}</b>\n🏗 Класс: <b>{cls_name}</b>\n"
            f"🎯 Цель: <b>{ev_cnt}</b> строек\n💰 Приз: <b>{fmt(prize)} ₽</b>\n\n"
            f"Нажми 🏆 Конкурс для слежения!"
        )
        st_clear(ctx)
        await msg.reply_text(f"✅ Конкурс <b>{ev_name}</b> создан!", reply_markup=main_kbd(uid), parse_mode=ParseMode.HTML)
        await broadcast(ctx, announce)
        return

    # ── /setnorma: ввод количества ──────────────────────
    if st == ST_NORMA_COUNT and msg.text:
        if not is_owner(uid): return
        try:
            cnt = int(msg.text.strip())
            if cnt <= 0: raise ValueError
        except ValueError:
            await msg.reply_text("❌ Введите положительное число:"); return
        cls = ctx.user_data.get("norma_cls", "high")
        DATA["daily_norm"] = {"class": cls, "count": cnt}
        await save_data()
        ctx.user_data.pop("st", None); ctx.user_data.pop("norma_cls", None)
        cn = "Высокий" if cls == "high" else "Средний"
        await msg.reply_text(
            f"✅ Норма установлена: <b>{cn} — {cnt} строек/день</b>",
            parse_mode=ParseMode.HTML,
        )
        await broadcast(ctx,
            f"📊 <b>НОВАЯ ДНЕВНАЯ НОРМА!</b>\n\n"
            f"🏗 Класс: <b>{cn}</b>\n🎯 Цель: <b>{cnt} строек/день</b>\n\n"
            f"⚠️ Кто не выполнит — получит выговор. 5 выговоров = кик."
        )
        await log_action(ctx, uid, f"📊 Норма: {cn} {cnt}/день")
        return

    # ── РЕДАКТИРОВАНИЕ КОНКУРСА: текстовое значение ─────
    if st == ST_ESET_VAL and msg.text:
        field = ctx.user_data.get("eset_field", "")
        text = msg.text.strip()
        if field == "name":
            DATA["event"]["name"] = text
            await save_data()
            await msg.reply_text(f"✅ Название: <b>{text}</b>", parse_mode=ParseMode.HTML)
        elif field == "count":
            try:
                cnt = int(text)
                if cnt <= 0: raise ValueError
            except ValueError:
                await msg.reply_text("❌ Введите положительное число:"); return
            DATA["event"]["required"] = cnt
            await save_data()
            await msg.reply_text(f"✅ Цель: <b>{cnt} строек</b>", parse_mode=ParseMode.HTML)
        elif field == "prize":
            try:
                prize = int(text)
                if prize <= 0: raise ValueError
            except ValueError:
                await msg.reply_text("❌ Введите положительное число:"); return
            DATA["event"]["prize"] = prize
            await save_data()
            await msg.reply_text(f"✅ Приз: <b>{fmt(prize)} ₽</b>", parse_mode=ParseMode.HTML)
        ctx.user_data.pop("st", None); ctx.user_data.pop("eset_field", None)
        return

    # ── Сохранение номера счёта (первый текст от нового пользователя) ─
    if user and user["has_access"] and user.get("bank_account") is None and msg.text and msg.text.strip().isdigit():
        user["bank_account"] = msg.text.strip()
        await save_data()
        await msg.reply_text(
            f"✅ Счёт <b>{user['bank_account']}</b> сохранён!",
            reply_markup=main_kbd(uid), parse_mode=ParseMode.HTML,
        )
        return

    # ── Кнопки главного меню ─────────────────────────────
    if not user or not user["has_access"] or user.get("blocked"): return
    t = msg.text or ""

    if t == "📝 Отправить отчет":
        st_clear(ctx)
        ctx.user_data["st"] = ST_REPORT_P1
        await msg.reply_text(
            "📸 Шаг 1/5 — фото <b>НАЧАЛА</b> стройки:",
            reply_markup=ReplyKeyboardRemove(), parse_mode=ParseMode.HTML,
        )

    elif t == "💰 Баланс":
        await msg.reply_text(
            f"💼 Баланс: <b>{fmt(user['balance'])} ₽</b>", parse_mode=ParseMode.HTML
        )

    elif t == "⏰ КД":
        if is_cd_active():
            cd = DATA["active_cd"]
            await msg.reply_text(
                f"⏰ КД до <b>{cd['expires_at'].strftime('%H:%M')} МСК</b> "
                f"(мин. {cd['minute']})\nПоставил: <b>{cd['nick']}</b>",
                parse_mode=ParseMode.HTML,
            )
        else:
            await msg.reply_text("✅ КД не активен.")

    elif t == "📋 Последний отчет":
        reps = [r for r in DATA["reports"].values() if r["user_id"] == uid]
        if not reps:
            await msg.reply_text("❌ Отчётов ещё нет."); return
        last = sorted(reps, key=lambda r: r["id"])[-1]
        st_label = "✅ Одобрен" if last["status"] == "approved" else "⏳ На рассмотрении"
        dt = last["at"].strftime("%d.%m.%Y %H:%M") if isinstance(last["at"], datetime) else "—"
        cls = "Высокий" if last["class"] == "high" else "Средний"
        await msg.reply_text(
            f"📋 <b>Отчёт #{last['id']}</b>\n"
            f"👤 {last['nick']}\n🏗 {cls}\n💰 {fmt(last['pay'])} ₽\n"
            f"📅 {dt}\n📶 {st_label}",
            parse_mode=ParseMode.HTML,
        )

    elif t == "📊 История строек":
        reps = sorted([r for r in DATA["reports"].values() if r["user_id"] == uid],
                      key=lambda r: r["id"])[-5:]
        if not reps:
            await msg.reply_text("📊 У вас нет отчётов."); return
        text = "📊 <b>Последние отчёты:</b>\n\n"
        for r in reps:
            s = "✅" if r["status"] == "approved" else "⏳"
            d = r["at"].strftime("%d.%m %H:%M") if isinstance(r["at"], datetime) else "—"
            text += f"{s} #{r['id']} — {fmt(r['pay'])} ₽ ({d})\n"
        await msg.reply_text(text, parse_mode=ParseMode.HTML)

    elif t == "🏆 Конкурс":
        ev = DATA["event"]
        if not ev.get("active"):
            await msg.reply_text("🏆 Нет активного конкурса."); return
        req = ev.get("required", 0)
        uc = count_event_reports(uid)
        lb = sorted(
            [(u.get("nick") or u["full_name"], count_event_reports(u["id"]))
             for u in DATA["users"].values() if u["has_access"] and not u.get("blocked")],
            key=lambda x: x[1], reverse=True,
        )
        text = f"{event_text()}\n\n📊 Ваш прогресс: <b>{uc}/{req}</b>\n\n🏅 <b>Топ участников:</b>\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, (name, cnt) in enumerate(lb[:10]):
            m = medals[i] if i < 3 else f"{i+1}."
            text += f"{m} {name} — <b>{cnt}/{req}</b>\n"
        await msg.reply_text(text, parse_mode=ParseMode.HTML)

    elif t == "👑 Панель владельца" and is_owner(uid):
        total_pay = sum(u["balance"] for u in DATA["users"].values())
        active = sum(1 for u in DATA["users"].values() if u["has_access"] and not u.get("blocked"))
        maint = "🔧 ВКЛ" if is_maintenance() else "✅ ВЫКЛ"
        ev = DATA["event"]
        ev_info = f"🏆 <b>{ev['name']}</b> (активен)" if ev.get("active") else "🏆 Конкурса нет"
        await msg.reply_text(
            f"👑 <b>ПАНЕЛЬ</b>\n👥 {len(DATA['users'])} польз. | ✅ {active} актив.\n"
            f"💰 К выплате: {fmt(total_pay)} ₽\n🔧 {maint}\n{ev_info}",
            parse_mode=ParseMode.HTML,
        )

    elif t == "👥 Пользователи" and is_owner(uid):
        await show_users(update, ctx)

    elif t == "💸 Выплаты" and is_owner(uid):
        await show_payroll(update, ctx)


# ══════════════════════════════════════════════════════════
# СПИСОК ПОЛЬЗОВАТЕЛЕЙ И ВЫПЛАТЫ
# ══════════════════════════════════════════════════════════

async def show_users(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Только активные с доступом, без заблокированных
    users = [u for u in DATA["users"].values()
             if u["has_access"] and not u.get("blocked") and not is_owner(u["id"])]
    if not users:
        await update.message.reply_text("👥 Активных пользователей нет."); return
    for u in users:
        role = "👮 Админ" if u["id"] in DATA["admins"] else "👤"
        nick = f" ({u['nick']})" if u.get("nick") else ""
        warn = u.get("warnings", 0)
        warn_str = f" | ⚠️ {warn}/5" if warn else ""
        sleep_str = ""
        if is_sleeping(u):
            sleep_str = f" | 💤 до {u['sleep_until'].strftime('%d.%m')}"
        text = (
            f"{role} <b>{u['full_name']}</b>{nick}\n"
            f"🆔 <code>{u['id']}</code>{warn_str}{sleep_str}\n"
            f"💳 {u.get('bank_account') or '—'} | 💰 {fmt(u['balance'])} ₽"
        )
        btns = [[
            InlineKeyboardButton("❌ Отозвать", callback_data=f"revoke_{u['id']}"),
            InlineKeyboardButton("🚫 Кикнуть",  callback_data=f"block_{u['id']}"),
        ]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)


async def show_payroll(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    workers = [u for u in DATA["users"].values() if u["balance"] > 0]
    if not workers:
        await update.message.reply_text("💸 Нет баланса к выплате."); return
    total = sum(u["balance"] for u in workers)
    await update.message.reply_text(
        f"💸 <b>ВЫПЛАТЫ</b>\n{len(workers)} чел. | <b>{fmt(total)} ₽</b>",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Выдать ВСЕМ ({fmt(total)} ₽)", callback_data="pay_all")
        ]]),
        parse_mode=ParseMode.HTML,
    )
    for u in workers:
        nick = f" ({u['nick']})" if u.get("nick") else ""
        await update.message.reply_text(
            f"👤 <b>{u['full_name']}</b>{nick}\n💳 {u.get('bank_account') or '—'}\n"
            f"💰 <b>{fmt(u['balance'])} ₽</b>",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ ЗП выдана", callback_data=f"pay_{u['id']}")
            ]]),
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
# КОМАНДЫ
# ══════════════════════════════════════════════════════════

async def cmd_event(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if DATA["event"].get("active"):
        await update.message.reply_text("⚠️ Конкурс уже идёт. Сначала /estop"); return
    st_clear(ctx)
    ctx.user_data["st"] = ST_EV_NAME
    await update.message.reply_text(
        "🏆 <b>Создание конкурса</b>\n\nШаг 1/4 — введите <b>название</b>:",
        parse_mode=ParseMode.HTML,
    )


async def cmd_estop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not DATA["event"].get("active"):
        await update.message.reply_text("⚠️ Нет активного конкурса."); return
    await update.message.reply_text(
        f"⚠️ Остановить конкурс <b>{DATA['event']['name']}</b>?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да", callback_data="estop_yes"),
            InlineKeyboardButton("❌ Нет", callback_data="estop_no"),
        ]]),
        parse_mode=ParseMode.HTML,
    )


async def cmd_egive(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not DATA["event"].get("active"):
        await update.message.reply_text("⚠️ Нет активного конкурса."); return
    if not ctx.args:
        await update.message.reply_text("❌ /egive [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Пользователь не найден."); return
    tid = user["id"]
    await update.message.reply_text(f"🏆 Выдаём приз {user['full_name']}...")
    await end_event_win(ctx, tid)
    await log_action(ctx, uid, f"🏆 Ручная победа: {user['full_name']} (<code>{tid}</code>)")


async def cmd_eset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not DATA["event"].get("active"):
        await update.message.reply_text("⚠️ Нет активного конкурса."); return
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Название",          callback_data="eset_name")],
        [InlineKeyboardButton("🏗 Класс",             callback_data="eset_class")],
        [InlineKeyboardButton("🎯 Количество строек", callback_data="eset_count")],
        [InlineKeyboardButton("💰 Приз",              callback_data="eset_prize")],
        [InlineKeyboardButton("❌ Отмена",            callback_data="eset_cancel")],
    ])
    await update.message.reply_text(
        f"⚙️ <b>Редактировать конкурс</b>\n\n{event_text()}\n\nЧто изменить?",
        reply_markup=kb, parse_mode=ParseMode.HTML,
    )


async def cmd_vipeall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    await update.message.reply_text(
        "⚠️ <b>ПОЛНЫЙ СБРОС!</b>\nБудет обнулено: все строки, балансы, отчёты.\nПользователи сохранятся. Уверены?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, сбросить", callback_data="vipeall_yes"),
            InlineKeyboardButton("❌ Отмена",       callback_data="vipeall_no"),
        ]]),
        parse_mode=ParseMode.HTML,
    )


async def cmd_givebonus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text("❌ /givebonus [@username или ID] [сумма] [причина]"); return
    user = resolve_user(args[0])
    try: amount = int(args[1]); reason = " ".join(args[2:])
    except ValueError:
        await update.message.reply_text("❌ Сумма — число."); return
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    user["balance"] += amount; await save_data()
    await update.message.reply_text(
        f"✅ +{fmt(amount)} ₽ → {user['full_name']}\nБаланс: {fmt(user['balance'])} ₽",
        parse_mode=ParseMode.HTML,
    )
    try: await ctx.bot.send_message(tid, f"🎁 Бонус <b>+{fmt(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(ctx, update.effective_user.id, f"🎁 Бонус {user['full_name']} +{fmt(amount)} ₽. {reason}")


async def cmd_takecash(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = ctx.args
    if len(args) < 3:
        await update.message.reply_text("❌ /takecash [@username или ID] [сумма] [причина]"); return
    user = resolve_user(args[0])
    try: amount = int(args[1]); reason = " ".join(args[2:])
    except ValueError:
        await update.message.reply_text("❌ Сумма — число."); return
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    user["balance"] = max(0, user["balance"] - amount); await save_data()
    await update.message.reply_text(f"✅ Снято {fmt(amount)} ₽ у {user['full_name']}", parse_mode=ParseMode.HTML)
    try: await ctx.bot.send_message(tid, f"⚠️ Снято <b>{fmt(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(ctx, update.effective_user.id, f"➖ {fmt(amount)} ₽ {user['full_name']}. {reason}")


async def cmd_makeadmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("❌ /makeadmin [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    if is_owner(tid):
        await update.message.reply_text("⚠️ Это владелец."); return
    DATA["admins"].add(tid); user["has_access"] = True; await save_data()
    await update.message.reply_text(f"✅ {user['full_name']} — администратор.")
    try: await ctx.bot.send_message(tid, "👮 Вам выданы права <b>администратора</b>!", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(ctx, update.effective_user.id, f"👮 Назначил admin {user['full_name']}")


async def cmd_takeadmin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not ctx.args:
        await update.message.reply_text("❌ /takeadmin [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    if tid in DATA["admins"]:
        DATA["admins"].discard(tid); await save_data()
        name = user["full_name"]
        await update.message.reply_text(f"✅ Права сняты с {name}.")
        try: await ctx.bot.send_message(tid, "⚠️ Права администратора сняты.")
        except Exception: pass
        await log_action(ctx, update.effective_user.id, f"🔻 Снял admin {name}")
    else:
        await update.message.reply_text("⚠️ Не является администратором.")


async def cmd_giveds(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        await update.message.reply_text("❌ /giveds [username или ID]"); return
    arg = ctx.args[0]
    if arg.lstrip("@").lstrip("-").isdigit():
        tid = int(arg.lstrip("@"))
        user = DATA["users"].get(tid)
    else:
        user = find_user_by_username(arg)
        tid = user["id"] if user else None
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    if user["has_access"]:
        await update.message.reply_text(f"⚠️ У {user['full_name']} уже есть доступ."); return
    user["has_access"] = True; user["blocked"] = False; await save_data()
    await update.message.reply_text(f"✅ Доступ выдан: {user['full_name']}", parse_mode=ParseMode.HTML)
    try: await ctx.bot.send_message(tid, "✅ Доступ выдан!\nВведите номер счёта:")
    except Exception: pass
    await log_action(ctx, update.effective_user.id, f"✅ Доступ {user['full_name']}")


async def cmd_texwork(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    reason = " ".join(ctx.args) if ctx.args else "Технические работы"
    DATA["maintenance"] = {"active": True, "reason": reason}; await save_data()
    await update.message.reply_text(f"🔧 Техработы включены: {reason}")
    await log_action(ctx, update.effective_user.id, f"🔧 Техработы: {reason}")


async def cmd_on(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    DATA["maintenance"] = {"active": False, "reason": ""}; await save_data()
    await update.message.reply_text("✅ Бот включён.")
    await log_action(ctx, update.effective_user.id, "✅ Техработы выключены.")


async def cmd_sozvat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = DATA["users"].get(uid)
    if not user or not user["has_access"] or user.get("blocked"):
        await update.message.reply_text("❌ Нет доступа."); return
    nick = user.get("nick") or user["full_name"]
    cnt = 0
    tasks = []
    for u in DATA["users"].values():
        if u["has_access"] and not u.get("blocked") and u["id"] != uid:
            tasks.append(ctx.bot.send_message(u["id"], f"🔔 <b>Нужна помощь!</b>\nОтправил: <b>{nick}</b>", parse_mode=ParseMode.HTML))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    cnt = sum(1 for r in results if not isinstance(r, Exception))
    await update.message.reply_text(f"✅ Оповещение отправлено {cnt} пользователям.")


async def cmd_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        await update.message.reply_text("❌ /msg [текст]"); return
    sender = DATA["users"].get(update.effective_user.id)
    nick = (sender.get("nick") or sender["full_name"]) if sender else update.effective_user.full_name
    text = " ".join(ctx.args)
    tasks = []
    for u in DATA["users"].values():
        if u["has_access"] and not u.get("blocked"):
            tasks.append(ctx.bot.send_message(u["id"], f"📢 {text}\n\n— <i>{nick}</i>", parse_mode=ParseMode.HTML))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text(f"✅ Отправлено {sum(1 for r in results if not isinstance(r, Exception))} польз.")


async def cmd_bank(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    user = DATA["users"].get(uid)
    if not user or not user["has_access"] or user.get("blocked"):
        await update.message.reply_text("❌ Нет доступа."); return
    if not ctx.args:
        await update.message.reply_text(f"💳 Счёт: <b>{user.get('bank_account') or 'не указан'}</b>\nИзменить: /bank [номер]", parse_mode=ParseMode.HTML)
        return
    user["bank_account"] = " ".join(ctx.args); await save_data()
    await update.message.reply_text(f"✅ Счёт: <b>{user['bank_account']}</b>", parse_mode=ParseMode.HTML)


async def cmd_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await update.message.reply_text(
        f"🆔 ID: <code>{u.id}</code>\n👤 @{u.username or '—'}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    approved = sum(1 for r in DATA["reports"].values() if r["status"] == "approved")
    pending  = sum(1 for r in DATA["reports"].values() if r["status"] == "pending")
    bal_sum  = sum(u["balance"] for u in DATA["users"].values())
    maint    = f"🔧 {DATA['maintenance']['reason']}" if is_maintenance() else "✅ ВЫКЛ"
    await update.message.reply_text(
        f"📊 <b>СТАТИСТИКА</b>\n\n"
        f"👥 Польз.: <b>{len(DATA['users'])}</b> | 👮 Админов: <b>{len(DATA['admins'])}</b>\n"
        f"📋 Отчётов: <b>{DATA['report_counter']-1}</b> (✅{approved} / ⏳{pending})\n"
        f"💰 К выплате: <b>{fmt(bal_sum)} ₽</b>\n🔧 {maint}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_userinfo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        await update.message.reply_text("❌ /userinfo [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    role = "👑 Владелец" if is_owner(tid) else ("👮 Админ" if tid in DATA["admins"] else "👤")
    status = "🚫 Заблок." if user.get("blocked") else ("✅" if user["has_access"] else "⏳")
    uname = f"@{user['username']}" if user.get("username") else "—"
    ev = DATA["event"]
    ev_txt = f"\n🏆 Конкурс: <b>{count_event_reports(tid)}/{ev.get('required',0)}</b>" if ev.get("active") else ""
    await update.message.reply_text(
        f"👤 <b>{user['full_name']}</b> {uname}\n🆔 <code>{tid}</code>\n"
        f"{role} | {status}\n"
        f"🏷 Ник: {user.get('nick') or '—'}\n"
        f"💳 {user.get('bank_account') or '—'}\n"
        f"💰 <b>{fmt(user['balance'])} ₽</b> | 📋 <b>{user['total_reports']}</b> строек{ev_txt}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_checkid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        await update.message.reply_text("❌ /checkid [username]"); return
    user = find_user_by_username(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    role = "👑" if is_owner(user["id"]) else ("👮" if user["id"] in DATA["admins"] else "👤")
    await update.message.reply_text(
        f"🔎 {role} <b>{user['full_name']}</b>\n🆔 <code>{user['id']}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_listadmins(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    lines = ["👑 <b>ВЛАДЕЛЬЦЫ:</b>"]
    for oid in OWNER_IDS:
        u = DATA["users"].get(oid)
        lines.append(f"  • {u['full_name'] if u else oid} (<code>{oid}</code>)")
    lines.append("\n👮 <b>АДМИНИСТРАТОРЫ:</b>")
    if DATA["admins"]:
        for aid in DATA["admins"]:
            u = DATA["users"].get(aid)
            lines.append(f"  • {u['full_name'] if u else aid} (<code>{aid}</code>)")
    else:
        lines.append("  — нет")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_otcets(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        await update.message.reply_text("❌ /otcets [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    reps = [r for r in DATA["reports"].values() if r["user_id"] == tid]
    if not reps:
        await update.message.reply_text(f"📋 У {user['full_name']} нет отчётов."); return
    await update.message.reply_text(f"📋 <b>{user['full_name']}</b> — {len(reps)} отчётов:", parse_mode=ParseMode.HTML)
    for r in reps:
        s = "✅" if r["status"] == "approved" else "⏳"
        d = r["at"].strftime("%d.%m.%Y %H:%M") if isinstance(r["at"], datetime) else "—"
        cls = "Высокий" if r["class"] == "high" else "Средний"
        await update.message.reply_text(
            f"{s} <b>#{r['id']}</b> | {cls} | {fmt(r['pay'])} ₽ | {d}",
            parse_mode=ParseMode.HTML,
        )


# ══════════════════════════════════════════════════════════
# НОВЫЕ КОМАНДЫ
# ══════════════════════════════════════════════════════════

async def cmd_changekd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Изменить минуту КД: /changekd [минута 0-59]"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not ctx.args:
        cd = DATA["active_cd"]
        if is_cd_active():
            await update.message.reply_text(
                f"⏰ Текущее КД: мин. <b>{cd['minute']}</b> | до <b>{cd['expires_at'].strftime('%H:%M')} МСК</b>\n"
                f"Поставил: <b>{cd.get('nick','—')}</b>\n\n"
                f"Изменить: /changekd [минута 0-59]",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text("✅ КД не активен.\nУстановить: /changekd [минута 0-59]")
        return
    try:
        minute = int(ctx.args[0])
        if not 0 <= minute <= 59: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Минута — число от 0 до 59."); return

    now = now_msk()
    base = now.replace(second=0, microsecond=0)
    if base.minute < minute:
        expires_at = base.replace(minute=minute)
    else:
        expires_at = (base + timedelta(hours=1)).replace(minute=minute)

    user = DATA["users"].get(uid)
    nick = user.get("nick") or user.get("full_name", str(uid)) if user else str(uid)
    old_cd = DATA["active_cd"].copy()
    DATA["active_cd"] = {"nick": nick, "minute": minute, "expires_at": expires_at}
    await save_data()

    await update.message.reply_text(
        f"✅ КД изменено!\n⏰ Теперь: мин. <b>{minute}</b> | до <b>{expires_at.strftime('%H:%M')} МСК</b>",
        parse_mode=ParseMode.HTML,
    )
    await log_action(ctx, uid, f"⏰ Изменил КД на :{minute:02d} (было :{old_cd.get('minute','—')})")


async def cmd_resetkd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Сбросить КД: /resetkd"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для администраторов."); return
    if not is_cd_active():
        await update.message.reply_text("✅ КД и так не активен."); return
    old = DATA["active_cd"].get("nick", "—")
    DATA["active_cd"] = {"nick": None, "minute": None, "expires_at": None}
    await save_data()
    await update.message.reply_text(f"🗑 КД сброшено. (было: {old})")
    await log_action(ctx, uid, f"🗑 Сбросил КД (было: {old})")


async def cmd_setbal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Установить точный баланс: /setbal [@username или ID] [сумма]"""
    if not is_owner(update.effective_user.id): return
    if len(ctx.args) < 2:
        await update.message.reply_text("❌ /setbal [@username или ID] [сумма]"); return
    user = resolve_user(ctx.args[0])
    try: amount = int(ctx.args[1])
    except ValueError:
        await update.message.reply_text("❌ Сумма — число."); return
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    if amount < 0:
        await update.message.reply_text("❌ Сумма не может быть отрицательной."); return
    tid = user["id"]
    old = user["balance"]
    user["balance"] = amount
    await save_data()
    await update.message.reply_text(
        f"✅ Баланс установлен!\n👤 {user['full_name']}\n💰 {fmt(old)} ₽ → <b>{fmt(amount)} ₽</b>",
        parse_mode=ParseMode.HTML,
    )
    try: await ctx.bot.send_message(tid, f"💰 Ваш баланс обновлён: <b>{fmt(amount)} ₽</b>", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(ctx, update.effective_user.id, f"💰 Установил баланс {user['full_name']}: {fmt(old)} → {fmt(amount)} ₽")


async def cmd_kick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Отозвать доступ и удалить: /kick [@username или ID]"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not ctx.args:
        await update.message.reply_text("❌ /kick [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    if is_owner(tid):
        await update.message.reply_text("⚠️ Нельзя кикнуть владельца."); return
    full_name = user["full_name"]
    DATA["users"].pop(tid, None)
    DATA["admins"].discard(tid)
    await save_data()
    await update.message.reply_text(f"❌ Доступ отозван и удалён: {full_name}")
    try: await ctx.bot.send_message(tid, "❌ Ваш доступ отозван.")
    except Exception: pass
    await log_action(ctx, uid, f"❌ Кикнул {full_name}")


async def cmd_ban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Заблокировать (удалить с бота): /ban [@username или ID]"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not ctx.args:
        await update.message.reply_text("❌ /ban [@username или ID]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    if is_owner(tid):
        await update.message.reply_text("⚠️ Нельзя забанить владельца."); return
    full_name = user["full_name"]
    # Кикаем из группы (если есть)
    gid = DATA.get("group_id")
    if gid:
        try:
            await ctx.bot.ban_chat_member(gid, tid)
        except Exception as e: logger.warning(f"ban group err: {e}")
    DATA["users"].pop(tid, None)
    DATA["admins"].discard(tid)
    await save_data()
    await update.message.reply_text(f"🚫 Забанен и удалён: {full_name}")
    try: await ctx.bot.send_message(tid, "🚫 Вы заблокированы навсегда.")
    except Exception: pass
    await log_action(ctx, uid, f"🚫 Забанил {full_name}")


async def cmd_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Разбанить в группе: /unban [@username или ID]"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if not ctx.args:
        await update.message.reply_text("❌ /unban [ID]"); return
    try: tid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("❌ Нужен числовой ID (юзера уже нет в боте)."); return
    gid = DATA.get("group_id")
    if not gid:
        await update.message.reply_text("⚠️ Группа не задана. /setgroup в группе."); return
    try:
        await ctx.bot.unban_chat_member(gid, tid)
        await update.message.reply_text(f"✅ Разбанен в группе: <code>{tid}</code>", parse_mode=ParseMode.HTML)
        await log_action(ctx, uid, f"✅ Разбанил в группе {tid}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")


# ══════════════════════════════════════════════════════════
# /setgroup, /setnorma, /setlimit, /sleep, /vig, /unvig, /poka
# ══════════════════════════════════════════════════════════

async def cmd_setgroup(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Запустить в группе, чтобы привязать её как рабочую."""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Эту команду нужно вызвать В ГРУППЕ."); return
    DATA["group_id"] = chat.id
    await save_data()
    await update.message.reply_text(
        f"✅ Группа привязана: <b>{chat.title}</b>\n<code>{chat.id}</code>\n\n"
        f"Теперь сюда будут идти выговоры и кики.",
        parse_mode=ParseMode.HTML,
    )
    await log_action(ctx, uid, f"📌 Привязал группу: {chat.title} ({chat.id})")


async def cmd_setnorma(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Установить дневную норму: /setnorma → выбор класса → ввод количества"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    norm = DATA.get("daily_norm", {})
    cur = ""
    if norm.get("class") and norm.get("count"):
        cn = "Высокий" if norm["class"] == "high" else "Средний"
        cur = f"\n\n📊 Сейчас: <b>{cn} — {norm['count']} строек/день</b>"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏗 Высокий", callback_data="norma_high"),
        InlineKeyboardButton("🏗 Средний", callback_data="norma_medium"),
    ], [InlineKeyboardButton("🗑 Сбросить норму", callback_data="norma_reset")]])
    await update.message.reply_text(
        f"📊 <b>Установка дневной нормы</b>{cur}\n\nВыберите класс строек:",
        reply_markup=kb, parse_mode=ParseMode.HTML,
    )


async def cmd_setlimit(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Дневной лимит оплачиваемых строек: /setlimit [1-средний / 2-высокий] [N]  (0 = без лимита)"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if len(ctx.args) < 2:
        lim = DATA.get("daily_limit", {})
        h = lim.get("high", 0); m = lim.get("medium", 0)
        h_s = f"{h} строек/день" if h > 0 else "без лимита"
        m_s = f"{m} строек/день" if m > 0 else "без лимита"
        await update.message.reply_text(
            f"📊 <b>Текущие лимиты:</b>\n🏗 Высокий: <b>{h_s}</b>\n🏗 Средний: <b>{m_s}</b>\n\n"
            f"Использование:\n/setlimit 1 [N] — средний\n/setlimit 2 [N] — высокий\n0 = без лимита",
            parse_mode=ParseMode.HTML,
        ); return
    try:
        kind = int(ctx.args[0])
        count = int(ctx.args[1])
        if kind not in (1, 2): raise ValueError
        if count < 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Используй: /setlimit 1 [N] (средний) или /setlimit 2 [N] (высокий). N ≥ 0"); return
    cls = "medium" if kind == 1 else "high"
    cls_name = "Средний" if cls == "medium" else "Высокий"
    DATA["daily_limit"][cls] = count
    await save_data()
    if count == 0:
        await update.message.reply_text(f"✅ Лимит на <b>{cls_name}</b>: <b>отключён</b>", parse_mode=ParseMode.HTML)
        await log_action(ctx, uid, f"📊 Отключил лимит ({cls_name})")
    else:
        await update.message.reply_text(f"✅ Лимит на <b>{cls_name}</b>: <b>{count} строек/день</b>", parse_mode=ParseMode.HTML)
        await broadcast(ctx, f"📊 <b>Дневной лимит обновлён!</b>\n🏗 {cls_name}: <b>{count} строек/день</b>\nСверх лимита оплата 0 ₽.")
        await log_action(ctx, uid, f"📊 Лимит {cls_name}: {count}/день")


async def cmd_sleep(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Отпуск: /sleep [@user или ID] [дней] [причина]"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для админов."); return
    if len(ctx.args) < 3:
        await update.message.reply_text("❌ /sleep [@user или ID] [дней] [причина]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    try:
        days = int(ctx.args[1])
        if days <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Дней — положительное число."); return
    reason = " ".join(ctx.args[2:])
    user["sleep_until"] = now_msk() + timedelta(days=days)
    await save_data()
    await update.message.reply_text(
        f"💤 {user['full_name']} в отпуске на <b>{days} дн.</b>\n"
        f"До: <b>{user['sleep_until'].strftime('%d.%m.%Y %H:%M')} МСК</b>\n"
        f"Причина: {reason}",
        parse_mode=ParseMode.HTML,
    )
    try: await ctx.bot.send_message(user["id"], f"💤 Вам выдан отпуск на {days} дн.\nПричина: {reason}")
    except Exception: pass
    await log_action(ctx, uid, f"💤 Отпуск {user['full_name']} на {days} дн. ({reason})")


async def cmd_vig(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Выдать выговор: /vig [ID] [кол-во] [причина] — ТОЛЬКО В ГРУППЕ"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для админов."); return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("❌ Команда работает только В ГРУППЕ."); return
    if len(ctx.args) < 3:
        await update.message.reply_text("❌ /vig [@user или ID] [кол-во] [причина]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    try:
        n = int(ctx.args[1])
        if n <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Кол-во — положительное число."); return
    reason = " ".join(ctx.args[2:])
    tid = user["id"]
    user["warnings"] = user.get("warnings", 0) + n
    total = user["warnings"]
    await save_data()

    nick = user.get("nick") or user["full_name"]
    if total >= 5:
        # КИК!
        gid = DATA.get("group_id") or chat.id
        try:
            await ctx.bot.ban_chat_member(gid, tid)
            await ctx.bot.unban_chat_member(gid, tid)
        except Exception as e: logger.warning(f"vig kick err: {e}")
        DATA["users"].pop(tid, None)
        DATA["admins"].discard(tid)
        await save_data()
        await update.message.reply_text(
            f"🚫 <b>{nick}</b> получил {total}/5 выговоров — <b>КИКНУТ!</b>\nПричина: {reason}",
            parse_mode=ParseMode.HTML,
        )
        try: await ctx.bot.send_message(tid, f"🚫 Вы кикнуты — 5 выговоров.\nПричина последнего: {reason}")
        except Exception: pass
        await log_action(ctx, uid, f"🚫 Кикнул по 5 выговорам {nick}")
    else:
        await update.message.reply_text(
            f"⚠️ <b>{nick}</b> получает выговор +{n} (всего <b>{total}/5</b>)\nПричина: {reason}",
            parse_mode=ParseMode.HTML,
        )
        try: await ctx.bot.send_message(tid, f"⚠️ Вам выдан выговор: +{n} (всего {total}/5)\nПричина: {reason}")
        except Exception: pass
        await log_action(ctx, uid, f"⚠️ Выговор {nick} +{n} ({total}/5). {reason}")


async def cmd_unvig(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Снять выговор: /unvig [@user или ID] [кол-во] [причина]"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для админов."); return
    if len(ctx.args) < 3:
        await update.message.reply_text("❌ /unvig [@user или ID] [кол-во] [причина]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    try:
        n = int(ctx.args[1])
        if n <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Кол-во — положительное число."); return
    reason = " ".join(ctx.args[2:])
    tid = user["id"]
    old = user.get("warnings", 0)
    user["warnings"] = max(0, old - n)
    total = user["warnings"]
    await save_data()
    nick = user.get("nick") or user["full_name"]
    await update.message.reply_text(
        f"✅ Снято {n} выговоров с <b>{nick}</b> ({old} → {total}/5)\nПричина: {reason}",
        parse_mode=ParseMode.HTML,
    )
    try: await ctx.bot.send_message(tid, f"✅ С вас сняли {n} выговоров (теперь {total}/5)\nПричина: {reason}")
    except Exception: pass
    await log_action(ctx, uid, f"✅ Снял {n} выговоров с {nick} ({old}→{total})")


async def cmd_poka(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Кикнуть из группы и удалить с бота: /poka [@user или ID] [причина]"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if len(ctx.args) < 2:
        await update.message.reply_text("❌ /poka [@user или ID] [причина]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    tid = user["id"]
    if is_owner(tid):
        await update.message.reply_text("⚠️ Нельзя выгнать владельца."); return
    reason = " ".join(ctx.args[1:])
    full_name = user["full_name"]
    nick = user.get("nick") or full_name
    gid = DATA.get("group_id")
    if gid:
        try:
            await ctx.bot.ban_chat_member(gid, tid)
        except Exception as e: logger.warning(f"poka kick err: {e}")
    DATA["users"].pop(tid, None)
    DATA["admins"].discard(tid)
    await save_data()
    await update.message.reply_text(
        f"👋 <b>{nick}</b> кикнут с группы и удалён.\nПричина: {reason}",
        parse_mode=ParseMode.HTML,
    )
    if gid:
        try:
            await ctx.bot.send_message(gid, f"👋 <b>{nick}</b> покинул нас.\nПричина: {reason}", parse_mode=ParseMode.HTML)
        except Exception: pass
    try: await ctx.bot.send_message(tid, f"👋 Вас выгнали.\nПричина: {reason}")
    except Exception: pass
    await log_action(ctx, uid, f"👋 /poka {full_name}: {reason}")


async def evening_check(ctx: ContextTypes.DEFAULT_TYPE):
    """Вечерняя проверка — кто не выполнил дневную норму."""
    norm = DATA.get("daily_norm", {})
    cls = norm.get("class"); need = norm.get("count", 0)
    if not cls or need <= 0:
        logger.info("Вечерняя проверка: нормы нет — пропуск.")
        return

    today = today_msk_str()
    if DATA.get("last_check_date") == today:
        logger.info("Вечерняя проверка: уже сегодня делалась.")
        return

    gid = DATA.get("group_id")
    cls_name = "Высокий" if cls == "high" else "Средний"
    failures = []   # [(user, count_today)]
    kicked = []

    for u in list(DATA["users"].values()):
        # Пропускаем владельцев, без доступа, заблокированных, спящих
        if is_owner(u["id"]): continue
        if not u["has_access"] or u.get("blocked"): continue
        if is_sleeping(u): continue
        cnt = count_today_approved(u["id"], cls)
        if cnt >= need: continue
        # Не выполнил норму
        u["warnings"] = u.get("warnings", 0) + 1
        if u["warnings"] >= 5:
            kicked.append(dict(u))
            tid = u["id"]
            if gid:
                try:
                    await ctx.bot.ban_chat_member(gid, tid)
                    await ctx.bot.unban_chat_member(gid, tid)
                except Exception as e: logger.warning(f"evening kick err {tid}: {e}")
            DATA["users"].pop(tid, None)
            DATA["admins"].discard(tid)
            try: await ctx.bot.send_message(tid, "🚫 Вы получили 5/5 выговоров и кикнуты с группы.")
            except Exception: pass
        else:
            failures.append((u, cnt))
            try:
                await ctx.bot.send_message(
                    u["id"],
                    f"⚠️ Вы не выполнили дневную норму ({cnt}/{need} {cls_name}).\n"
                    f"Выговор +1 (всего <b>{u['warnings']}/5</b>).",
                    parse_mode=ParseMode.HTML,
                )
            except Exception: pass

    DATA["last_check_date"] = today
    await save_data()

    if not gid:
        logger.info("Вечерняя проверка: нет group_id.")
        return

    if not failures and not kicked:
        try:
            await ctx.bot.send_message(
                gid,
                f"✅ <b>Все молодцы!</b>\nДневная норма ({cls_name} — {need}/день) выполнена всеми.",
                parse_mode=ParseMode.HTML,
            )
        except Exception: pass
        return

    lines = [f"📊 <b>ИТОГИ ДНЯ</b> — норма: {cls_name} {need}/день\n"]
    if failures:
        lines.append("⚠️ <b>Не выполнили норму:</b>")
        for u, cnt in failures:
            nick = u.get("nick") or u["full_name"]
            lines.append(f"• {nick} ({cnt}/{need}) → выговор {u['warnings']}/5")
        lines.append("")
    if kicked:
        lines.append("🚫 <b>Кикнуты (5/5 выговоров):</b>")
        for u in kicked:
            nick = u.get("nick") or u["full_name"]
            lines.append(f"• {nick}")
    try:
        await ctx.bot.send_message(gid, "\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"evening report err: {e}")


async def cmd_checknorma(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Запустить вечернюю проверку прямо сейчас (для теста): /checknorma"""
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев."); return
    DATA["last_check_date"] = None  # сбросим, чтобы можно было прогнать
    await update.message.reply_text("🔍 Запускаю проверку...")
    await evening_check(ctx)
    await update.message.reply_text("✅ Проверка завершена.")


async def cmd_warnings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Список всех с выговорами: /warnings"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для админов."); return
    items = sorted(
        [u for u in DATA["users"].values() if u.get("warnings", 0) > 0],
        key=lambda u: -u.get("warnings", 0),
    )
    if not items:
        await update.message.reply_text("✅ Никого с выговорами нет."); return
    lines = ["⚠️ <b>Список с выговорами:</b>\n"]
    for u in items:
        nick = u.get("nick") or u["full_name"]
        lines.append(f"• {nick} — <b>{u['warnings']}/5</b>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_addreport(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Вручную добавить одобренный отчёт: /addreport [@username или ID] [high/medium]"""
    uid = update.effective_user.id
    if not is_owner(uid):
        await update.message.reply_text("❌ Только для владельцев."); return
    if len(ctx.args) < 2:
        await update.message.reply_text("❌ /addreport [@username или ID] [high/medium]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    cls = ctx.args[1].lower()
    if cls not in ("high", "medium"):
        await update.message.reply_text("❌ Тип: high или medium"); return
    pay = PAYMENTS[cls]
    tid = user["id"]
    rep_id = DATA["report_counter"]
    DATA["reports"][rep_id] = {
        "id": rep_id, "user_id": tid,
        "nick": user.get("nick") or user["full_name"],
        "class": cls, "pay": pay, "cd": 0,
        "p1": None, "p2": None,
        "status": "approved", "approved_by": uid,
        "at": now_msk(), "msg_ids": {},
    }
    DATA["report_counter"] += 1
    user["balance"] += pay
    user["total_reports"] += 1
    await save_data()
    cls_name = "Высокий" if cls == "high" else "Средний"
    await update.message.reply_text(
        f"✅ Отчёт #{rep_id} добавлен!\n👤 {user['full_name']}\n🏗 {cls_name}\n💰 +{fmt(pay)} ₽",
        parse_mode=ParseMode.HTML,
    )
    try: await ctx.bot.send_message(tid, f"✅ Вам добавлен отчёт #{rep_id} (+{fmt(pay)} ₽)", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(ctx, uid, f"➕ Добавил отчёт #{rep_id} {user['full_name']} +{fmt(pay)} ₽")


async def cmd_nick(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Установить ник пользователю: /nick [@username или ID] [ник]"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("❌ Только для администраторов."); return
    if len(ctx.args) < 2:
        await update.message.reply_text("❌ /nick [@username или ID] [ник]"); return
    user = resolve_user(ctx.args[0])
    if not user:
        await update.message.reply_text("❌ Не найден."); return
    new_nick = " ".join(ctx.args[1:])
    old_nick = user.get("nick", "—")
    user["nick"] = new_nick
    await save_data()
    await update.message.reply_text(
        f"✅ Ник обновлён!\n👤 {user['full_name']}\n🏷 {old_nick} → <b>{new_nick}</b>",
        parse_mode=ParseMode.HTML,
    )
    await log_action(ctx, uid, f"🏷 Изменил ник {user['full_name']}: {old_nick} → {new_nick}")


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Список всех команд"""
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text(
            "📋 <b>Команды для пользователей:</b>\n\n"
            "/start — главное меню\n"
            "/bank [номер] — посмотреть/изменить счёт\n"
            "/id — узнать свой ID\n"
            "/sozvat — позвать всех на помощь",
            parse_mode=ParseMode.HTML,
        )
        return

    owner_cmds = ""
    if is_owner(uid):
        owner_cmds = (
            "\n\n👑 <b>Команды владельца:</b>\n"
            "💰 <b>Деньги:</b>\n"
            "/setbal [@user] [сумма] — установить баланс\n"
            "/givebonus [@user] [сумма] [причина] — начислить бонус\n"
            "/takecash [@user] [сумма] [причина] — снять деньги\n\n"
            "👥 <b>Управление:</b>\n"
            "/kick [@user] — отозвать доступ + удалить\n"
            "/ban [@user] — забанить + удалить\n"
            "/unban [ID] — разбанить в группе\n"
            "/poka [@user] [причина] — кикнуть с группы и удалить\n"
            "/makeadmin [@user] — назначить админа\n"
            "/takeadmin [@user] — снять админку\n"
            "/addreport [@user] [high/medium] — добавить отчёт\n"
            "/listadmins — все админы\n\n"
            "📊 <b>Норма и лимиты:</b>\n"
            "/setgroup — привязать группу (вызвать В ГРУППЕ)\n"
            "/setnorma — установить дневную норму\n"
            "/setlimit [1/2] [N] — лимит: 1=средний, 2=высокий, 0=нет\n"
            "/checknorma — запустить проверку нормы СЕЙЧАС\n\n"
            "🛠 <b>Прочее:</b>\n"
            "/vipeall — полный сброс (балансы, отчёты)\n"
            "/texwork [причина] — техработы\n"
            "/on — снять техработы\n"
            "/event — создать конкурс\n"
            "/estop, /egive, /eset"
        )

    await update.message.reply_text(
        "📋 <b>Команды администратора:</b>\n\n"
        "⏰ <b>КД:</b>\n"
        "/changekd [минута] — изменить КД\n"
        "/resetkd — сбросить КД\n\n"
        "👥 <b>Пользователи:</b>\n"
        "/userinfo [@user] — инфо о пользователе\n"
        "/checkid [@username] — найти ID\n"
        "/otcets [@user] — отчёты пользователя\n"
        "/giveds [@user] — выдать доступ\n"
        "/nick [@user] [ник] — установить ник\n"
        "/sleep [@user] [дней] [причина] — отпуск\n\n"
        "⚠️ <b>Выговоры (в группе):</b>\n"
        "/vig [@user] [N] [причина] — выдать N выговоров\n"
        "/unvig [@user] [N] [причина] — снять N выговоров\n"
        "/warnings — список всех с выговорами\n\n"
        "📊 <b>Статистика:</b>\n"
        "/stats — общая\n\n"
        "📢 <b>Сообщения:</b>\n"
        "/msg [текст] — всем пользователям"
        + owner_cmds,
        parse_mode=ParseMode.HTML,
    )


# ══════════════════════════════════════════════════════════
# ОБРАБОТЧИК ОШИБОК
# ══════════════════════════════════════════════════════════

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception: {ctx.error}", exc_info=ctx.error)


# ══════════════════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════════════════

def main():
    load_data()
    app = Application.builder().token(BOT_TOKEN).build()

    # Команды
    commands = [
        ("start",      cmd_start),
        ("help",       cmd_help),
        # КД
        ("changekd",   cmd_changekd),
        ("resetkd",    cmd_resetkd),
        # Балансы
        ("givebonus",  cmd_givebonus),
        ("takecash",   cmd_takecash),
        ("setbal",     cmd_setbal),
        # Управление пользователями
        ("makeadmin",  cmd_makeadmin),
        ("takeadmin",  cmd_takeadmin),
        ("giveds",     cmd_giveds),
        ("kick",       cmd_kick),
        ("ban",        cmd_ban),
        ("unban",      cmd_unban),
        ("poka",       cmd_poka),
        ("nick",       cmd_nick),
        ("addreport",  cmd_addreport),
        ("sleep",      cmd_sleep),
        # Группа и норма
        ("setgroup",   cmd_setgroup),
        ("setnorma",   cmd_setnorma),
        ("setlimit",   cmd_setlimit),
        ("vig",        cmd_vig),
        ("unvig",      cmd_unvig),
        ("warnings",   cmd_warnings),
        ("checknorma", cmd_checknorma),
        # Инфо
        ("sozvat",     cmd_sozvat),
        ("msg",        cmd_msg),
        ("bank",       cmd_bank),
        ("id",         cmd_id),
        ("checkid",    cmd_checkid),
        ("stats",      cmd_stats),
        ("userinfo",   cmd_userinfo),
        ("listadmins", cmd_listadmins),
        ("otcets",     cmd_otcets),
        # Режимы
        ("texwork",    cmd_texwork),
        ("on",         cmd_on),
        # Конкурсы
        ("event",      cmd_event),
        ("estop",      cmd_estop),
        ("egive",      cmd_egive),
        ("eset",       cmd_eset),
        ("vipeall",    cmd_vipeall),
    ]
    for cmd, fn in commands:
        app.add_handler(CommandHandler(cmd, fn))

    # ─── JobQueue: вечерняя проверка нормы в 22:00 МСК (=19:00 UTC) ───
    try:
        from datetime import time as dtime
        if app.job_queue is not None:
            app.job_queue.run_daily(
                evening_check,
                time=dtime(hour=19, minute=0),  # 19:00 UTC = 22:00 МСК
                name="evening_check",
            )
            logger.info("✅ Вечерняя проверка запланирована на 22:00 МСК.")
        else:
            logger.warning("⚠️ JobQueue недоступен (установи python-telegram-bot[job-queue]).")
    except Exception as e:
        logger.error(f"Ошибка планирования вечерней проверки: {e}")

    # Единый роутер колбэков — нет ConversationHandler-конфликтов
    app.add_handler(CallbackQueryHandler(on_callback))

    # Единый роутер сообщений (текст + фото)
    app.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | filters.PHOTO,
        on_message,
    ))

    app.add_error_handler(error_handler)

    webhook_url = os.environ.get("WEBHOOK_URL")
    port = int(os.environ.get("PORT", 8443))

    if webhook_url:
        logger.info(f"Webhook: {webhook_url}")
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=BOT_TOKEN,
            webhook_url=f"{webhook_url}/{BOT_TOKEN}",
            drop_pending_updates=True,
        )
    else:
        logger.info("Polling-режим.")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
