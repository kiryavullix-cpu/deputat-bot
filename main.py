#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DEPUTAT - Автоматизированный бот для отчетов
"""

import logging
import os
import json
from datetime import datetime, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, InputMediaPhoto
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, ConversationHandler, filters
from telegram.constants import ParseMode

# ================================
# НАСТРОЙКИ
# ================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8724153136:AAFhD24OvSoepxott4H-9WodBJAd-1rUh7U")
OWNER_IDS = [6693142204, 5711452887]
DATA_FILE = 'data.json'
MSK = timedelta(hours=3)  # Московское время UTC+3

PAYMENTS = {
    'high': 350000,
    'medium': 200000
}

# ================================
# ДАННЫЕ
# ================================

DATA = {
    'users': {},
    'reports': {},
    'report_counter': 1,
    'admins': set(),
    'active_cd': {'nick': None, 'minute': None, 'expires_at': None},
    'maintenance': {'active': False, 'reason': ''}
}

# ================================
# ЛОГИРОВАНИЕ
# ================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

REPORT_PHOTO1, REPORT_PHOTO2, REPORT_NICK, REPORT_CLASS, REPORT_CD = range(5)

# ================================
# СОХРАНЕНИЕ И ЗАГРУЗКА ДАННЫХ
# ================================

def save_data():
    try:
        reports_serialized = {}
        for rep_id, rep in DATA['reports'].items():
            rep_copy = dict(rep)
            if isinstance(rep_copy.get('at'), datetime):
                rep_copy['at'] = rep_copy['at'].isoformat()
            if 'msg_ids' in rep_copy:
                rep_copy['msg_ids'] = {str(k): v for k, v in rep_copy['msg_ids'].items()}
            reports_serialized[str(rep_id)] = rep_copy

        ac = DATA['active_cd']
        data_to_save = {
            'users': {str(k): v for k, v in DATA['users'].items()},
            'reports': reports_serialized,
            'report_counter': DATA['report_counter'],
            'admins': list(DATA['admins']),
            'active_cd': {
                'nick': ac.get('nick'),
                'minute': ac.get('minute'),
                'expires_at': ac['expires_at'].isoformat() if ac.get('expires_at') else None,
            },
            'maintenance': DATA['maintenance']
        }
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения данных: {e}")


def load_data():
    global DATA
    if not os.path.exists(DATA_FILE):
        logger.info("Файл данных не найден, начинаем с чистого листа.")
        return
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            saved = json.load(f)

        DATA['users'] = {}
        for k, v in saved.get('users', {}).items():
            uid = int(k)
            v['id'] = int(v['id'])
            v['telegram_id'] = int(v['telegram_id'])
            DATA['users'][uid] = v

        DATA['reports'] = {}
        for rep_id, rep in saved.get('reports', {}).items():
            rep['id'] = int(rep['id'])
            rep['user_id'] = int(rep['user_id'])
            if rep.get('at'):
                try:
                    rep['at'] = datetime.fromisoformat(rep['at'])
                except Exception:
                    rep['at'] = datetime.now()
            if 'msg_ids' in rep:
                rep['msg_ids'] = {int(k2): v2 for k2, v2 in rep['msg_ids'].items()}
            DATA['reports'][int(rep_id)] = rep

        DATA['report_counter'] = saved.get('report_counter', 1)
        DATA['admins'] = set(saved.get('admins', []))

        ac = saved.get('active_cd', {})
        DATA['active_cd'] = {
            'nick': ac.get('nick'),
            'minute': ac.get('minute'),
            'expires_at': datetime.fromisoformat(ac['expires_at']) if ac.get('expires_at') else None,
        }

        DATA['maintenance'] = saved.get('maintenance', {'active': False, 'reason': ''})

        logger.info(f"Данные загружены: {len(DATA['users'])} юзеров, {len(DATA['reports'])} отчётов")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных: {e}")

# ================================
# УТИЛИТЫ
# ================================

def is_owner(user_id):
    return user_id in OWNER_IDS

def is_admin(user_id):
    return user_id in OWNER_IDS or user_id in DATA['admins']

def get_all_privileged():
    return list(OWNER_IDS) + [uid for uid in DATA['admins'] if uid not in OWNER_IDS]

def is_maintenance():
    return DATA['maintenance'].get('active', False)

def detect_class(text):
    text = text.lower()
    for p in ['выс', 'высок', 'вышка', 'high', 'хай']:
        if p in text: return 'high', PAYMENTS['high']
    for p in ['сред', 'medium', 'ср', 'мед']:
        if p in text: return 'medium', PAYMENTS['medium']
    return None, 0

def format_balance(amount):
    return f"{amount:,}".replace(',', ' ')

def now_msk():
    return datetime.utcnow() + MSK

def calculate_cd_expiry(cd_minute):
    now = now_msk()
    if now.minute < cd_minute:
        expiry = now.replace(minute=cd_minute, second=0, microsecond=0)
    else:
        next_hour = now + timedelta(hours=1)
        expiry = next_hour.replace(minute=cd_minute, second=0, microsecond=0)
    return expiry

def is_cd_active():
    exp = DATA['active_cd'].get('expires_at')
    if not exp:
        return False
    return now_msk() < exp

def get_user_by_username(username: str):
    username = username.lstrip('@').lower()
    for u in DATA['users'].values():
        if u.get('username') and u['username'].lower() == username:
            return u
    return None

async def log_action(context, actor_id, text):
    """Уведомляет всех других привилегированных пользователей о действии."""
    actor = DATA['users'].get(actor_id)
    actor_name = actor['full_name'] if actor else str(actor_id)
    role = "👑 Владелец" if is_owner(actor_id) else "👮 Админ"
    msg = f"📣 <b>{role} {actor_name}:</b>\n{text}"
    for uid in get_all_privileged():
        if uid != actor_id:
            try:
                await context.bot.send_message(uid, msg, parse_mode=ParseMode.HTML)
            except Exception:
                pass

def build_keyboard(user_id):
    keyboard = [['📝 Отправить отчет', '💰 Баланс'], ['📊 История строек', '⏰ КД', '📋 Последний отчет']]
    if is_owner(user_id):
        keyboard.append(['👑 Панель владельца', '👥 Пользователи', '💸 Выплаты'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ================================
# КОМАНДЫ И ОБРАБОТЧИКИ
# ================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    u = update.effective_user

    if is_owner(user_id) and user_id not in DATA['users']:
        DATA['users'][user_id] = {
            'id': user_id, 'telegram_id': user_id, 'username': u.username,
            'full_name': u.full_name, 'has_access': True, 'balance': 0,
            'total_reports': 0, 'bank_account': None, 'nick': None, 'blocked': False
        }
        save_data()

    user = DATA['users'].get(user_id)

    if user:
        if user['has_access']:
            await update.message.reply_text(
                f"👋 Привет, <b>{user['full_name']}</b>!\n"
                f"💼 Баланс: <b>{format_balance(user['balance'])} ₽</b>\n"
                f"🏗️ Строек: <b>{user['total_reports']}</b>",
                reply_markup=build_keyboard(user_id), parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("⏳ Ваш запрос на доступ еще на рассмотрении.")
    else:
        keyboard = [[InlineKeyboardButton("🔑 Получить доступ", callback_data="request_access")]]
        await update.message.reply_text(
            "🏗️ <b>DEPUTAT - Система отчетов</b>\n\nНажми кнопку ниже:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML
        )

async def request_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    u = query.from_user

    if is_maintenance() and not is_owner(u.id):
        await query.message.reply_text(f"🔧 Бот на техработах: {DATA['maintenance']['reason']}")
        return

    if u.id in DATA['users']:
        await query.message.reply_text("⏳ Запрос уже отправлен.")
        return

    DATA['users'][u.id] = {
        'id': u.id, 'telegram_id': u.id, 'username': u.username, 'full_name': u.full_name,
        'has_access': False, 'balance': 0, 'total_reports': 0,
        'bank_account': None, 'nick': None, 'blocked': False
    }
    save_data()

    for owner_id in get_all_privileged():
        try:
            btn = [[InlineKeyboardButton("✅ Выдать доступ", callback_data=f"grant_{u.id}")]]
            await context.bot.send_message(
                owner_id,
                f"🔔 <b>Новый запрос на доступ!</b>\nЮзер: {u.full_name}\nID: <code>{u.id}</code>",
                reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.warning(f"Не удалось отправить уведомление {owner_id}: {e}")
    await query.message.reply_text("✅ Запрос отправлен владельцам!")

async def grant_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id): return

    target_id = int(query.data.split('_')[1])
    if target_id in DATA['users']:
        if DATA['users'][target_id]['has_access']:
            await query.answer("⚠️ Доступ уже был выдан другим администратором!", show_alert=True)
            return
        DATA['users'][target_id]['has_access'] = True
        save_data()
        granter_name = query.from_user.full_name
        await query.message.edit_text(f"✅ Доступ для <code>{target_id}</code> выдан — {granter_name}!", parse_mode=ParseMode.HTML)
        await context.bot.send_message(target_id, "✅ <b>Доступ одобрен!</b>\nВведите ваш <b>номер счета</b>:", parse_mode=ParseMode.HTML)
        await log_action(context, query.from_user.id, f"✅ Выдал доступ пользователю <code>{target_id}</code> ({DATA['users'][target_id]['full_name']})")

# ================================
# ЛОГИКА ОТЧЕТОВ
# ================================

async def report_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if is_maintenance() and not is_owner(user_id):
        await update.message.reply_text(f"🔧 Бот на техработах: {DATA['maintenance']['reason']}")
        return ConversationHandler.END
    user = DATA['users'].get(user_id)
    if not user or not user['has_access'] or user.get('blocked'): return ConversationHandler.END
    context.user_data.clear()
    await update.message.reply_text("📸 Отправьте фото <b>НАЧАЛА</b> стройки:", parse_mode=ParseMode.HTML)
    return REPORT_PHOTO1

async def report_photo1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo: return REPORT_PHOTO1
    context.user_data['photo_start'] = update.message.photo[-1].file_id
    await update.message.reply_text("📸 Отправьте фото <b>ОКОНЧАНИЯ</b> стройки:", parse_mode=ParseMode.HTML)
    return REPORT_PHOTO2

async def report_photo2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo: return REPORT_PHOTO2
    context.user_data['photo_end'] = update.message.photo[-1].file_id
    user = DATA['users'].get(update.effective_user.id)
    saved_nick = user.get('nick') if user else None
    if saved_nick:
        keyboard = ReplyKeyboardMarkup([[saved_nick]], resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text(
            f"👤 Ваш ник: <b>{saved_nick}</b>\nНажмите кнопку или введите другой:",
            reply_markup=keyboard, parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("👤 Напишите ваш <b>ник</b>:", parse_mode=ParseMode.HTML)
    return REPORT_NICK

async def report_nick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['nick'] = update.message.text
    DATA['users'][update.effective_user.id]['nick'] = update.message.text
    save_data()
    await update.message.reply_text("🏗️ Напишите <b>класс стройки</b> (Высокий/Средний):", parse_mode=ParseMode.HTML)
    return REPORT_CLASS

async def report_class(update: Update, context: ContextTypes.DEFAULT_TYPE):
    b_class, pay = detect_class(update.message.text)
    if not b_class: return REPORT_CLASS
    context.user_data['building_class'] = b_class
    context.user_data['payment'] = pay
    await update.message.reply_text(
        f"💰 Оплата: {format_balance(pay)} ₽\n⏰ Напишите <b>минуту КД</b> (0-59):",
        parse_mode=ParseMode.HTML
    )
    return REPORT_CD

async def report_cd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cd_min = int(update.message.text)
        if not 0 <= cd_min <= 59: raise ValueError
    except Exception:
        return REPORT_CD

    user_id = update.effective_user.id
    user = DATA['users'][user_id]

    # КД ставится сразу при отправке (МСК)
    expires_at = calculate_cd_expiry(cd_min)
    nick = context.user_data.get('nick', user.get('nick', user['full_name']))
    DATA['active_cd'] = {'nick': nick, 'minute': cd_min, 'expires_at': expires_at}

    rep_id = DATA['report_counter']
    DATA['reports'][rep_id] = {
        'id': rep_id, 'user_id': user_id, 'nick': context.user_data['nick'],
        'class': context.user_data['building_class'], 'pay': context.user_data['payment'],
        'cd': cd_min, 'p1': context.user_data['photo_start'], 'p2': context.user_data['photo_end'],
        'status': 'pending', 'at': datetime.now(), 'msg_ids': {}
    }
    DATA['report_counter'] += 1

    txt = (f"📋 <b>ОТЧЕТ #{rep_id}</b>\n"
           f"👤 Ник: {context.user_data['nick']}\n"
           f"🏗 Класс: {context.user_data['building_class']}\n"
           f"💰 {format_balance(context.user_data['payment'])} ₽\n"
           f"💳 Счёт: {user['bank_account'] or '—'}\n"
           f"⏰ КД до: {expires_at.strftime('%H:%M')} МСК (минута {cd_min})")
    btn = [[InlineKeyboardButton("✅ ОДОБРИТЬ", callback_data=f"appr_{rep_id}")]]
    media_group = [
        InputMediaPhoto(media=context.user_data['photo_start'], caption="📸 Начало стройки"),
        InputMediaPhoto(media=context.user_data['photo_end'], caption="📸 Конец стройки"),
    ]

    for o_id in get_all_privileged():
        try:
            sent_media = await context.bot.send_media_group(o_id, media_group)
            sent_btn = await context.bot.send_message(o_id, txt, reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML)
            DATA['reports'][rep_id]['msg_ids'][o_id] = {
                'media': [m.message_id for m in sent_media],
                'btn': sent_btn.message_id
            }
        except Exception as e:
            logger.warning(f"Не удалось отправить отчет {o_id}: {e}")

    save_data()
    await update.message.reply_text(
        f"✅ Отчет #{rep_id} отправлен!\n⏰ КД активен до <b>{expires_at.strftime('%H:%M')} МСК</b>",
        parse_mode=ParseMode.HTML
    )
    context.user_data.clear()
    return ConversationHandler.END

async def approve_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id): return
    rep_id = int(query.data.split('_')[1])
    rep = DATA['reports'].get(rep_id)

    if not rep: return
    if rep['status'] == 'approved':
        await query.answer("⚠️ Этот отчет уже был одобрен другим владельцем!", show_alert=True)
        return
    if rep['status'] == 'pending':
        rep['status'] = 'approved'
        rep['approved_by'] = query.from_user.id
        uid = rep['user_id']
        DATA['users'][uid]['balance'] += rep['pay']
        DATA['users'][uid]['total_reports'] += 1
        approver_name = query.from_user.full_name

        approved_txt = (f"📋 <b>ОТЧЕТ #{rep_id}</b>\n"
                        f"👤 Ник: {rep['nick']}\n"
                        f"🏗 Класс: {rep['class']}\n"
                        f"💰 {format_balance(rep['pay'])} ₽\n"
                        f"⏰ КД: минута {rep['cd']}\n\n"
                        f"✅ <b>ОДОБРЕНО</b> — {approver_name}")

        for o_id, ids in rep.get('msg_ids', {}).items():
            btn_id = ids.get('btn') if isinstance(ids, dict) else None
            if btn_id:
                try:
                    await context.bot.edit_message_text(
                        approved_txt, chat_id=o_id,
                        message_id=btn_id, parse_mode=ParseMode.HTML
                    )
                except Exception: pass

        if not rep.get('msg_ids'):
            try:
                await query.message.edit_text(approved_txt, parse_mode=ParseMode.HTML)
            except Exception: pass

        save_data()
        await context.bot.send_message(uid, f"🎉 Отчет #{rep_id} одобрен! +{format_balance(rep['pay'])} ₽")
        await log_action(context, query.from_user.id, f"✅ Одобрил отчет #{rep_id} (ник: {rep['nick']}, +{format_balance(rep['pay'])} ₽)")

# ================================
# УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ
# ================================

async def show_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid): return
    users = [u for u in DATA['users'].values() if not is_owner(u['id'])]
    if not users:
        await update.message.reply_text("👥 Пользователей нет.")
        return
    for u in users:
        role = "👮 Админ" if u['id'] in DATA['admins'] else "👤"
        status = "🚫 Заблокирован" if u.get('blocked') else ("✅ Активен" if u['has_access'] else "⏳ Ожидает")
        nick = f" ({u['nick']})" if u.get('nick') else ""
        bank = u.get('bank_account') or '—'
        text = (f"{role} <b>{u['full_name']}</b>{nick}\n"
                f"🆔 <code>{u['id']}</code> | 📶 {status}\n"
                f"💳 {bank} | 💰 {format_balance(u['balance'])} ₽")
        btns = []
        if u['has_access'] and not u.get('blocked'):
            btns.append([InlineKeyboardButton("❌ Забрать доступ", callback_data=f"revoke_{u['id']}")])
            btns.append([InlineKeyboardButton("🚫 Заблокировать", callback_data=f"block_{u['id']}")])
        elif u.get('blocked'):
            btns.append([InlineKeyboardButton("✅ Разблокировать", callback_data=f"unblock_{u['id']}")])
        else:
            btns.append([InlineKeyboardButton("✅ Выдать доступ", callback_data=f"grant_{u['id']}")])
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)

async def manage_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_owner(query.from_user.id): return

    parts = query.data.split('_')
    action = parts[0]
    target_id = int(parts[1])
    user = DATA['users'].get(target_id)
    if not user: return

    if action == 'revoke':
        user['has_access'] = False
        save_data()
        await query.message.edit_text(query.message.text + "\n\n❌ Доступ забран", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "❌ Ваш доступ был отозван владельцем.")
        except Exception: pass
        await log_action(context, query.from_user.id, f"❌ Отозвал доступ у {user['full_name']} (<code>{target_id}</code>)")
    elif action == 'block':
        user['blocked'] = True
        user['has_access'] = False
        save_data()
        await query.message.edit_text(query.message.text + "\n\n🚫 Заблокирован", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "🚫 Вы были заблокированы владельцем.")
        except Exception: pass
        await log_action(context, query.from_user.id, f"🚫 Заблокировал {user['full_name']} (<code>{target_id}</code>)")
    elif action == 'unblock':
        user['blocked'] = False
        save_data()
        await query.message.edit_text(query.message.text + "\n\n✅ Разблокирован", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "✅ Вы были разблокированы владельцем.")
        except Exception: pass
        await log_action(context, query.from_user.id, f"✅ Разблокировал {user['full_name']} (<code>{target_id}</code>)")

# ================================
# СИСТЕМА ВЫПЛАТ
# ================================

async def show_payroll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid): return
    users_with_balance = [u for u in DATA['users'].values() if u['balance'] > 0]
    if not users_with_balance:
        await update.message.reply_text("💸 Нет пользователей с балансом для выплат.")
        return
    total = sum(u['balance'] for u in users_with_balance)
    btn_all = [[InlineKeyboardButton(f"✅ ЗП выдано ВСЕМ ({format_balance(total)} ₽)", callback_data="pay_all")]]
    await update.message.reply_text(
        f"💸 <b>СИСТЕМА ВЫПЛАТ</b>\n\nПользователей к выплате: {len(users_with_balance)}\nОбщая сумма: <b>{format_balance(total)} ₽</b>",
        reply_markup=InlineKeyboardMarkup(btn_all), parse_mode=ParseMode.HTML
    )
    for u in users_with_balance:
        nick = f" ({u['nick']})" if u.get('nick') else ""
        bank = u.get('bank_account') or '—'
        text = (f"👤 <b>{u['full_name']}</b>{nick}\n"
                f"💳 Счёт: {bank}\n"
                f"💰 К выплате: <b>{format_balance(u['balance'])} ₽</b>")
        btn = [[InlineKeyboardButton("✅ ЗП выдано", callback_data=f"pay_{u['id']}")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML)

async def handle_payroll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_owner(query.from_user.id): return

    if query.data == 'pay_all':
        count = 0
        names = []
        for u in DATA['users'].values():
            if u['balance'] > 0:
                names.append(f"{u['full_name']} ({format_balance(u['balance'])} ₽)")
                u['balance'] = 0
                count += 1
                try:
                    await context.bot.send_message(u['id'], "✅ Ваша зарплата была выдана! Баланс обнулён.")
                except Exception: pass
        save_data()
        await query.message.edit_text(f"✅ <b>ЗП выдана всем!</b> Обнулено: {count} пользователей.", parse_mode=ParseMode.HTML)
        await log_action(context, query.from_user.id, f"💸 Выдал ЗП всем ({count} чел.): {', '.join(names)}")
    elif query.data.startswith('pay_'):
        target_id = int(query.data.split('_')[1])
        user = DATA['users'].get(target_id)
        if user and user['balance'] > 0:
            paid = user['balance']
            user['balance'] = 0
            save_data()
            await query.message.edit_text(query.message.text + "\n\n✅ <b>ЗП выдана! Баланс обнулён.</b>", parse_mode=ParseMode.HTML)
            try:
                await context.bot.send_message(target_id, "✅ Ваша зарплата была выдана! Баланс обнулён.")
            except Exception: pass
            await log_action(context, query.from_user.id, f"💸 Выдал ЗП {user['full_name']} (<code>{target_id}</code>): {format_balance(paid)} ₽")
        else:
            await query.message.edit_text(query.message.text + "\n\n⚠️ Баланс уже 0.", parse_mode=ParseMode.HTML)

# ================================
# КНОПКИ И ТЕКСТОВЫЕ ОБРАБОТЧИКИ
# ================================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text
    uid = update.effective_user.id
    user = DATA['users'].get(uid)

    if not user: return

    # Проверка техработ
    if is_maintenance() and not is_owner(uid):
        await update.message.reply_text(f"🔧 Бот на техработах.\nПричина: {DATA['maintenance']['reason']}")
        return

    # Сохранение счета при первом вводе
    if user['has_access'] and user['bank_account'] is None and txt.isdigit():
        user['bank_account'] = txt
        save_data()
        await update.message.reply_text(f"✅ Счет {txt} сохранен! Можете слать отчеты.")
        return

    if txt == '💰 Баланс':
        await update.message.reply_text(
            f"💼 Баланс: <b>{format_balance(user['balance'])} ₽</b>",
            parse_mode=ParseMode.HTML
        )

    elif txt == '⏰ КД':
        if is_cd_active():
            cd = DATA['active_cd']
            exp = cd['expires_at']
            await update.message.reply_text(
                f"⏰ КД активен до <b>{exp.strftime('%H:%M')} МСК</b> (минута {cd['minute']})\n"
                f"Поставил: <b>{cd['nick']}</b>",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("✅ КД не активен.")

    elif txt == '📋 Последний отчет':
        if not DATA['reports']:
            await update.message.reply_text("📋 Отчётов ещё не было.")
            return
        last = list(DATA['reports'].values())[-1]
        st = "✅ Одобрен" if last['status'] == 'approved' else "⏳ На рассмотрении"
        date = last['at'].strftime('%d.%m.%Y %H:%M') if isinstance(last['at'], datetime) else '—'
        cls = "Высокий" if last['class'] == 'high' else "Средний"
        await update.message.reply_text(
            f"📋 <b>Последний отчет #{last['id']}</b>\n"
            f"👤 Ник: {last['nick']}\n"
            f"🏗 Класс: {cls}\n"
            f"💰 Сумма: {format_balance(last['pay'])} ₽\n"
            f"⏰ КД: минута {last['cd']}\n"
            f"📅 Дата: {date}\n"
            f"📶 Статус: {st}",
            parse_mode=ParseMode.HTML
        )

    elif txt == '📊 История строек':
        u_reps = [r for r in DATA['reports'].values() if r['user_id'] == uid][-5:]
        if not u_reps:
            await update.message.reply_text("📊 У вас ещё нет отчётов.")
            return
        msg = "📊 <b>Последние 5 отчётов:</b>\n\n"
        for r in u_reps:
            st = "✅" if r['status'] == 'approved' else "⏳"
            date = r['at'].strftime('%d.%m %H:%M') if isinstance(r['at'], datetime) else '—'
            msg += f"{st} Отчет #{r['id']} — {format_balance(r['pay'])} ₽ ({date})\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

    elif txt == '👑 Панель владельца' and is_owner(uid):
        total_pay = sum(u['balance'] for u in DATA['users'].values())
        active = sum(1 for u in DATA['users'].values() if u['has_access'] and not u.get('blocked'))
        blocked = sum(1 for u in DATA['users'].values() if u.get('blocked'))
        maint = "🔧 ВКЛ" if is_maintenance() else "✅ ВЫКЛ"
        await update.message.reply_text(
            f"👑 <b>ПАНЕЛЬ ВЛАДЕЛЬЦА</b>\n\n"
            f"👥 Всего юзеров: {len(DATA['users'])}\n"
            f"✅ Активных: {active}\n"
            f"🚫 Заблокированных: {blocked}\n"
            f"💰 Всего к выплате: {format_balance(total_pay)} ₽\n"
            f"🔧 Техработы: {maint}",
            parse_mode=ParseMode.HTML
        )
    elif txt == '👥 Пользователи' and is_owner(uid):
        await show_users(update, context)
    elif txt == '💸 Выплаты' and is_owner(uid):
        await show_payroll(update, context)

# ================================
# КОМАНДЫ ВЛАДЕЛЬЦЕВ
# ================================

async def cmd_givebonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("❌ Формат: /givebonus [ID] [сумма] [причина]")
        return
    try:
        target_id = int(args[0])
        amount = int(args[1])
        reason = ' '.join(args[2:])
    except ValueError:
        await update.message.reply_text("❌ ID и сумма должны быть числами.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    user['balance'] += amount
    save_data()
    await update.message.reply_text(
        f"✅ Выдан бонус <b>{format_balance(amount)} ₽</b> — {user['full_name']}\n"
        f"Причина: {reason}\nНовый баланс: {format_balance(user['balance'])} ₽",
        parse_mode=ParseMode.HTML
    )
    try:
        await context.bot.send_message(target_id, f"🎁 Вам начислен бонус <b>+{format_balance(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(context, update.effective_user.id, f"🎁 Выдал бонус {user['full_name']} (<code>{target_id}</code>): +{format_balance(amount)} ₽. Причина: {reason}")

async def cmd_takecash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("❌ Формат: /takecash [ID] [сумма] [причина]")
        return
    try:
        target_id = int(args[0])
        amount = int(args[1])
        reason = ' '.join(args[2:])
    except ValueError:
        await update.message.reply_text("❌ ID и сумма должны быть числами.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    old_bal = user['balance']
    user['balance'] = max(0, user['balance'] - amount)
    save_data()
    await update.message.reply_text(
        f"✅ Снято <b>{format_balance(amount)} ₽</b> у {user['full_name']}\n"
        f"Причина: {reason}\nНовый баланс: {format_balance(user['balance'])} ₽",
        parse_mode=ParseMode.HTML
    )
    try:
        await context.bot.send_message(target_id, f"⚠️ С вашего баланса снято <b>{format_balance(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(context, update.effective_user.id, f"➖ Снял {format_balance(amount)} ₽ у {user['full_name']} (<code>{target_id}</code>). Причина: {reason}")

async def cmd_makeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("❌ Формат: /makeadmin [ID]")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    if is_owner(target_id):
        await update.message.reply_text("⚠️ Этот пользователь уже является владельцем.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    DATA['admins'].add(target_id)
    user['has_access'] = True
    save_data()
    await update.message.reply_text(f"✅ {user['full_name']} назначен администратором.")
    try:
        await context.bot.send_message(target_id, "👮 Вам выданы права <b>администратора</b>!\nТеперь вы можете одобрять отчёты и выдавать доступ.", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(context, update.effective_user.id, f"👮 Назначил администратором: {user['full_name']} (<code>{target_id}</code>)")

async def cmd_takeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    if not context.args:
        await update.message.reply_text("❌ Формат: /takeadmin [ID]")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    if target_id in DATA['admins']:
        DATA['admins'].discard(target_id)
        user = DATA['users'].get(target_id)
        name = user['full_name'] if user else str(target_id)
        save_data()
        await update.message.reply_text(f"✅ Права администратора сняты с {name}.")
        try:
            await context.bot.send_message(target_id, "⚠️ Ваши права администратора были сняты.")
        except Exception: pass
        await log_action(context, update.effective_user.id, f"🔻 Снял права администратора с {name} (<code>{target_id}</code>)")
    else:
        await update.message.reply_text("⚠️ Этот пользователь не является администратором.")

async def cmd_giveds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    if not context.args:
        await update.message.reply_text("❌ Формат: /giveds [username или ID]")
        return
    arg = context.args[0]
    user = None
    target_id = None

    if arg.lstrip('@').isdigit() or arg.lstrip('-').isdigit():
        target_id = int(arg)
        user = DATA['users'].get(target_id)
    else:
        user = get_user_by_username(arg)
        if user:
            target_id = user['id']

    if not user:
        await update.message.reply_text(
            "❌ Пользователь не найден в базе бота.\n"
            "Чтобы выдать доступ, пользователь должен хотя бы один раз написать боту /start."
        )
        return

    if user['has_access']:
        await update.message.reply_text(f"⚠️ У {user['full_name']} уже есть доступ.")
        return

    user['has_access'] = True
    user['blocked'] = False
    save_data()
    granter = DATA['users'].get(update.effective_user.id)
    granter_name = granter['full_name'] if granter else update.effective_user.full_name
    await update.message.reply_text(f"✅ Доступ выдан: <b>{user['full_name']}</b> (<code>{target_id}</code>)", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(target_id, f"✅ <b>Доступ выдан!</b>\nВыдал: {granter_name}\nВведите ваш <b>номер счета</b>:", parse_mode=ParseMode.HTML)
    except Exception: pass
    await log_action(context, update.effective_user.id, f"✅ Выдал доступ (команда) {user['full_name']} (<code>{target_id}</code>)")

async def cmd_texwork(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев.")
        return
    reason = ' '.join(context.args) if context.args else 'Технические работы'
    DATA['maintenance'] = {'active': True, 'reason': reason}
    save_data()
    await update.message.reply_text(f"🔧 Бот переведён в режим техработ.\nПричина: {reason}")
    await log_action(context, update.effective_user.id, f"🔧 Включил техработы. Причина: {reason}")

async def cmd_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев.")
        return
    reason = ' '.join(context.args) if context.args else 'Работа возобновлена'
    DATA['maintenance'] = {'active': False, 'reason': ''}
    save_data()
    await update.message.reply_text(f"✅ Бот включён. {reason}")
    await log_action(context, update.effective_user.id, f"✅ Выключил техработы. {reason}")

async def cmd_sozvat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = DATA['users'].get(user_id)
    if not user or not user['has_access'] or user.get('blocked'):
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    nick = user.get('nick') or user['full_name']
    msg = f"🔔 <b>Нужна помощь на стройке!</b>\n\nОтправил: <b>{nick}</b>"
    count = 0
    for u in DATA['users'].values():
        if u['has_access'] and not u.get('blocked') and u['id'] != user_id:
            try:
                await context.bot.send_message(u['id'], msg, parse_mode=ParseMode.HTML)
                count += 1
            except Exception: pass
    await update.message.reply_text(f"✅ Оповещение отправлено {count} пользователям.")

async def cmd_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    if not context.args:
        await update.message.reply_text("❌ Формат: /msg [текст]")
        return
    sender = DATA['users'].get(update.effective_user.id)
    nick = (sender.get('nick') or sender['full_name']) if sender else update.effective_user.full_name
    text = ' '.join(context.args)
    broadcast = f"📢 {text}\n\n— <i>by {nick}</i>"
    count = 0
    for u in DATA['users'].values():
        if u['has_access'] and not u.get('blocked'):
            try:
                await context.bot.send_message(u['id'], broadcast, parse_mode=ParseMode.HTML)
                count += 1
            except Exception: pass
    await update.message.reply_text(f"✅ Сообщение отправлено {count} пользователям.")

async def cmd_bank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = DATA['users'].get(user_id)
    if not user or not user['has_access'] or user.get('blocked'):
        await update.message.reply_text("❌ У вас нет доступа к этой команде.")
        return
    if not context.args:
        current = user.get('bank_account') or 'не указан'
        await update.message.reply_text(f"💳 Текущий счёт: <b>{current}</b>\n\nЧтобы изменить: /bank (номер счёта)", parse_mode=ParseMode.HTML)
        return
    account = ' '.join(context.args)
    user['bank_account'] = account
    save_data()
    await update.message.reply_text(f"✅ Банковский счёт сохранён: <b>{account}</b>", parse_mode=ParseMode.HTML)

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = update.effective_user
    username = f"@{u.username}" if u.username else "нет"
    await update.message.reply_text(
        f"🆔 Ваш Telegram ID: <code>{uid}</code>\n👤 Username: {username}",
        parse_mode=ParseMode.HTML
    )

async def cmd_checkid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    if not context.args:
        await update.message.reply_text("❌ Формат: /checkid [username]")
        return
    user = get_user_by_username(context.args[0])
    if not user:
        await update.message.reply_text("❌ Пользователь с таким username не найден в базе бота.")
        return
    role = "👑 Владелец" if is_owner(user['id']) else ("👮 Администратор" if user['id'] in DATA['admins'] else "👤 Пользователь")
    status = "🚫 Заблокирован" if user.get('blocked') else ("✅ Активен" if user['has_access'] else "⏳ Без доступа")
    await update.message.reply_text(
        f"🔎 <b>{user['full_name']}</b>\n"
        f"🆔 ID: <code>{user['id']}</code>\n"
        f"👤 Username: @{user.get('username') or '—'}\n"
        f"🎭 Роль: {role}\n"
        f"📶 Статус: {status}",
        parse_mode=ParseMode.HTML
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    total_users = len(DATA['users'])
    active_users = sum(1 for u in DATA['users'].values() if u['has_access'] and not u.get('blocked'))
    blocked_users = sum(1 for u in DATA['users'].values() if u.get('blocked'))
    total_reports = DATA['report_counter'] - 1
    approved = sum(1 for r in DATA['reports'].values() if r['status'] == 'approved')
    pending = sum(1 for r in DATA['reports'].values() if r['status'] == 'pending')
    total_paid = sum(u['balance'] for u in DATA['users'].values())
    admins_count = len(DATA['admins'])
    maint = f"🔧 ВКЛ ({DATA['maintenance']['reason']})" if is_maintenance() else "✅ ВЫКЛ"
    text = (f"📊 <b>СТАТИСТИКА БОТА</b>\n\n"
            f"👥 Всего пользователей: <b>{total_users}</b>\n"
            f"✅ С доступом: <b>{active_users}</b>\n"
            f"🚫 Заблокировано: <b>{blocked_users}</b>\n"
            f"👮 Администраторов: <b>{admins_count}</b>\n\n"
            f"📋 Всего отчётов: <b>{total_reports}</b>\n"
            f"✅ Одобрено: <b>{approved}</b>\n"
            f"⏳ На рассмотрении: <b>{pending}</b>\n\n"
            f"💰 Общий долг по зарплате: <b>{format_balance(total_paid)} ₽</b>\n"
            f"🔧 Техработы: {maint}")
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    if not context.args:
        await update.message.reply_text("❌ Формат: /userinfo [ID]")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    role = "👑 Владелец" if is_owner(target_id) else ("👮 Администратор" if target_id in DATA['admins'] else "👤 Пользователь")
    status = "🚫 Заблокирован" if user.get('blocked') else ("✅ Активен" if user['has_access'] else "⏳ Без доступа")
    await update.message.reply_text(
        f"👤 <b>{user['full_name']}</b>\n"
        f"🆔 ID: <code>{target_id}</code>\n"
        f"👤 Username: @{user.get('username') or '—'}\n"
        f"🎭 Роль: {role}\n"
        f"📶 Статус: {status}\n"
        f"🏷 Ник: {user.get('nick') or '—'}\n"
        f"💳 Счёт: {user.get('bank_account') or '—'}\n"
        f"💰 Баланс: <b>{format_balance(user['balance'])} ₽</b>\n"
        f"📋 Отчётов: <b>{user['total_reports']}</b>",
        parse_mode=ParseMode.HTML
    )

async def cmd_listadmins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("❌ Только для владельцев.")
        return
    lines = ["👑 <b>ВЛАДЕЛЬЦЫ:</b>"]
    for oid in OWNER_IDS:
        u = DATA['users'].get(oid)
        name = u['full_name'] if u else str(oid)
        lines.append(f"  • {name} (<code>{oid}</code>)")
    lines.append("\n👮 <b>АДМИНИСТРАТОРЫ:</b>")
    if DATA['admins']:
        for aid in DATA['admins']:
            u = DATA['users'].get(aid)
            name = u['full_name'] if u else str(aid)
            lines.append(f"  • {name} (<code>{aid}</code>)")
    else:
        lines.append("  — нет администраторов")
    await update.message.reply_text('\n'.join(lines), parse_mode=ParseMode.HTML)

async def cmd_otcets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Только для администраторов.")
        return
    if not context.args:
        await update.message.reply_text("❌ Формат: /otcets [ID пользователя]")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден.")
        return
    reps = [r for r in DATA['reports'].values() if r['user_id'] == target_id]
    if not reps:
        await update.message.reply_text(f"📋 У {user['full_name']} нет отчётов.")
        return
    await update.message.reply_text(
        f"📋 <b>Отчёты — {user['full_name']}</b>\nВсего: {len(reps)}",
        parse_mode=ParseMode.HTML
    )
    for r in reps:
        st = "✅ Одобрен" if r['status'] == 'approved' else "⏳ На рассмотрении"
        date = r['at'].strftime('%d.%m.%Y %H:%M') if isinstance(r['at'], datetime) else '—'
        cls = "Высокий" if r['class'] == 'high' else "Средний"
        await update.message.reply_text(
            f"<b>Отчет #{r['id']}</b>\n"
            f"👤 Ник: {r['nick']}\n"
            f"🏗 Класс: {cls}\n"
            f"💰 Сумма: {format_balance(r['pay'])} ₽\n"
            f"⏰ КД: минута {r['cd']}\n"
            f"📅 Дата: {date}\n"
            f"📶 Статус: {st}",
            parse_mode=ParseMode.HTML
        )

# ================================
# ГЛАВНАЯ ФУНКЦИЯ
# ================================

def main():
    load_data()

    app = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^📝 Отправить отчет$'), report_start)],
        states={
            REPORT_PHOTO1: [MessageHandler(filters.PHOTO, report_photo1)],
            REPORT_PHOTO2: [MessageHandler(filters.PHOTO, report_photo2)],
            REPORT_NICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_nick)],
            REPORT_CLASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_class)],
            REPORT_CD: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_cd)],
        },
        fallbacks=[
            CommandHandler('start', start),
            MessageHandler(filters.Regex('^📝 Отправить отчет$'), report_start),
        ],
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('givebonus', cmd_givebonus))
    app.add_handler(CommandHandler('takecash', cmd_takecash))
    app.add_handler(CommandHandler('makeadmin', cmd_makeadmin))
    app.add_handler(CommandHandler('takeadmin', cmd_takeadmin))
    app.add_handler(CommandHandler('sozvat', cmd_sozvat))
    app.add_handler(CommandHandler('msg', cmd_msg))
    app.add_handler(CommandHandler('bank', cmd_bank))
    app.add_handler(CommandHandler('id', cmd_id))
    app.add_handler(CommandHandler('checkid', cmd_checkid))
    app.add_handler(CommandHandler('stats', cmd_stats))
    app.add_handler(CommandHandler('userinfo', cmd_userinfo))
    app.add_handler(CommandHandler('listadmins', cmd_listadmins))
    app.add_handler(CommandHandler('otcets', cmd_otcets))
    app.add_handler(CommandHandler('giveds', cmd_giveds))
    app.add_handler(CommandHandler('texwork', cmd_texwork))
    app.add_handler(CommandHandler('on', cmd_on))
    app.add_handler(CallbackQueryHandler(request_access, pattern='^request_access$'))
    app.add_handler(CallbackQueryHandler(grant_access, pattern='^grant_'))
    app.add_handler(CallbackQueryHandler(approve_report, pattern='^appr_'))
    app.add_handler(CallbackQueryHandler(manage_user, pattern='^(revoke|block|unblock)_'))
    app.add_handler(CallbackQueryHandler(handle_payroll, pattern='^pay'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.run_polling()

if __name__ == '__main__':
    main()
