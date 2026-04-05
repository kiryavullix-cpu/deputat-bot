#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DEPUTAT - Автоматизированный бот для отчетов
ВЕРСИЯ БЕЗ БД (Вся логика сохранена)
"""

import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, InputMediaPhoto
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, ConversationHandler, filters
from telegram.constants import ParseMode

# ================================
# НАСТРОЙКИ
# ================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8724153136:AAFhD24OvSoepxott4H-9WodBJAd-1rUh7U")

# ID владельцев
OWNER_IDS = [6693142204, 5711452887]

# Суммы выплат
PAYMENTS = {
    'high': 350000,    # Высокий класс
    'medium': 200000   # Средний класс
}

# ================================
# ВРЕМЕННОЕ ХРАНИЛИЩЕ (Вместо БД)
# ================================
# Здесь храним всех юзеров и их отчеты прямо в памяти программы
DATA = {
    'users': {},      # {user_id: {данные пользователя}}
    'reports': {},    # {report_id: {данные отчета}}
    'report_counter': 1,
    'admins': set()   # ID пользователей с правами администратора
}

# ================================
# ЛОГИРОВАНИЕ
# ================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Состояния диалога
REPORT_PHOTO1, REPORT_PHOTO2, REPORT_NICK, REPORT_CLASS, REPORT_CD = range(5)

# ================================
# УТИЛИТЫ (Твоя логика)
# ================================

def is_owner(user_id):
    return user_id in OWNER_IDS

def is_admin(user_id):
    return user_id in OWNER_IDS or user_id in DATA['admins']

def get_all_privileged():
    return list(OWNER_IDS) + [uid for uid in DATA['admins'] if uid not in OWNER_IDS]

def detect_class(text):
    text = text.lower()
    high_patterns = ['выс', 'высок', 'вышка', 'high', 'хай']
    for pattern in high_patterns:
        if pattern in text: return 'high', PAYMENTS['high']
    medium_patterns = ['сред', 'medium', 'ср', 'мед']
    for pattern in medium_patterns:
        if pattern in text: return 'medium', PAYMENTS['medium']
    return None, 0

def format_balance(amount):
    return f"{amount:,}".replace(',', ' ')

def get_cd_status_and_time(cd_minute):
    now = datetime.now()
    curr_m, curr_h = now.minute, now.hour
    if curr_m < cd_minute:
        return True, f"{curr_h:02d}:{cd_minute:02d}"
    else:
        return False, f"{(curr_h + 1) % 24:02d}:{cd_minute:02d}"

# ================================
# КОМАНДЫ И ОБРАБОТЧИКИ
# ================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    u = update.effective_user

    # Если владелец и ещё не зарегистрирован — регистрируем автоматически с полным доступом
    if is_owner(user_id) and user_id not in DATA['users']:
        DATA['users'][user_id] = {
            'id': user_id, 'telegram_id': user_id, 'username': u.username, 'full_name': u.full_name,
            'has_access': True, 'balance': 0, 'total_reports': 0, 'bank_account': 'owner', 'nick': None, 'blocked': False
        }

    user = DATA['users'].get(user_id)

    if user:
        if user['has_access']:
            keyboard = [['📝 Отправить отчет', '💰 Баланс'], ['📊 История строек', '⏰ КД']]
            if is_owner(user_id): keyboard.append(['👑 Панель владельца', '👥 Пользователи', '💸 Выплаты'])
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                f"👋 Привет, <b>{user['full_name']}</b>!\n💼 Баланс: <b>{format_balance(user['balance'])} ₽</b>\n"
                f"🏗️ Строек: <b>{user['total_reports']}</b>",
                reply_markup=reply_markup, parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text("⏳ Ваш запрос на доступ еще на рассмотрении.")
    else:
        keyboard = [[InlineKeyboardButton("🔑 Получить доступ", callback_data="request_access")]]
        await update.message.reply_text("🏗️ <b>DEPUTAT - Система отчетов</b>\n\nНажми кнопку ниже:", 
                                       reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)

async def request_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    u = query.from_user

    if u.id in DATA['users']:
        await query.message.reply_text("⏳ Запрос уже отправлен.")
        return

    DATA['users'][u.id] = {
        'id': u.id, 'telegram_id': u.id, 'username': u.username, 'full_name': u.full_name,
        'has_access': False, 'balance': 0, 'total_reports': 0, 'bank_account': None, 'nick': None, 'blocked': False
    }

    for owner_id in get_all_privileged():
        try:
            btn = [[InlineKeyboardButton("✅ Выдать доступ", callback_data=f"grant_{u.id}")]]
            await context.bot.send_message(owner_id, f"🔔 <b>Новый запрос!</b>\nЮзер: {u.full_name}\nID: {u.id}", 
                                         reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML)
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
        granter_name = query.from_user.full_name
        await query.message.edit_text(f"✅ Доступ для {target_id} выдан администратором {granter_name}!")
        await context.bot.send_message(target_id, "✅ <b>Доступ одобрен!</b>\nВведите ваш <b>номер счета</b>:", parse_mode=ParseMode.HTML)
        for o_id in get_all_privileged():
            if o_id != query.from_user.id:
                try:
                    await context.bot.send_message(o_id, f"ℹ️ Доступ пользователю {target_id} выдан администратором {granter_name}.")
                except Exception: pass

# ================================
# ЛОГИКА ОТЧЕТОВ (Твой основной функционал)
# ================================

async def report_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = DATA['users'].get(update.effective_user.id)
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
    await update.message.reply_text("🏗️ Напишите <b>класс стройки</b> (Высокий/Средний):", parse_mode=ParseMode.HTML)
    return REPORT_CLASS

async def report_class(update: Update, context: ContextTypes.DEFAULT_TYPE):
    b_class, pay = detect_class(update.message.text)
    if not b_class: return REPORT_CLASS
    context.user_data['building_class'] = b_class
    context.user_data['payment'] = pay
    await update.message.reply_text(f"💰 Оплата: {format_balance(pay)} ₽\n⏰ Напишите <b>минуту КД</b> (0-59):", parse_mode=ParseMode.HTML)
    return REPORT_CD

async def report_cd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cd_min = int(update.message.text)
        if not 0 <= cd_min <= 59: raise ValueError
    except: return REPORT_CD

    user_id = update.effective_user.id
    user = DATA['users'][user_id]
    active, cd_t = get_cd_status_and_time(cd_min)

    rep_id = DATA['report_counter']
    DATA['reports'][rep_id] = {
        'id': rep_id, 'user_id': user_id, 'nick': context.user_data['nick'],
        'class': context.user_data['building_class'], 'pay': context.user_data['payment'],
        'cd': cd_min, 'p1': context.user_data['photo_start'], 'p2': context.user_data['photo_end'],
        'status': 'pending', 'at': datetime.now(), 'msg_ids': {}
    }
    DATA['report_counter'] += 1

    # Уведомление владельцам и админам
    txt = (f"📋 <b>ОТЧЕТ #{rep_id}</b>\n"
           f"👤 Ник: {context.user_data['nick']}\n"
           f"🏗 Класс: {context.user_data['building_class']}\n"
           f"💰 {format_balance(context.user_data['payment'])} ₽\n"
           f"💳 Счёт: {user['bank_account']}")
    btn = [[InlineKeyboardButton("✅ ОДОБРИТЬ", callback_data=f"appr_{rep_id}")]]
    media_group = [
        InputMediaPhoto(media=context.user_data['photo_start'], caption="📸 Начало стройки"),
        InputMediaPhoto(media=context.user_data['photo_end'], caption="📸 Конец стройки"),
    ]

    for o_id in get_all_privileged():
        try:
            sent_media = await context.bot.send_media_group(o_id, media_group)
            sent_btn = await context.bot.send_message(o_id, txt, reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML)
            media_ids = [m.message_id for m in sent_media]
            DATA['reports'][rep_id]['msg_ids'][o_id] = media_ids + [sent_btn.message_id]
        except Exception as e:
            logger.warning(f"Не удалось отправить отчет {o_id}: {e}")

    await update.message.reply_text(f"✅ Отчет #{rep_id} отправлен! КД до: {cd_t}")
    context.user_data.clear()
    return ConversationHandler.END

async def approve_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id): return
    rep_id = int(query.data.split('_')[1])
    rep = DATA['reports'].get(rep_id)

    if not rep:
        return
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
        # Удаляем все сообщения отчёта у всех владельцев/админов
        for o_id, msg_ids in rep.get('msg_ids', {}).items():
            for mid in msg_ids:
                try:
                    await context.bot.delete_message(o_id, mid)
                except Exception: pass
        # Если msg_ids не было (старый формат) — просто удаляем текущее сообщение
        if not rep.get('msg_ids'):
            try:
                await query.message.delete()
            except Exception: pass
        await context.bot.send_message(uid, f"🎉 Отчет #{rep_id} одобрен! +{format_balance(rep['pay'])} ₽")
        # Уведомить остальных привилегированных что уже одобрено
        for o_id in get_all_privileged():
            if o_id != query.from_user.id:
                try:
                    await context.bot.send_message(o_id, f"ℹ️ Отчет #{rep_id} ({rep['nick']}) одобрен — {approver_name}.")
                except Exception: pass

# ================================
# УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ (для владельцев)
# ================================

async def show_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid): return
    users = [u for u in DATA['users'].values() if not is_owner(u['id'])]
    if not users:
        await update.message.reply_text("👥 Пользователей пока нет.")
        return
    for u in users:
        status = "🚫 Заблокирован" if u.get('blocked') else ("✅ Активен" if u['has_access'] else "⏳ Ожидает")
        nick = f" | Ник: {u['nick']}" if u.get('nick') else ""
        text = (f"👤 <b>{u['full_name']}</b>{nick}\n"
                f"🆔 {u['id']}\n"
                f"💰 {format_balance(u['balance'])} ₽ | Строек: {u['total_reports']}\n"
                f"Статус: {status}")
        btns = []
        if u['has_access'] and not u.get('blocked'):
            btns.append(InlineKeyboardButton("❌ Забрать доступ", callback_data=f"revoke_{u['id']}"))
        if not u.get('blocked'):
            btns.append(InlineKeyboardButton("🚫 Заблокировать", callback_data=f"block_{u['id']}"))
        else:
            btns.append(InlineKeyboardButton("✅ Разблокировать", callback_data=f"unblock_{u['id']}"))
        markup = InlineKeyboardMarkup([btns]) if btns else None
        await update.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

async def manage_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id): return

    action, target_id = query.data.split('_', 1)
    target_id = int(target_id)
    user = DATA['users'].get(target_id)
    if not user: return

    if action == 'revoke':
        user['has_access'] = False
        await query.message.edit_text(query.message.text + "\n\n❌ Доступ забран", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "❌ Ваш доступ был отозван владельцем.")
        except Exception: pass
    elif action == 'block':
        user['blocked'] = True
        user['has_access'] = False
        await query.message.edit_text(query.message.text + "\n\n🚫 Пользователь заблокирован", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "🚫 Вы были заблокированы владельцем.")
        except Exception: pass
    elif action == 'unblock':
        user['blocked'] = False
        await query.message.edit_text(query.message.text + "\n\n✅ Пользователь разблокирован", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(target_id, "✅ Вы были разблокированы владельцем.")
        except Exception: pass

# ================================
# СИСТЕМА ВЫПЛАТ (для владельцев)
# ================================

async def show_payroll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_owner(uid): return
    users_with_balance = [u for u in DATA['users'].values() if u['balance'] > 0 and not is_owner(u['id'])]
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
        text = (f"👤 <b>{u['full_name']}</b>{nick}\n"
                f"💳 Счёт: {u['bank_account']}\n"
                f"💰 К выплате: <b>{format_balance(u['balance'])} ₽</b>")
        btn = [[InlineKeyboardButton("✅ ЗП выдано", callback_data=f"pay_{u['id']}")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(btn), parse_mode=ParseMode.HTML)

async def handle_payroll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not is_owner(query.from_user.id): return

    if query.data == 'pay_all':
        count = 0
        for u in DATA['users'].values():
            if u['balance'] > 0 and not is_owner(u['id']):
                u['balance'] = 0
                count += 1
                try:
                    await context.bot.send_message(u['id'], "✅ Ваша зарплата была выдана! Баланс обнулён.")
                except Exception: pass
        await query.message.edit_text(f"✅ <b>ЗП выдана всем!</b> Обнулено: {count} пользователей.", parse_mode=ParseMode.HTML)
    elif query.data.startswith('pay_'):
        target_id = int(query.data.split('_')[1])
        user = DATA['users'].get(target_id)
        if user and user['balance'] > 0:
            user['balance'] = 0
            await query.message.edit_text(query.message.text + "\n\n✅ <b>ЗП выдана! Баланс обнулён.</b>", parse_mode=ParseMode.HTML)
            try:
                await context.bot.send_message(target_id, "✅ Ваша зарплата была выдана! Баланс обнулён.")
            except Exception: pass
        else:
            await query.message.edit_text(query.message.text + "\n\n⚠️ Баланс уже 0.", parse_mode=ParseMode.HTML)

# ================================
# ОСТАЛЬНЫЕ КНОПКИ
# ================================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text
    uid = update.effective_user.id
    user = DATA['users'].get(uid)

    if not user: return

    # Сохранение счета, если его еще нет
    if user['has_access'] and user['bank_account'] is None and txt.isdigit():
        user['bank_account'] = txt
        await update.message.reply_text(f"✅ Счет {txt} сохранен! Можете слать отчеты.")
        return

    if txt == '💰 Баланс':
        await update.message.reply_text(f"💼 Баланс: <b>{format_balance(user['balance'])} ₽</b>", parse_mode=ParseMode.HTML)
    elif txt == '⏰ КД':
        # Берем последний отчет
        last = list(DATA['reports'].values())[-1] if DATA['reports'] else None
        if last:
            act, t = get_cd_status_and_time(last['cd'])
            await update.message.reply_text(f"⏰ КД до: <b>{t}</b>\nУстановил: {last['nick']}", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("КД еще не ставили.")
    elif txt == '📊 История строек':
        u_reps = [r for r in DATA['reports'].values() if r['user_id'] == uid][-5:]
        msg = "📊 <b>Последние 5 отчетов:</b>\n\n"
        for r in u_reps:
            st = "✅" if r['status'] == 'approved' else "⏳"
            msg += f"{st} Отчет #{r['id']} — {format_balance(r['pay'])} ₽\n"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    elif txt == '👑 Панель владельца' and is_owner(uid):
        total_pay = sum(u['balance'] for u in DATA['users'].values())
        active = sum(1 for u in DATA['users'].values() if u['has_access'] and not u.get('blocked'))
        blocked = sum(1 for u in DATA['users'].values() if u.get('blocked'))
        await update.message.reply_text(
            f"👑 <b>АДМИН-ИНФО</b>\n\n"
            f"👥 Всего юзеров: {len(DATA['users'])}\n"
            f"✅ Активных: {active}\n"
            f"🚫 Заблокированных: {blocked}\n"
            f"💰 Всего выплат: {format_balance(total_pay)} ₽",
            parse_mode=ParseMode.HTML
        )
    elif txt == '👥 Пользователи' and is_owner(uid):
        await show_users(update, context)
    elif txt == '💸 Выплаты' and is_owner(uid):
        await show_payroll(update, context)

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
    await update.message.reply_text(f"✅ Выдан бонус <b>{format_balance(amount)} ₽</b> пользователю {user['full_name']}.\nПричина: {reason}\nНовый баланс: {format_balance(user['balance'])} ₽", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(target_id, f"🎁 Вам начислен бонус <b>+{format_balance(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass

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
    user['balance'] = max(0, user['balance'] - amount)
    await update.message.reply_text(f"✅ Снято <b>{format_balance(amount)} ₽</b> у пользователя {user['full_name']}.\nПричина: {reason}\nНовый баланс: {format_balance(user['balance'])} ₽", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(target_id, f"⚠️ С вашего баланса снято <b>{format_balance(amount)} ₽</b>\nПричина: {reason}", parse_mode=ParseMode.HTML)
    except Exception: pass

async def cmd_makeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = context.args
    if not args:
        await update.message.reply_text("❌ Формат: /makeadmin [ID]")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    if is_owner(target_id):
        await update.message.reply_text("⚠️ Этот пользователь уже является владельцем.")
        return
    user = DATA['users'].get(target_id)
    if not user:
        await update.message.reply_text("❌ Пользователь не найден. Убедитесь что он писал боту.")
        return
    DATA['admins'].add(target_id)
    user['has_access'] = True
    await update.message.reply_text(f"✅ Пользователь {user['full_name']} ({target_id}) назначен администратором.")
    try:
        await context.bot.send_message(target_id, "👮 Вам выданы права <b>администратора</b>!\nТеперь вы можете одобрять отчёты и выдавать доступ.", parse_mode=ParseMode.HTML)
    except Exception: pass

async def cmd_takeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update.effective_user.id): return
    args = context.args
    if not args:
        await update.message.reply_text("❌ Формат: /takeadmin [ID]")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом.")
        return
    if target_id in DATA['admins']:
        DATA['admins'].discard(target_id)
        user = DATA['users'].get(target_id)
        name = user['full_name'] if user else str(target_id)
        await update.message.reply_text(f"✅ Права администратора сняты с {name}.")
        try:
            await context.bot.send_message(target_id, "⚠️ Ваши права администратора были сняты.")
        except Exception: pass
    else:
        await update.message.reply_text("⚠️ Этот пользователь не является администратором.")

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
    nick = sender.get('nick') or sender['full_name'] if sender else update.effective_user.full_name
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

def main():
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
    app.add_handler(CallbackQueryHandler(request_access, pattern='^request_access$'))
    app.add_handler(CallbackQueryHandler(grant_access, pattern='^grant_'))
    app.add_handler(CallbackQueryHandler(approve_report, pattern='^appr_'))
    app.add_handler(CallbackQueryHandler(manage_user, pattern='^(revoke|block|unblock)_'))
    app.add_handler(CallbackQueryHandler(handle_payroll, pattern='^pay'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.run_polling()

if __name__ == '__main__':
    main()
