import os

from aiogram import F, Router, Bot
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import asyncio

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton



import app.keyboards as kb


from requests import (get_or_create_user, get_user_data, get_user_by_tg_id, 
                      get_user_points,create_support_ticket, update_ticket_with_answer, 
                      get_ticket_by_group_message, get_user_tickets,
                      close_ticket, update_phone_universal)
import logging
import re

router = Router()
logger = logging.getLogger(__name__)

SUPPORT_GROUP_ID = "-1003396880757"

class QuestionState(StatesGroup):
    waiting_for_question = State()

class PhoneState(StatesGroup):
    waiting_for_phone = State()

@router.message(CommandStart())
async def cmd_start(message: Message):
    referrer_code = None
    if len(message.text.split()) > 1:
        referrer_code = message.text.split()[1]
    
    logger.info(f"User {message.from_user.first_name} ({message.from_user.id}) started")
    
    # Получаем данные пользователя
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    username = message.from_user.username
    
    # Создаем или получаем пользователя (теперь с именем)
    user_id = get_or_create_user(
        tg_id=message.from_user.id,
        first_name=first_name,      # ← Передаем имя
        last_name=last_name,        # ← Передаем фамилию
        username=username,          # ← Передаем @username
        referrer_code=referrer_code
    )
    
    if user_id:
        # Получаем данные пользователя
        user_data = get_user_data(message.from_user.id)
        if user_data and user_data.get('phone'):
            await message.answer(
                f'Привет! Я бонусный помощник PONNY PRINT 🎁\n\n'
                f'У меня можно:\n• Посмотреть свои баллы\n• Получить скидку для друга\n• Узнать условия',
                reply_markup=kb.main
            )
        else:
            await message.answer(
                f"Чтобы посмотреть ваши баллы, мне нужно найти вас в системе\n\n"
                f"Пожалуйста, выберите удобный способ для отправки номера.",
                reply_markup=kb.phone_alt
            )
    else:
        await message.answer(
            "Отправьте номер телефона:",
            reply_markup=kb.phone_request
        )

# Обработчик кнопки "Поделиться контактом"
@router.callback_query(F.data == 'share_contact')
async def share_contact_handler(callback: CallbackQuery):
    await callback.message.edit_text(
        "Нажмите на скрепку 📎 рядом с полем ввода сообщения → "
        "«Контакты» → «Отправить мой контакт»"
        "Используйте номер который вы указывали при заказе и/или будете использовать при новом заказе!",
        reply_markup=kb.phone_menu
    )
    await callback.answer()

# Обработчик кнопки "Ввести вручную"
@router.callback_query(F.data == 'enter_manual')
async def enter_manual_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите ваш номер телефона в формате:\n"
        "• +79991234567\n"
        "• 89991234567\n"
        "• 9991234567\n"
        "Вводите номер который вы указывали при заказе и/или будете использовать при новом заказе!",
        reply_markup=kb.phone_menu
    )
    await state.set_state(PhoneState.waiting_for_phone)
    await callback.answer()

# Обработчик ввода телефона вручную
@router.message(PhoneState.waiting_for_phone)
async def process_manual_phone(message: Message, state: FSMContext, bot: Bot):
    phone = message.text.strip()
    
    # Показываем "печатает..."
    await bot.send_chat_action(message.chat.id, "typing")
    await asyncio.sleep(1)
    
    # Используем универсальную функцию
    success, result_msg = update_phone_universal(message.from_user.id, phone)
    
    if success:
        # Получаем текущие баллы пользователя
        from requests import get_user_data
        user_data = get_user_data(message.from_user.id)
        
        total_points = 0
        if user_data:
            total_points = user_data.get('points_manual', 0) + user_data.get('points_referral', 0)
        
        if "приветственных" in result_msg:
            # Если начислены приветственные баллы
            await message.answer(
                f"{result_msg}\n\n"
                f'Привет! Я бонусный помощник PONNY PRINT 🎁\n\n'
                f'У меня можно:\n• Посмотреть свои баллы\n• Получить скидку для друга\n• Узнать условия',
                parse_mode="Markdown",
                reply_markup=kb.main
            )
        else:
            # Обычное сообщение
            await message.answer(
                f'Привет! Я бонусный помощник PONNY PRINT 🎁\n\n'
                f'У меня можно:\n• Посмотреть свои баллы\n• Получить скидку для друга\n• Узнать условия',
                parse_mode="Markdown",
                reply_markup=kb.main
            )
    else:
        # Ошибка
        await message.answer(
            result_msg,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="enter_manual")]
            ])
        )
    
    await state.clear()

# Обработчик кнопки "Назад" в меню телефона
@router.callback_query(F.data == 'back_to_phone_menu')
async def back_to_phone_menu(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите новый номер телефона в формате:\n"
        "• +79991234567\n"
        "• 89991234567\n"
        "• 9991234567",
        reply_markup=kb.cancelchange
    )
    await state.set_state(PhoneState.waiting_for_phone)
    await callback.answer()

@router.callback_query(F.data == 'mypoints')
async def mypoints_handler(callback: CallbackQuery):
    user_data = get_user_data(callback.from_user.id)
    
    if user_data and user_data.get('phone'):
        points = get_user_points(callback.from_user.id)
        
        # Проверяем, есть ли хоть какие-то баллы
        has_points = points['manual'] > 0 or points['referral'] > 0
        
        if has_points:
            await callback.message.edit_text(
                f"✅ Отлично! Я всё нашел\n\nВаш баланс: \n\n"
                f"{points['manual']} баллов за заказы\n"
                f"{points['referral']} баллов за приглашенного друга\n"
                f'💡 1 балл = 1 рубль\n\n'
                f'Кстати, их можно потратить при следующем заказе.\n'
                f'Мы сами проверим их наличие и учтём при оформлении заказа',
                reply_markup=kb.tomain
            )
        else:
            # Клавиатура для случая "нет баллов"
            
            await callback.message.edit_text(
                "❌ Пу Пу Пу…\n"
                "Не могу найти информацию по этому номеру.\n\n"
                "Возможные причины:\n"
                "• Вы указали другой номер при заказе\n"
                "• У вас еще нет бонусных баллов\n"
                "• Техническая ошибка\n\n"
                "Что делать:\n"
                "1. Проверьте номер телефона\n"
                "2. Обратитесь к менеджеру",
                reply_markup=kb.no_points_keyboard
            )
    else:
        # Если нет телефона вообще
        await callback.message.answer(
            "Для просмотра баллов нужен номер телефона.\n"
            "Выберите способ ввода:",
            reply_markup=kb.phone_menu
        )
    
    await callback.answer()

@router.callback_query(F.data == 'change_phone')
async def change_phone_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите новый номер телефона в формате:\n"
        "• +79991234567\n"
        "• 89991234567\n"
        "• 9991234567",
        reply_markup=kb.cancelchange
    )
    await state.set_state(PhoneState.waiting_for_phone)
    await callback.answer()

@router.callback_query(F.data == 'change_phone_for_ask')
async def change_phone_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "Введите новый номер телефона в формате:\n"
        "• +79991234567\n"
        "• 89991234567\n"
        "• 9991234567",
        reply_markup=kb.cancelchangeforask
    )
    await state.set_state(PhoneState.waiting_for_phone)
    await callback.answer()

@router.callback_query(F.data == 'referral')
async def referral_handler(callback: CallbackQuery):
    user = get_user_by_tg_id(callback.from_user.id)
    
    if user and user.phone:
        bot_username = "@testtestoksanabotbot_bot"  # ЗАМЕНИ НА СВОЙ
        
        referral_link = f"https://t.me/{bot_username.lstrip('@')}?start={user.referral_code}"

        await callback.message.edit_text(
            f"🎁 Подарите другу скидку 500₽ на первый заказ!\n\n"
            f'Каждый новый заказ по вашей ссылке приносит вам 500 бонусных баллов и 500 баллов для друга.\n'
            f'И, конечно, улучшает ваш рейтинг 📈\n' 
            f'На данный момент ваша максимальная скидка 15%\n\n'
            f"Условия для друга:\n"
            f'Скидка действует на любой заказ\n'
            f'Минимальная сумма заказа — 3000 руб.\n'
            f'Бонусы не суммируется с другими акциями\n'
            f'Максимальная сумма скидки 15%:\n\n'
            f'Только🤫тсс… бонусов на всех не хватит, ссылку можно отправить только избранным 🫶🏻\n\n'
            f"🔗 Персональная ссылка для тебя:\n"
            f"{referral_link}",
            reply_markup=kb.tomain
        )
    else:
        await callback.message.answer(
            "Пожалуйста, сначала зарегистрируйтесь, отправив номер телефона:",
            reply_markup=kb.phone_request
        )
    await callback.answer()

# Обработчик для получения номера телефона
@router.message(F.contact)
async def process_phone(message: Message):
    if message.contact:
        phone = message.contact.phone_number
        logger.info(f"User {message.from_user.id} sent phone: {phone}")
        
        # Используй отдельную функцию для обновления телефона
        from requests import update_user_phone
        
        if update_user_phone(message.from_user.id, phone):
            await message.answer(
                f"Отлично, номер успешно привязан👌🏻\n\n"
                f"Теперь вы можете использовать бонусы PONNY PRINT\n"
                f'А ещё:\n• Посмотреть свои баллы\n• Получить скидку для друга\n• Узнать условия',
                reply_markup=kb.main
            )
        else:
            await message.answer(
                "❌ Произошла ошибка. Попробуйте позже.",
                reply_markup=kb.main
            )

@router.callback_query(F.data == 'back_to_main')
async def backtomain (call: CallbackQuery):
        await call.message.edit_text(
                f'Привет! Я бонусный помощник PONNY PRINT 🎁\n\n'
                f'У меня можно:\n• Посмотреть свои баллы\n• Получить скидку для друга\n• Узнать условия',
                reply_markup=kb.main
            )

@router.callback_query(F.data == 'conditions')
async def conditions (call: CallbackQuery):
    await call.message.edit_text (
        f'📋 Условия бонусной программы PONNY PRINT\n\n'
        f'🌟 НАЧИСЛЕНИЕ БАЛЛОВ:\n'
        f'+5% баллами от каждого заказа без учета стоимости доставки по России\n'
        f'+500 баллов за каждого приглашенного друга\n'
        f'+1000 баллов потому, у дизайнеров хорошее настроение и вы попали под раздачу 😘\n\n'
        f'🎯 КАК ПОТРАТИТЬ:\n'
        f'• 1 балл = 1 рубль\n• Оплата до 15% суммы заказа\n• Не сгорают 3 месяца\n• Можно потратить на любой заказ\n\n'
        f'⚡️ ОГРАНИЧЕНИЯ:\n'
        f'• Баллы не применяются к актуальным акциям\n'
        f'• Минимальный заказ для оплаты баллами - 3000 руб.\n• Баллы не конвертируются в деньги',
        reply_markup=kb.tomain
    )

@router.callback_query(F.data == 'callmanager')
async def callmanager (call: CallbackQuery):
    await call.message.edit_text(
        f'Вы хотите связаться с живым человеком 😌\n\n'
        f'📞 Часы его работы: Пн-Пт, 10:00-19:00 по МСК\n'
        f'⏱️ Среднее время ответа: 10-20 минут\n\n'
        f'Выберите тему вопроса:\n\n',
        reply_markup=kb.questions
    )

@router.callback_query(F.data == 'pointproblem')
async def pointproble (call: CallbackQuery):
    await call.message.edit_text (
        f'Возможные проблемы:\n\n'
        f'Вы ввели неправильный номер телефона\n'
        f'У вас 0 бонусов на балансе\n\n'
        f'Если проблема не решилась, нажмите кнопку "📋 Другое"', 
        reply_markup=kb.change_number_ask
    )

@router.callback_query ((F.data == 'orederquestion') | (F.data == 'other'))
async def choosemethod (call: CallbackQuery):
    await call.message.edit_text ('Выберите как с вами связаться',
                        reply_markup= kb.choosemethods)

@router.callback_query(F.data == "sendreplytochat")
async def sendtochat (call: CallbackQuery):
    await call.message.edit_text(
        f'Напишите вопрос - @KsanaSafronova',
        reply_markup=kb.simpletomain
    )

@router.callback_query(F.data == "replytobot")
async def otherquestions(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.message.edit_text(
        f'Опишите вашу ситуацию коротко (1-3 предложения) и мы всё проверим\n\n'
        f'✍️ Напишите сообщение ниже:'
    )
    await state.set_state(QuestionState.waiting_for_question)
    await call.answer()

@router.message(QuestionState.waiting_for_question)
async def process_user_question(message: Message, state: FSMContext, bot: Bot):
    user_question = message.text
    
    # Показываем "печатает..."
    await bot.send_chat_action(message.chat.id, "typing")
    await asyncio.sleep(2)
    
    try:
        # 1. Отправляем вопрос в группу
        group_message = await bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=f"🆘 *НОВЫЙ ВОПРОС ОТ ПОЛЬЗОВАТЕЛЯ*\n\n"
                 f"👤 *Пользователь:* {message.from_user.first_name}\n"
                 f"🔹 Username: @{message.from_user.username if message.from_user.username else 'нет'}\n"
                 f"🔹 ID: `{message.from_user.id}`\n\n"
                 f"❓ *Вопрос:*\n{user_question}\n\n"
                 f"👇 *Ответьте на это сообщение, чтобы отправить ответ пользователю*",
            parse_mode="Markdown"
        )
        
        # 2. Сохраняем тикет в БД
        ticket_id = create_support_ticket(
            user_id=message.from_user.id,
            question=user_question,
            group_message_id=group_message.message_id
        )
        
        # 3. Подтверждаем пользователю
        await message.answer(
            f'✅ *Ваш вопрос отправлен в поддержку!*\n\n'
            f'⏰ *Примерное время ответа:* 10-20 минут\n\n'
            f'✍🏻 Мы ответим вам в этом чате\n\n'
            f'Воспользуйтесь моментом ожидания,'
            f' чтобы посмотреть наши любимые работы или вернитесь в главное меню',
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text='🗃️ Посмотреть работы', url='https://pin.it/6gS0am7KS')],
                [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
            ])
        )
        
    except Exception as e:
        logger.error(f"Error sending to group: {e}")
        await message.edit_text(
            '❌ Произошла ошибка при отправке вопроса.\n'
            'Пожалуйста, попробуйте позже или свяжитесь напрямую.',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
            ])
        )
    
    await state.clear()


@router.message(F.chat.id == int(SUPPORT_GROUP_ID), F.reply_to_message)
async def handle_group_reply(message: Message, bot: Bot):
    """Обрабатывает ответы в группе поддержки"""
    
    # Получаем ID оригинального сообщения
    replied_message_id = message.reply_to_message.message_id
    
    # Ищем тикет по ID сообщения в группе
    ticket = get_ticket_by_group_message(replied_message_id)
    
    if ticket and not ticket.is_answered:
        # Отправляем ответ пользователю
        try:
            await bot.send_message(
                chat_id=ticket.user_id,
                text=f"📩 *Ответ от поддержки:*\n\n{message.text}\n\n"
                     f"Благодарим за обращение\n\n"
                     f'Контакты для связи:\n'
                     f"Ponnyprint@mail.ru",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⏪ Назад", callback_data="callmanager")],
                    [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
                ])
            )
            
            # Обновляем тикет
            update_ticket_with_answer(ticket.id, message.text)
            
            # Подтверждаем в группе
            await message.reply(
                f"✅ Ответ отправлен пользователю [ID: {ticket.user_id}]",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🗒️ Закрыть тикет", callback_data=f"close_{ticket.id}")]
                ])
            )
            
        except Exception as e:
            logger.error(f"Error sending reply to user: {e}")
            await message.reply(f"❌ Ошибка отправки: {e}")
    elif ticket and ticket.is_answered:
        await message.reply("⚠️ Этот тикет уже был отвечен ранее.")

@router.callback_query(F.data.startswith("close_"))
async def close_ticket_handler(callback: CallbackQuery):
    """Закрывает тикет"""
    try:
        ticket_id = int(callback.data.split("_")[1])
        
        # Функция для закрытия тикета (добавь в requests.py)
        from requests import close_ticket
        if close_ticket(ticket_id):
            # Редактируем сообщение
            await callback.message.edit_text(
                f"🗒️ Тикет #{ticket_id} закрыт",
                reply_markup=None  # Убираем кнопку
            )
        else:
            await callback.answer("❌ Ошибка закрытия тикета")
            
    except (ValueError, IndexError):
        await callback.answer("❌ Ошибка обработки")
    
    await callback.answer()

@router.message(Command("mytickets"))
async def my_tickets_command(message: Message):
    """Показывает все вопросы пользователя"""
    tickets = get_user_tickets(message.from_user.id)
    
    if not tickets:
        await message.answer("📭 У вас еще не было вопросов к поддержке.")
        return
    
    text = "📋 *Ваши запросы в поддержку:*\n\n"
    
    for ticket in tickets[:5]:  # Последние 5 тикетов
        status = "✅ Отвечено" if ticket.is_answered else "⏳ Ожидает ответа"
        date = ticket.created_at.strftime("%d.%m.%Y %H:%M")
        
        text += f"🆔 *#{ticket.id}* - {date}\n"
        text += f"📝 *Вопрос:* {ticket.user_question[:50]}...\n"
        text += f"📊 *Статус:* {status}\n"
        
        if ticket.answer_text:
            text += f"📩 *Ответ:* {ticket.answer_text[:50]}...\n"
        
        text += "\n" + "-"*30 + "\n\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.callback_query(F.data.startswith("reply_"))
async def quick_reply_callback(callback: CallbackQuery, bot: Bot):
    """Кнопка быстрого ответа в группе"""
    user_id = int(callback.data.split("_")[1])
    
    # Открываем диалог для ответа
    await callback.message.edit_text(
        f"✏️ *Напишите ответ для пользователя {user_id}:*\n\n"
        f"Или ответьте на сообщение выше.",
        parse_mode="Markdown"
    )
    await callback.answer()

@router.message(Command("groupid"))
async def get_group_id(message: Message):
    """Показывает ID чата"""
    await message.answer(f"ID этого чата: `{message.chat.id}`", parse_mode="Markdown")


@router.callback_query(F.data == 'backtopquestions')
async def deleteask (call: CallbackQuery):
    await call.message.edit_text(
        f'Вы хотите связаться с живым человеком 😌\n\n'
        f'📞 Часы его работы: Пн-Пт, 10:00-19:00 по МСК\n'
        f'⏱️ Среднее время ответа: 10-20 минут\n\n'
        f'Выберите тему вопроса:\n\n',
        reply_markup=kb.questions
    )

@router.message(Command('debug'))
async def debug_command(message: Message):
    from requests import get_user_by_tg_id
    user = get_user_by_tg_id(message.from_user.id)
    
    if user:
        debug_info = f"""
        Отладочная информация:
        ID: {user.id}
        TG ID: {user.tg_id}
        Phone: {user.phone}
        Referral Code: {user.referral_code}
        Points Referral: {user.points_referral}
        Points Manual: {user.points_manual}
        Invited By: {user.invited_by}
        Total Points: {user.get_total_points}
        """
        await message.answer(debug_info)
    else:
        await message.answer("Пользователь не найден в базе данных.")

'''
@router.message(Command("delete_no_phone"))
async def delete_no_phone_command(message: Message):
    """Удалить пользователей без телефона: /delete_no_phone"""
    if not is_admin(message.from_user.id):
        return
    
    count, msg = delete_users_without_phone()
    await message.answer(msg)'''