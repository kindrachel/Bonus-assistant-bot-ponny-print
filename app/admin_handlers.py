import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from requests import ( get_statistics, get_all_users, search_users_by_phone, search_users_by_name, get_user_by_id, 
                      update_user_points, delete_empty_users, clean_duplicate_phones, add_user_with_details,
                      quick_add_user,get_points_history
)
from models import Session, User, PointsHistory
from config import ADMIN_IDS
import app.keyboards as kb

logger = logging.getLogger(__name__)
router = Router()

class AdminState(StatesGroup):

    waiting_for_phone_search = State()
    waiting_for_name_search = State()
    waiting_for_user_id = State()
    waiting_for_points = State()
    waiting_for_points_type = State()
    waiting_for_admin_id = State()
    waiting_for_phone_search = State()
    waiting_for_name_search = State()
    waiting_for_user_id = State()
    waiting_for_points = State()
    waiting_for_points_type = State()
    waiting_for_admin_id = State()
    waiting_for_add_user_phone = State()
    waiting_for_add_user_points = State()
    waiting_for_add_user_name = State()

# Проверка на админа
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ========== КОМАНДЫ ==========

@router.message(Command("admin"))
async def admin_command(message: Message):
    """Команда /admin - вход в админ-панель"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещен")
        return
    
    await message.answer(
        "👨‍💼 *Админ-панель*\n\n"
        "Выберите раздел:",
        parse_mode="Markdown",
        reply_markup=kb.admin_main
    )

@router.message(Command("ap"))
async def admin_shortcut(message: Message):
    """Короткая команда /ap для админ-панели"""
    await admin_command(message)

# ========== ГЛАВНОЕ МЕНЮ ==========

@router.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: CallbackQuery):
    """📊 Статистика"""
    if not is_admin(callback.from_user.id):
        return
    
    stats = get_statistics()
    
    text = (
        "📊 *Статистика бота:*\n\n"
        f"👥 Всего пользователей: *{stats['total_users']}*\n"
        f"📱 С привязанным телефоном: *{stats['users_with_phone']}*\n"
        f"⭐ Всего баллов в системе: *{stats['total_points']}*\n"
        f"📈 Процент с телефоном: *{round(stats['users_with_phone'] / stats['total_users'] * 100 if stats['total_users'] > 0 else 0)}%*"
    )
    
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back_main")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_users")
async def admin_users_handler(callback: CallbackQuery):
    """👥 Управление пользователями"""
    await callback.message.edit_text(
        "👥 *Управление пользователями*\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=kb.admin_users_menu
    )
    await callback.answer()

@router.callback_query(F.data == "admin_points")
async def admin_points_handler(callback: CallbackQuery):
    """💰 Управление баллами"""
    await callback.message.edit_text(
        "💰 *Управление баллами*\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=kb.admin_points_menu
    )
    await callback.answer()

@router.callback_query(F.data == "admin_cleanup")
async def admin_cleanup_handler(callback: CallbackQuery):
    """🧹 Очистка базы"""
    await callback.message.edit_text(
        "🧹 *Очистка базы данных*\n\n"
        "Выберите тип очистки:",
        parse_mode="Markdown",
        reply_markup=kb.admin_cleanup_menu
    )
    await callback.answer()

@router.callback_query(F.data == "admin_settings")
async def admin_settings_handler(callback: CallbackQuery):
    """⚙️ Настройки"""
    await callback.message.edit_text(
        "⚙️ *Настройки админ-панели*\n\n"
        "Выберите действие:",
        parse_mode="Markdown",
        reply_markup=kb.admin_settings_menu
    )
    await callback.answer()

@router.callback_query(F.data == "admin_exit")
async def admin_exit_handler(callback: CallbackQuery):
    """🚪 Выйти из админки"""
    await callback.message.edit_text(
        "✅ Вы вышли из админ-панели",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
        ])
    )
    await callback.answer()

# ========== УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ==========

@router.callback_query(F.data == "admin_users_list")
async def admin_users_list_handler(callback: CallbackQuery):
    """📋 Список пользователей"""
    users = get_all_users(limit=20)
    
    if not users:
        await callback.message.edit_text("📭 Нет пользователей")
        return
    
    text = "👥 *Последние 20 пользователей:*\n\n"
    for user in users:
        phone_display = user.phone if user.phone else "📵 нет телефона"
        name_display = f"{user.first_name or ''} {user.last_name or ''}".strip()
        if not name_display:
            name_display = "Без имени"
        
        text += (
            f"🆔 *{user.id}* - {name_display}\n"
            f"📱 {phone_display}\n"
            f"⭐ {user.get_total_points()} баллов\n"
            f"🎫 {user.referral_code}\n"
            f"---\n"
        )
    
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="admin_back_main")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_search")
async def admin_search_handler(callback: CallbackQuery, state: FSMContext):
    """🔍 Поиск пользователя"""
    await callback.message.edit_text(
        "🔍 *Поиск пользователя*\n\n"
        "Введите номер телефона или имя:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users")]
        ])
    )
    await state.set_state(AdminState.waiting_for_phone_search)
    await callback.answer()

@router.message(AdminState.waiting_for_phone_search)
async def process_admin_search(message: Message, state: FSMContext):
    """Обработка поиска пользователя"""
    search_text = message.text.strip()
    
    # Ищем по телефону
    users_by_phone = search_users_by_phone(search_text)
    # Ищем по имени
    users_by_name = search_users_by_name(search_text)
    
    # Объединяем результаты
    all_users = list(set(users_by_phone + users_by_name))
    
    if not all_users:
        await message.answer(
            f"❌ Не найдено пользователей по запросу: '{search_text}'",
            reply_markup=kb.admin_users_menu
        )
        await state.clear()
        return
    
    text = f"🔍 *Найдено пользователей: {len(all_users)}*\n\n"
    
    for i, user in enumerate(all_users[:10], 1):
        name_display = f"{user.first_name or ''} {user.last_name or ''}".strip()
        if not name_display:
            name_display = "Без имени"
        
        text += (
            f"{i}. 🆔 *{user.id}* - {name_display}\n"
            f"   📱 {user.phone or 'нет телефона'}\n"
            f"   ⭐ {user.get_total_points()} баллов\n"
        )
        
        # Кнопки для каждого пользователя
        if i == 1:  # Для первого пользователя показываем кнопки
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="💰 Добавить баллы", 
                                       callback_data=f"admin_add_to_{user.id}"),
                    InlineKeyboardButton(text="👁️ Подробнее", 
                                       callback_data=f"admin_view_{user.id}")
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users")]
            ])
    
    if len(all_users) > 10:
        text += f"\n... и еще {len(all_users) - 10} пользователей"
    
    await message.answer(
        text,
        parse_mode="Markdown",
        reply_markup=keyboard if 'keyboard' in locals() else kb.admin_users_menu
    )
    await state.clear()

@router.callback_query(F.data.startswith("admin_view_"))
async def admin_view_user_handler(callback: CallbackQuery):
    """👁️ Подробная информация о пользователе"""
    user_id = int(callback.data.split("_")[2])
    user = get_user_by_id(user_id)
    
    if not user:
        await callback.answer("❌ Пользователь не найден")
        return
    
    text = (
        f"👤 *Подробная информация*\n\n"
        f"🆔 ID в базе: {user.id}\n"
        f"🆔 Telegram ID: {user.tg_id or 'не привязан'}\n"
        f"📱 Телефон: {user.phone or 'не указан'}\n"
        f"👤 Имя: {user.first_name or 'не указано'}\n"
        f"👤 Фамилия: {user.last_name or 'не указана'}\n"
        f"⭐ Всего баллов: {user.get_total_points()}\n"
        f"  • Ручные: {user.points_manual}\n"
        f"  • Реферальные: {user.points_referral}\n"
        f"🎫 Реферальный код: {user.referral_code}\n"
        f"👥 Приглашен: {user.invited_by or 'нет'}\n"
    )
    
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💰 Добавить баллы", 
                                   callback_data=f"admin_add_to_{user.id}"),
                InlineKeyboardButton(text="🗑️ Удалить", 
                                   callback_data=f"admin_delete_{user.id}")
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_users")]
        ])
    )
    await callback.answer()

# ========== УПРАВЛЕНИЕ БАЛЛАМИ ==========

@router.callback_query(F.data.startswith("admin_add_to_"))
async def admin_add_points_handler(callback: CallbackQuery, state: FSMContext):
    """💰 Добавить баллы конкретному пользователю"""
    user_id = int(callback.data.split("_")[3])
    
    await state.update_data(user_id=user_id)
    
    await callback.message.edit_text(
        "💰 *Добавление баллов*\n\n"
        "Введите количество баллов для добавления:",
        parse_mode="Markdown"
    )
    
    await state.set_state(AdminState.waiting_for_points)
    await callback.answer()

@router.message(AdminState.waiting_for_points)
async def process_add_points(message: Message, state: FSMContext):
    """Обработка добавления баллов"""
    try:
        points = int(message.text.strip())
        data = await state.get_data()
        user_id = data.get('user_id')
        
        if points <= 0:
            await message.answer("❌ Количество баллов должно быть положительным")
            await state.clear()
            return
        
        user = get_user_by_id(user_id)
        if not user:
            await message.answer("❌ Пользователь не найден")
            await state.clear()
            return
        
        # Добавляем баллы
        if update_user_points(user_id, 'add_manual', points):
            await message.answer(
                f"✅ Успешно!\n\n"
                f"👤 Пользователь: {user.first_name or 'ID:' + str(user.id)}\n"
                f"➕ Добавлено: {points} баллов\n"
                f"📊 Теперь всего: {user.get_total_points()} баллов",
                reply_markup=kb.admin_main
            )
        else:
            await message.answer("❌ Ошибка при добавлении баллов")
        
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите число (количество баллов)")
        await state.clear()

# ========== ОЧИСТКА БАЗЫ ==========

@router.callback_query(F.data == "admin_clean_empty")
async def admin_clean_empty_handler(callback: CallbackQuery):
    """🧹 Очистить пустых пользователей"""
    count, msg = delete_empty_users()
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_cleanup")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_clean_duplicates")
async def admin_clean_duplicates_handler(callback: CallbackQuery):
    """🔄 Удалить дубликаты телефонов"""
    count, msg = clean_duplicate_phones()
    
    await callback.message.edit_text(
        msg,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_cleanup")]
        ])
    )
    await callback.answer()

@router.callback_query(F.data == "admin_db_stats")
async def admin_db_stats_handler(callback: CallbackQuery):
    """📊 Статистика БД"""
    session = Session()
    try:
        from sqlalchemy import func
        
        total_users = session.query(func.count(User.id)).scalar()
        users_with_phone = session.query(func.count(User.id)).filter(User.phone.isnot(None)).scalar()
        users_with_tg = session.query(func.count(User.id)).filter(User.tg_id.isnot(None)).scalar()
        empty_users = session.query(func.count(User.id)).filter(
            User.phone.is_(None),
            User.tg_id.is_(None)
        ).scalar()
        
        stats = (
            f"📊 *Статистика базы данных:*\n\n"
            f"👥 Всего записей: {total_users}\n"
            f"📱 С телефоном: {users_with_phone}\n"
            f"🤖 С TG ID: {users_with_tg}\n"
            f"🚫 Пустых записей: {empty_users}\n\n"
            f"Для очистки используйте соответствующие кнопки."
        )
        
        await callback.message.edit_text(
            stats,
            parse_mode="Markdown",
            reply_markup=kb.admin_cleanup_menu
        )
        
    finally:
        session.close()
    await callback.answer()

# ========== НАВИГАЦИЯ ==========

@router.callback_query(F.data == "admin_back_main")
async def admin_back_main_handler(callback: CallbackQuery, state: FSMContext):
    """🔙 Назад в главное меню админки"""
    await state.clear()
    await callback.message.edit_text(
        "👨‍💼 *Админ-панель*\n\n"
        "Выберите раздел:",
        parse_mode="Markdown",
        reply_markup=kb.admin_main
    )
    await callback.answer()

@router.callback_query(F.data == "admin_back")
async def admin_back_handler(callback: CallbackQuery):
    """🔙 Общая кнопка назад"""
    await callback.message.edit_text(
        "👨‍💼 *Админ-панель*\n\n"
        "Выберите раздел:",
        parse_mode="Markdown",
        reply_markup=kb.admin_main
    )
    await callback.answer()

# ========== БЫСТРЫЕ КОМАНДЫ ==========

@router.message(Command("users"))
async def quick_users_command(message: Message):
    """Быстрый список пользователей: /users"""
    if not is_admin(message.from_user.id):
        return
    
    users = get_all_users(limit=10)
    
    text = "👥 *Последние 10 пользователей:*\n\n"
    for user in users:
        text += f"{user.id}. {user.phone or 'нет телефона'} - {user.get_total_points()} баллов\n"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("addpoints"))
async def quick_add_points_command(message: Message):
    """Быстрое добавление баллов: /addpoints телефон баллы"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        _, phone, points = message.text.split()
        points = int(points)
        
        session = Session()
        try:
            user = session.query(User).filter(User.phone == phone).first()
            if user:
                user.points_manual += points
                session.commit()
                await message.answer(
                    f"✅ Добавлено {points} баллов пользователю {phone}\n"
                    f"📊 Теперь у него: {user.get_total_points()} баллов"
                )
            else:
                await message.answer(f"❌ Пользователь с телефоном {phone} не найден")
        finally:
            session.close()
            
    except ValueError:
        await message.answer("❌ Формат: /addpoints телефон баллы")

@router.callback_query(F.data == "admin_add_user_full")
async def admin_add_user_full_handler(callback: CallbackQuery, state: FSMContext):
    """Полный режим добавления пользователя"""
    await callback.message.edit_text(
        "👤 *Полное добавление пользователя*\n\n"
        "Шаг 1/3: Введите номер телефона:",
        parse_mode="Markdown"
    )
    await state.set_state(AdminState.waiting_for_add_user_phone)
    await callback.answer()

@router.message(AdminState.waiting_for_add_user_phone)
async def process_add_user_phone_full(message: Message, state: FSMContext):
    phone = message.text.strip()
    await state.update_data(phone=phone)
    
    await message.answer(
        f"📱 Телефон: {phone}\n\n"
        f"Шаг 2/3: Введите имя пользователя (или пропустите):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏩ Пропустить", callback_data="skip_name")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
        ])
    )
    await state.set_state(AdminState.waiting_for_add_user_name)

@router.callback_query(F.data == "skip_name", AdminState.waiting_for_add_user_name)
async def skip_name_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data.get('phone')
    
    await callback.message.edit_text(
        f"📱 Телефон: {phone}\n"
        f"👤 Имя: не указано\n\n"
        f"Шаг 3/3: Введите количество баллов:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏩ 0 баллов", callback_data="zero_points")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
        ])
    )
    await state.set_state(AdminState.waiting_for_add_user_points)
    await callback.answer()

@router.message(AdminState.waiting_for_add_user_name)
async def process_add_user_name(message: Message, state: FSMContext):
    name = message.text.strip()
    await state.update_data(first_name=name)
    
    data = await state.get_data()
    phone = data.get('phone')
    
    await message.answer(
        f"📱 Телефон: {phone}\n"
        f"👤 Имя: {name}\n\n"
        f"Шаг 3/3: Введите количество баллов:"
    )
    await state.set_state(AdminState.waiting_for_add_user_points)

@router.message(AdminState.waiting_for_add_user_points)
async def process_add_user_points_full(message: Message, state: FSMContext):
    try:
        points = int(message.text.strip())
        data = await state.get_data()
        phone = data.get('phone')
        first_name = data.get('first_name')
        
        # Добавляем пользователя
        success, result_msg, user_data = add_user_with_details(
            phone=phone,
            points=points,
            first_name=first_name
        )
        
        if success:
            if user_data.get('is_new'):
                response = (
                    f"✅ *Новый пользователь создан!*\n\n"
                    f"📱 Телефон: {phone}\n"
                    f"👤 Имя: {first_name or 'не указано'}\n"
                    f"🆔 ID в базе: {user_data['id']}\n"
                    f"⭐ Баллы: {user_data['points']}\n"
                    f"🎫 Реф. код: {user_data.get('referral_code', 'сгенерирован')}"
                )
            else:
                response = (
                    f"✅ *Пользователь обновлен!*\n\n"
                    f"📱 Телефон: {phone}\n"
                    f"⭐ Теперь баллов: {user_data['points']}"
                )
        else:
            response = result_msg
        
        await message.answer(response, parse_mode="Markdown")
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите число (количество баллов)")

# Команда для добавления пользователя
@router.message(Command("adduser"))
async def add_user_command(message: Message, state: FSMContext):
    """Добавить пользователя: /adduser телефон баллы (опционально)"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        parts = message.text.split()
        
        if len(parts) == 2:
            # Только телефон
            phone = parts[1]
            result = quick_add_user(phone, 0)
            await message.answer(result)
            
        elif len(parts) == 3:
            # Телефон + баллы
            phone = parts[1]
            points = int(parts[2])
            result = quick_add_user(phone, points)
            await message.answer(result)
            
        else:
            # Интерактивный режим
            await message.answer(
                "👤 *Добавление пользователя*\n\n"
                "Введите номер телефона:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
                ])
            )
            await state.set_state(AdminState.waiting_for_add_user_phone)
            
    except ValueError:
        await message.answer("❌ Формат: /adduser телефон [баллы]")

# Обработчик ввода телефона
@router.message(AdminState.waiting_for_add_user_phone)
async def process_add_user_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    
    # Сохраняем телефон
    await state.update_data(phone=phone)
    
    await message.answer(
        f"📱 Телефон: {phone}\n\n"
        f"Введите количество баллов (или 0):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏩ Пропустить (0 баллов)", callback_data="skip_points")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
        ])
    )
    
    await state.set_state(AdminState.waiting_for_add_user_points)

# Обработчик пропуска баллов
@router.callback_query(F.data == "skip_points", AdminState.waiting_for_add_user_points)
async def skip_points_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data.get('phone')
    
    # Добавляем пользователя без баллов
    result = quick_add_user(phone, 0)
    
    await callback.message.edit_text(result)
    await state.clear()
    await callback.answer()

# Обработчик ввода баллов
@router.message(AdminState.waiting_for_add_user_points)
async def process_add_user_points(message: Message, state: FSMContext):
    try:
        points = int(message.text.strip())
        data = await state.get_data()
        phone = data.get('phone')
        
        # Добавляем пользователя
        result = quick_add_user(phone, points)
        
        await message.answer(result)
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Введите число (количество баллов)")

# Кнопка в админ-панели для добавления пользователя
@router.callback_query(F.data == "admin_add_user")
async def admin_add_user_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "👤 *Добавление пользователя*\n\n"
        "Введите номер телефона:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back_main")]
        ])
    )
    await state.set_state(AdminState.waiting_for_add_user_phone)
    await callback.answer()

@router.message(Command("history"))
async def points_history_command(message: Message):
    """Показать историю начисления баллов: /history"""
    from requests import get_points_history, get_user_by_tg_id
    
    user = get_user_by_tg_id(message.from_user.id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return
    
    history = get_points_history(user.id, limit=10)
    
    if not history:
        await message.answer("📭 У вас еще нет истории начисления баллов")
        return
    
    text = "📊 *История начисления баллов:*\n\n"
    
    for record in history:
        date = record.created_at.strftime("%d.%m.%Y %H:%M")
        type_emoji = {
            'manual': '🖊️',
            'referral': '👥',
            'welcome': '🎁',
            'admin': '👑'
        }.get(record.points_type, '💰')
        
        text += f"{type_emoji} *{date}*\n"
        text += f"   {record.points_amount} баллов"
        if record.description:
            text += f" - {record.description}"
        text += "\n\n"
    
    # Добавляем итоги
    from requests import get_user_points_summary
    summary = get_user_points_summary(user.id)
    
    if summary:
        text += f"💰 *Итого:* {summary['total_points']} баллов\n"
        text += f"   • Ручные: {summary['manual_points']}\n"
        text += f"   • Реферальные: {summary['referral_points']}\n"
        
        if summary['last_manual_update']:
            last_update = summary['last_manual_update'].strftime("%d.%m.%Y")
            text += f"\n📅 Последнее обновление: {last_update}"
    
    await message.answer(text, parse_mode="Markdown")

@router.message(Command("userhistory"))
async def user_history_command(message: Message):
    """История баллов пользователя: /userhistory телефон"""
    if not is_admin(message.from_user.id):
        return
    
    try:
        phone = message.text.split()[1]
        
        session = Session()
        try:
            user = session.query(User).filter(User.phone == phone).first()
            if not user:
                await message.answer("❌ Пользователь не найден")
                return
            
            history = session.query(PointsHistory)\
                .filter(PointsHistory.user_id == user.id)\
                .order_by(PointsHistory.created_at.desc())\
                .limit(20)\
                .all()
            
            if not history:
                await message.answer(f"📭 У пользователя {phone} нет истории баллов")
                return
            
            text = f"📊 *История баллов пользователя {phone}:*\n\n"
            
            total_added = 0
            for record in history:
                date = record.created_at.strftime("%d.%m.%Y")
                total_added += record.points_amount
                
                text += f"• {date}: {record.points_amount} баллов"
                if record.description:
                    text += f" ({record.description})"
                text += "\n"
            
            text += f"\n💰 Всего начислено: {total_added} баллов"
            text += f"\n📊 Текущий баланс: {user.get_total_points()} баллов"
            
            await message.answer(text, parse_mode="Markdown")
            
        finally:
            session.close()
            
    except IndexError:
        await message.answer("❌ Формат: /userhistory телефон")