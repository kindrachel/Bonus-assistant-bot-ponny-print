from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    ReplyKeyboardRemove
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

main = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='👤 Мои баллы', callback_data='mypoints')],
    [InlineKeyboardButton(text='🎁 Скидка для друга', callback_data='referral')],
    [InlineKeyboardButton(text='❓ Условия программы', callback_data='conditions')],
    [InlineKeyboardButton(text='🗃️ Посмотреть работы', url='https://pin.it/6gS0am7KS')],
    [InlineKeyboardButton(text='💬 Связаться с человеком', callback_data='callmanager')]
])


# Клавиатура для запроса номера телефона
phone_request = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📱 Отправить номер телефона", request_contact=True)],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# Альтернативная клавиатура для ручного ввода телефона
phone_alt = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📞 Ввести телефон вручную",callback_data='enter_manual')],
        [InlineKeyboardButton(text='📱 Поделиться контактом', callback_data='share_contact')],
    ],
    resize_keyboard=True
)

phone_menu = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏪ Назад", callback_data="back_to_phone_menu")]
            ])
    
no_points_keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Ввести другой номер", callback_data="change_phone")],
                [InlineKeyboardButton(text="💬 Связаться с человеком", callback_data="callmanager")],
                [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
            ])

tomain = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")],
                [InlineKeyboardButton(text='💬 Связаться с человеком', callback_data='callmanager')]
        ])

simpletomain = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")],
        ])

cancelchange = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏪ Отмена", callback_data="mypoints")]
        ])



questions = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='💳Проблема с баллами', callback_data='pointproblem')],
    [InlineKeyboardButton(text='🖨️ Оформить заказ или вопрос по заказу', callback_data='orederquestion')],
    [InlineKeyboardButton(text='📋 Другое', callback_data='other')],
    [InlineKeyboardButton(text="🏠 На главную", callback_data="back_to_main")]
])

questionsmenu = InlineKeyboardMarkup(inline_keyboard= [
    [InlineKeyboardButton(text='📋 Другое', callback_data='other')],
    [InlineKeyboardButton(text='⏪ Назад', callback_data='backtopquestions')]
])

change_number_ask = InlineKeyboardMarkup(inline_keyboard= [
    [InlineKeyboardButton(text="🔄 Ввести другой номер", callback_data="change_phone_for_ask")],
    [InlineKeyboardButton(text='📋 Другое', callback_data='other')],
    [InlineKeyboardButton(text='⏪ Назад', callback_data='backtopquestions')]
])

cancelchangeforask = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏪ Отмена", callback_data="pointproblem")]
        ])

choosemethods = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="✏️ Получить ответ здесь", callback_data='replytobot')],
    [InlineKeyboardButton(text="✒️ Получить ответ в личные сообщения", callback_data='sendreplytochat')]
])









# Основное меню админ-панели
admin_main = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
    [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_users")],
    [InlineKeyboardButton(text="👤 Добавить пользователя", callback_data="admin_add_user")],
    [InlineKeyboardButton(text="💰 Управление баллами", callback_data="admin_points")],
    [InlineKeyboardButton(text="🧹 Очистка базы", callback_data="admin_cleanup")],
    [InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_settings")],
    [InlineKeyboardButton(text="🚪 Выйти из админки", callback_data="admin_exit")]
])

# Меню управления пользователями
admin_users_menu = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔍 Поиск пользователя", callback_data="admin_search")],
    [InlineKeyboardButton(text="📋 Список пользователей", callback_data="admin_users_list")],
    [InlineKeyboardButton(text="👤 Информация о себе", callback_data="admin_my_info")],
    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
])

# Меню управления баллами
admin_points_menu = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="➕ Добавить баллы", callback_data="admin_add_points")],
    [InlineKeyboardButton(text="➖ Убрать баллы", callback_data="admin_remove_points")],
    [InlineKeyboardButton(text="✏️ Установить баллы", callback_data="admin_set_points")],
    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
])

# Меню очистки базы
admin_cleanup_menu = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🧹 Очистить пустых", callback_data="admin_clean_empty")],
    [InlineKeyboardButton(text="🔄 Удалить дубликаты", callback_data="admin_clean_duplicates")],
    [InlineKeyboardButton(text="📊 Статистика БД", callback_data="admin_db_stats")],
    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
])

# Меню настроек
admin_settings_menu = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="👑 Добавить админа", callback_data="admin_add_admin")],
    [InlineKeyboardButton(text="🔧 Настройки бота", callback_data="admin_bot_settings")],
    [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
])