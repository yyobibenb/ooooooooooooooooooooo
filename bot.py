import asyncio
import os
import logging
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, LinkPreviewOptions, InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from aiogram.enums import ParseMode
from database import (init_db, add_user, get_all_users, save_broadcast, log_click, get_stats,
                      update_button_content, get_button_content, get_all_keyboard_buttons,
                      add_keyboard_button, delete_keyboard_button, rename_keyboard_button,
                      generate_short_id, get_button_by_short_id, move_button_up, move_button_down)

# Load chat continuation texts
CHATS_CONTINUATION_FILE = "chats_continuation.json"
CHATS_CONTINUATION = {}


def load_chats_continuation():
    global CHATS_CONTINUATION
    try:
        if os.path.exists(CHATS_CONTINUATION_FILE):
            with open(CHATS_CONTINUATION_FILE, 'r', encoding='utf-8') as f:
                CHATS_CONTINUATION = json.load(f)
    except Exception as e:
        logger.error(f"Error loading chats continuation: {e}")
        CHATS_CONTINUATION = {}


def save_chats_continuation():
    try:
        with open(CHATS_CONTINUATION_FILE, 'w', encoding='utf-8') as f:
            json.dump(CHATS_CONTINUATION, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving chats continuation: {e}")


import logging
import sys

# Настройка логирования для вывода в консоль (stdout)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

ADMIN_ID = int(os.environ.get("ADMIN_ID", "5855297931"))
# BOT_TOKEN should be set via environment variable for security
BOT_TOKEN = "8575852674:AAEcaG0l7cQ3JHSrs1MaBkA_wQPQYshpSs0"
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

# Helper function to send messages without link preview
async def send_message(message_obj, text, **kwargs):
    """Отправка сообщения без предпросмотра ссылок"""
    if 'link_preview_options' not in kwargs:
        kwargs['link_preview_options'] = LinkPreviewOptions(is_disabled=True)
    return await message_obj.answer(text, **kwargs)

def make_callback_data(button_id: str) -> str:
    """
    Создает callback_data для inline кнопки используя короткий ID
    Telegram ограничивает callback_data до 64 байт
    """
    short_id = generate_short_id(button_id)
    return f"dyn:{short_id}"

def group_buttons_by_row(buttons, buttons_data=None, default_per_row=1):
    """
    Группирует кнопки по рядам с учётом индивидуальной ширины каждой кнопки.

    buttons_data: список словарей с информацией о кнопках (включая row_width)
    row_width: сколько таких кнопок помещается в ряд (1=на весь ряд, 2=половина, 3=треть, 4=четверть)
    """
    if not buttons:
        return []

    grouped = []
    current_row = []
    current_row_capacity = 0  # Сколько уже занято в текущем ряду (в единицах 1/4)

    for i, btn in enumerate(buttons):
        # Определяем ширину этой кнопки
        row_width = default_per_row
        if buttons_data and i < len(buttons_data):
            row_width = buttons_data[i].get('row_width', default_per_row)

        # Конвертируем row_width в единицы занимаемого места (в четвертях ряда)
        # row_width=1 означает кнопка на весь ряд (4/4), 2 = половина (2/4), 3 = треть (≈1.33/4), 4 = четверть (1/4)
        if row_width is None or row_width == 0:
            # По умолчанию 2 кнопки в ряду
            btn_size = 2
        elif row_width == 1:
            btn_size = 4  # На весь ряд
        elif row_width == 2:
            btn_size = 2  # Половина ряда
        elif row_width == 3:
            btn_size = 1.33  # Треть ряда (примерно)
        elif row_width == 4:
            btn_size = 1  # Четверть ряда
        else:
            btn_size = 4 / row_width  # Общая формула

        # Если кнопка не помещается в текущий ряд, начинаем новый
        if current_row and (current_row_capacity + btn_size > 4.1):  # 4.1 для допуска погрешности
            grouped.append(current_row)
            current_row = []
            current_row_capacity = 0

        current_row.append(btn)
        current_row_capacity += btn_size

        # Если ряд заполнен или это кнопка на весь ряд, закрываем ряд
        if current_row_capacity >= 3.9 or row_width == 1:  # 3.9 для допуска погрешности
            grouped.append(current_row)
            current_row = []
            current_row_capacity = 0

    # Добавляем остаток
    if current_row:
        grouped.append(current_row)

    return grouped

def create_page_navigation_buttons(button_id, current_page, total_pages):
    """
    Создаёт кнопки навигации для многостраничного контента
    """
    buttons = []
    short_id = generate_short_id(button_id)

    # Кнопка "Назад" если не первая страница
    if current_page > 0:
        buttons.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"page:{short_id}:{current_page - 1}"
            )
        )

    # Индикатор страницы
    buttons.append(
        InlineKeyboardButton(
            text=f"📄 {current_page + 1}/{total_pages}",
            callback_data=f"page_info:{short_id}:{current_page}"
        )
    )

    # Кнопка "Вперёд" если не последняя страница
    if current_page < total_pages - 1:
        buttons.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"page:{short_id}:{current_page + 1}"
            )
        )

    return buttons

class AdminMenuStates(StatesGroup):
    main = State()
    managing_menu = State()
    adding_button_label = State()
    adding_button_content = State()
    adding_button_photo = State()
    adding_inline_button_text = State()
    adding_inline_button_url = State()
    confirming_button = State()
    creating_nested = State() # For deep nesting

class BroadcastStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_photo = State()
    waiting_for_buttons_menu = State()
    waiting_for_button_name = State()
    waiting_for_button_url = State()
    confirm_broadcast = State()

class ButtonEditStates(StatesGroup):
    selecting_button = State()
    waiting_for_content = State()
    waiting_for_photo = State()
    waiting_for_inline_buttons = State()

# Новая система редактирования контента
class ContentEditorStates(StatesGroup):
    selecting_menu = State()  # Выбор меню для редактирования
    editing_text = State()     # Редактирование текста
    editing_inline_buttons = State()  # Редактирование инлайн-кнопок
    adding_inline_button = State()    # Добавление новой инлайн-кнопки
    waiting_button_text = State()     # Ожидание текста кнопки
    waiting_button_url = State()      # Ожидание URL кнопки
    waiting_submenu_content = State() # Ожидание текста для нового подменю
    waiting_button_width = State()    # Ожидание выбора ширины кнопки
    managing_inline_buttons = State()  # Управление инлайн-кнопками (удаление, редактирование)
    editing_inline_button_name = State()  # Редактирование названия инлайн-кнопки
    editing_keyboard_button_name = State()  # Редактирование названия кнопки клавиатуры
    setting_buttons_layout = State()  # Настройка расположения инлайн-кнопок
    managing_pages = State()           # Управление страницами (список страниц)
    editing_page = State()             # Редактирование текста страницы
    adding_page = State()              # Добавление новой страницы

class ChatsContinuationStates(StatesGroup):
    selecting_chat_section = State()
    managing_lines = State()
    editing_line = State()


# Fixed menu structure
# ============================================================================
# MENU_STRUCTURE - МИГРИРОВАНО В БАЗУ ДАННЫХ
# ============================================================================
# 
# ⚠️ ВАЖНО: После запуска migrate_menu_to_db.py все данные меню находятся в БД!
#
# Этот словарь оставлен пустым, так как:
# ✅ Все кнопки клавиатуры загружаются из таблицы keyboard_buttons
# ✅ Весь текстовый контент загружается из таблицы button_content
# ✅ Все инлайн-кнопки хранятся в JSON в таблице button_content
# ✅ Вся иерархия меню сохранена через parent_id
#
# Старая структура сохранена в файле MENU_STRUCTURE_BACKUP.py для справки.
#
# Для управления меню используйте:
# - Админ-панель → Управление кнопками клавиатуры
# - Админ-панель → Редактор контента
#
# ============================================================================

MENU_STRUCTURE = {}
# Если нужно вернуть старую структуру:
# 1. Откройте файл MENU_STRUCTURE_BACKUP.py
# 2. Скопируйте содержимое сюда
# 3. Перезапустите бота


def get_dynamic_keyboard(user_id=None):
    """
    DEPRECATED: Используйте get_dynamic_keyboard_async() вместо этой функции.
    После миграции в БД эта функция не используется, так как не может загружать данные из БД синхронно.
    """
    keyboard = []

    # После миграции кнопки берутся только из БД через async функцию
    # Эта синхронная функция оставлена для совместимости, но не должна использоваться

    if ADMIN_ID and user_id == ADMIN_ID:
        keyboard.append([KeyboardButton(text="🔐 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

async def get_dynamic_keyboard_async(user_id=None):
    """Получает клавиатуру только из БД (после миграции файл не используется)"""
    keyboard = []
    row = []

    # Берём все кнопки только из БД
    dynamic_btns = await get_all_keyboard_buttons()
    for btn in dynamic_btns:
        lbl = btn['label']
        # Пропускаем призраков
        if lbl.lower().strip() in ["удалить lambi", "удалить ламби", "📝 редактировать чаты"]:
            continue
        row.append(KeyboardButton(text=lbl))
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    if ADMIN_ID and user_id == ADMIN_ID:
        keyboard.append([KeyboardButton(text="🔐 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def get_submenu_keyboard(menu_key, parent_sub_key=None):
    menu = MENU_STRUCTURE.get(menu_key)
    if not menu or 'submenu' not in menu:
        return None

    # If parent_sub_key is specified, get the nested submenu
    if parent_sub_key:
        sub_menu = menu['submenu'].get(parent_sub_key)
        if not sub_menu or 'submenu' not in sub_menu:
            return None
        submenu_dict = sub_menu['submenu']
    else:
        submenu_dict = menu['submenu']

    keyboard = []
    row = []
    for sub_key, sub_menu in submenu_dict.items():
        row.append(KeyboardButton(text=sub_menu['label']))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def get_nav_keyboard_inline(menu_key, sub_key, page_index):
    """For multi-page sections like terminology"""
    # Support for deep search if menu_key is empty (used for deep inline pages)
    menu = None
    if menu_key and menu_key in MENU_STRUCTURE:
        menu = MENU_STRUCTURE[menu_key]
        if sub_key and 'submenu' in menu and sub_key in menu['submenu']:
            menu = menu['submenu'][sub_key]
    else:
        # Deep search for sub_key
        for m_key, m_data in MENU_STRUCTURE.items():
            if m_key == sub_key:
                menu = m_data
                break
            if 'submenu' in m_data:
                if sub_key in m_data['submenu']:
                    menu = m_data['submenu'][sub_key]
                    break
                for s_key, s_data in m_data['submenu'].items():
                    if 'submenu' in s_data and sub_key in s_data['submenu']:
                        menu = s_data['submenu'][sub_key]
                        break
            if menu: break

    if not menu:
        return None

    keyboard = []
    buttons = []

    if 'pages' in menu:
        total_pages = len(menu['pages'])
    else:
        return None

    if page_index > 0:
        buttons.append(
            InlineKeyboardButton(
                text="◀️",
                callback_data=f"page:{menu_key}:{sub_key}:{page_index-1}"))

    buttons.append(
        InlineKeyboardButton(text=f"{page_index+1}/{total_pages}",
                             callback_data="noop"))

    if page_index < total_pages - 1:
        buttons.append(
            InlineKeyboardButton(
                text="▶️",
                callback_data=f"page:{menu_key}:{sub_key}:{page_index+1}"))

    keyboard.append(buttons)

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def edit_message_safe(query: types.CallbackQuery,
                            text: str,
                            reply_markup,
                            parse_mode=ParseMode.HTML,
                            link_preview_disabled=True):
    """Safely edit message in both regular and inline modes"""
    if query.message:
        # Regular message edit
        await query.message.edit_text(text,
                                      reply_markup=reply_markup,
                                      parse_mode=parse_mode,
                                      link_preview_options=LinkPreviewOptions(
                                          is_disabled=link_preview_disabled))
    elif query.inline_message_id:
        # Inline message edit
        await bot.edit_message_text(inline_message_id=query.inline_message_id,
                                    text=text,
                                    reply_markup=reply_markup,
                                    parse_mode=parse_mode,
                                    link_preview_options=LinkPreviewOptions(
                                        is_disabled=link_preview_disabled))


async def get_dynamic_keyboard(user_id=None):
    """Генерирует главную клавиатуру, включая динамические кнопки из БД."""
    keyboard = []
    row = []
    for key, menu in MENU_STRUCTURE.items():
        row.append(KeyboardButton(text=menu['label']))
        if len(row) == 2:
            keyboard.append(row)
            row = []

    # Добавляем динамические кнопки только если они существуют
    dynamic_btns = await get_all_keyboard_buttons()
    for btn in dynamic_btns:
        # Проверяем на системные имена и пустые лейблы
        lbl = btn['label'].lower().strip()
        if not btn['label'] or lbl in ["удалить lambi", "📝 редактировать чаты", "удалить ламби"]:
            continue
        row.append(KeyboardButton(text=btn['label']))
        if len(row) == 2:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)
    if ADMIN_ID and user_id == ADMIN_ID:
        keyboard.append([KeyboardButton(text="🔐 Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

@router.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    try:
        await add_user(user_id, message.from_user.username,
                       message.from_user.first_name, message.from_user.last_name)
    except Exception as e:
        logger.error(f"Error adding user in start: {e}")

    user_name = message.from_user.first_name or "Пользователь"
    user_link = f'<a href="tg://user?id={user_id}">{user_name}</a>'
    start_text = (
        f"<b>Привет</b>, {user_link} 😎\n\n"
        "Меня зовут Ламби, я помогу с поиском нужной тебе информации.\n\n"
        "А благодаря инлайн-режиму, ты можешь делиться информацией не только быстро и в пару кликов, но и где угодно: в личных переписках, чатах и каналах.\n"
        "<blockquote>Чтобы воспользоваться инлайн-режимом, введи в строке ввода сообщения юзер бота и выбирай нужный пункт</blockquote>\n\n"
        "<b>Блог владельца: t.me/+2m6vI9IYsBA0NTYy</b>\n"
        "<b>Лучший чат: t.me/+Mo58T7pcKxpmNjYy</b>")
    keyboard = await get_dynamic_keyboard(user_id)
    try:
        await message.answer_photo(photo=types.FSInputFile("start_image.jpg"),
                                   caption=start_text,
                                   reply_markup=keyboard,
                                   parse_mode=ParseMode.HTML)
    except Exception:
        await message.answer(start_text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

@router.message(F.text == "🔐 Админ-панель")
async def admin_button(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас нет доступа к админ-панели.")
        return

    # Очищаем состояние при входе в админ-панель
    await state.clear()

    admin_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📢 Рассылка")],
                  [KeyboardButton(text="📊 Статистика")],
                  [KeyboardButton(text="🏗 Управление меню")],
                  [KeyboardButton(text="✏️ Редактор контента")],
                  [KeyboardButton(text="🔙 Выйти")]],
        resize_keyboard=True)
    await message.answer("🔐 <b>Админ-панель</b>\n\nВыберите действие:",
                         reply_markup=admin_keyboard,
                         parse_mode=ParseMode.HTML)

class AdminMenuStates(StatesGroup):
    main = State()
    managing_menu = State()
    adding_button_label = State()
    adding_button_content = State()
    adding_button_photo = State()
    adding_inline_button_text = State()
    adding_inline_button_url = State()
    confirming_button = State()
    creating_nested = State() # For deep nesting
    button_action_menu = State()  # Меню действий над кнопкой
    renaming_button = State()  # Переименование кнопки
    reordering_buttons = State()  # Изменение порядка кнопок

@router.message(F.text == "🏗 Управление меню")
async def manage_menu(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return

    # Очищаем состояние при входе в управление меню
    await state.clear()

    buttons = await get_all_keyboard_buttons()
    text = "🏗 <b>Управление меню</b>\n\nВыберите кнопку для управления или создайте новую."
    kb = []
    for btn in buttons:
        kb.append([KeyboardButton(text=f"⚙️ {btn['label']}")])
    kb.append([KeyboardButton(text="➕ Создать новую кнопку")])
    kb.append([KeyboardButton(text="🔄 Изменить порядок кнопок")])
    kb.append([KeyboardButton(text="⬅️ Назад")])
    await state.set_state(AdminMenuStates.managing_menu)
    await message.answer(text, reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True), parse_mode=ParseMode.HTML)

@router.message(AdminMenuStates.managing_menu)
async def process_menu_management(message: types.Message, state: FSMContext):
    if message.text == "➕ Создать новую кнопку":
        await state.set_state(AdminMenuStates.adding_button_label)
        await message.answer("Введите название для кнопки:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))
    elif message.text == "🔄 Изменить порядок кнопок":
        # Показываем интерфейс изменения порядка кнопок
        await show_reorder_interface(message, state)
    elif message.text.startswith("⚙️ "):
        # Показываем меню управления конкретной кнопкой (только удаление)
        label = message.text[2:].strip()
        await state.update_data(selected_button_label=label)
        kb = [
            [KeyboardButton(text="❌ Удалить")],
            [KeyboardButton(text="⬅️ Назад")]
        ]
        await state.set_state(AdminMenuStates.button_action_menu)
        await message.answer(
            f"⚙️ <b>Управление кнопкой: {label}</b>\n\nВыберите действие:",
            reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
            parse_mode=ParseMode.HTML
        )
    elif message.text == "⬅️ Назад":
        await admin_button(message, state)

@router.message(AdminMenuStates.button_action_menu)
async def process_button_action(message: types.Message, state: FSMContext):
    """Обработка действий над выбранной кнопкой"""
    if message.text == "⬅️ Назад":
        return await manage_menu(message, state)

    data = await state.get_data()
    label = data.get('selected_button_label')

    if message.text == "❌ Удалить":
        # Удаляем кнопку
        success = await delete_keyboard_button(label)
        if success:
            await message.answer(f"✅ Кнопка '{label}' удалена.")
        else:
            await message.answer(f"❌ Ошибка при удалении кнопки '{label}'")
        return await manage_menu(message, state)

@router.message(AdminMenuStates.renaming_button)
async def process_button_rename(message: types.Message, state: FSMContext):
    """Обработка переименования кнопки клавиатуры"""
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    data = await state.get_data()
    old_label = data.get('selected_button_label')
    new_label = message.text.strip()

    if not new_label:
        await message.answer("❌ Название не может быть пустым")
        return

    # Переименовываем кнопку в БД
    success = await rename_keyboard_button(old_label, new_label)

    if success:
        await message.answer(f"✅ Кнопка переименована: '{old_label}' → '{new_label}'")
    else:
        await message.answer(f"❌ Ошибка при переименовании кнопки")

    await manage_menu(message, state)

async def show_reorder_interface(message: types.Message, state: FSMContext):
    """Показывает интерфейс для изменения порядка кнопок"""
    buttons = await get_all_keyboard_buttons()

    if not buttons:
        await message.answer("❌ Нет кнопок для изменения порядка")
        return await manage_menu(message, state)

    # Формируем текст с пронумерованными кнопками
    text = "🔄 <b>Изменение порядка кнопок</b>\n\n"
    text += "Текущий порядок:\n"

    for idx, btn in enumerate(buttons, 1):
        text += f"{idx}. {btn['label']}\n"

    text += "\nВыберите кнопку для перемещения:"

    # Создаём клавиатуру с кнопками
    kb = []
    for btn in buttons:
        kb.append([KeyboardButton(text=f"🔹 {btn['label']}")])
    kb.append([KeyboardButton(text="⬅️ Назад")])

    await state.set_state(AdminMenuStates.reordering_buttons)
    await message.answer(
        text,
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminMenuStates.reordering_buttons)
async def process_reordering(message: types.Message, state: FSMContext):
    """Обработка перемещения кнопок"""
    if message.text == "⬅️ Назад":
        return await manage_menu(message, state)

    if message.text == "⬆️ Вверх":
        # Перемещаем выбранную кнопку вверх
        data = await state.get_data()
        selected_label = data.get('reorder_selected_button')

        if not selected_label:
            await message.answer("❌ Кнопка не выбрана")
            return

        success = await move_button_up(selected_label)
        if success:
            await message.answer(f"✅ Кнопка '{selected_label}' перемещена вверх")
        else:
            await message.answer(f"❌ Не удалось переместить кнопку вверх (возможно, она уже первая)")

        # Показываем обновлённый список
        await show_reorder_interface(message, state)

    elif message.text == "⬇️ Вниз":
        # Перемещаем выбранную кнопку вниз
        data = await state.get_data()
        selected_label = data.get('reorder_selected_button')

        if not selected_label:
            await message.answer("❌ Кнопка не выбрана")
            return

        success = await move_button_down(selected_label)
        if success:
            await message.answer(f"✅ Кнопка '{selected_label}' перемещена вниз")
        else:
            await message.answer(f"❌ Не удалось переместить кнопку вниз (возможно, она уже последняя)")

        # Показываем обновлённый список
        await show_reorder_interface(message, state)

    elif message.text.startswith("🔹 "):
        # Выбрана кнопка для перемещения
        label = message.text[2:]
        await state.update_data(reorder_selected_button=label)

        # Показываем кнопки управления
        kb = [
            [KeyboardButton(text="⬆️ Вверх"), KeyboardButton(text="⬇️ Вниз")],
            [KeyboardButton(text="✅ Готово")],
            [KeyboardButton(text="⬅️ Назад")]
        ]

        await message.answer(
            f"🔹 Выбрана кнопка: <b>{label}</b>\n\nИспользуйте кнопки для перемещения:",
            reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
            parse_mode=ParseMode.HTML
        )

    elif message.text == "✅ Готово":
        await message.answer("✅ Порядок кнопок изменён!")
        await manage_menu(message, state)

@router.message(AdminMenuStates.adding_button_label)
async def add_btn_label(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена": return await manage_menu(message, state)
    await state.update_data(label=message.text)
    await state.set_state(AdminMenuStates.adding_button_content)
    await message.answer("Введите текст сообщения (поддерживается HTML):")

@router.message(AdminMenuStates.adding_button_content)
async def add_btn_content(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена": return await manage_menu(message, state)
    await state.update_data(content=message.text)
    await state.set_state(AdminMenuStates.adding_button_photo)
    await message.answer("Отправьте фото или напишите 'пропустить':", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="пропустить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(AdminMenuStates.adding_button_photo)
async def add_btn_photo(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена": return await manage_menu(message, state)
    photo = message.photo[-1].file_id if message.photo else None
    await state.update_data(photo=photo, inline_buttons_list=[]) # Initialize list
    await state.set_state(AdminMenuStates.adding_inline_button_text)
    await message.answer("Введите текст для инлайн-кнопки (или 'завершить'):",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="завершить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(AdminMenuStates.adding_inline_button_text)
async def add_inline_text(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена": return await manage_menu(message, state)
    if message.text == "завершить":
        data = await state.get_data()
        await finalize_creation(message, state, data)
        return
    await state.update_data(inline_label=message.text)
    await state.set_state(AdminMenuStates.adding_inline_button_url)
    await message.answer("Введите ссылку (URL) или 'меню' для создания вложенного раздела:",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="меню")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

async def process_dynamic_inline(query: types.CallbackQuery, state: FSMContext):
    """Handler for all dynamic inline buttons (callback_data starts with 'dyn:')"""
    button_id = query.data[4:]
    logger.info(f"🔄 Processing dynamic button: {button_id}")

    # Log click for statistics
    await log_click(button_id)

    item = await get_button_content(button_id)
    if not item:
        # Fallback to simple external link behavior if not in DB
        await query.answer("Информация временно недоступна.", show_alert=True)
        return

    # Check for inline buttons
    reply_markup = None
    if item['buttons_json']:
        try:
            btns_data = json.loads(item['buttons_json'])
            inline_kb = []
            for b in btns_data:
                # If the button has a URL, it's an external link
                if b.get('url') and b.get('url') != 'меню':
                    inline_kb.append([InlineKeyboardButton(text=b['text'], url=b['url'])])
                # If it's a nested menu link
                else:
                    # The button ID for the submenu is the one stored in data or label+text
                    # We use nested_id from creation: parent_id + ":" + b['text']
                    submenu_id = f"{button_id}:{b['text']}"
                    inline_kb.append([InlineKeyboardButton(text=b['text'], callback_data=make_callback_data(submenu_id))])

            # Add Back button if it's a submenu
            if item.get('parent_id'):
                inline_kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=make_callback_data(item['parent_id']))])

            reply_markup = InlineKeyboardMarkup(inline_keyboard=inline_kb)
        except Exception as e:
            logger.error(f"Error parsing buttons JSON: {e}")

    # Send content
    text = item['content']
    photo = item['photo_file_id']

    try:
        # For dynamic menus, we always try to edit the message to provide a smooth transition
        if photo:
            # If there's a photo, we use input_media to edit if possible, 
            # or just send new if it's easier to maintain state
            await query.message.answer_photo(photo, caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            await query.message.delete()
        else:
            await safe_edit_message(query, text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error displaying button content: {e}")
        # If edit fails (e.g. message is too old or same content), send as new
        if photo:
            await query.message.answer_photo(photo, caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            await query.message.answer(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)

@router.message(AdminMenuStates.adding_inline_button_url)
async def add_inline_url(message: types.Message, state: FSMContext):
    data = await state.get_data()
    inline_label = data.get('inline_label')

    if message.text == "меню":
        # Start creating a nested menu item
        # The parent is the current button we are configuring
        # If we are editing a nested button, we might need a better parent tracking
        # For now, let's use the current button label as parent
        await state.update_data(current_parent_id=data.get('editing_button_label') or data.get('label'))
        await state.set_state(AdminMenuStates.creating_nested)
        await message.answer(f"📝 Создаем вложенное меню для кнопки '<b>{inline_label}</b>'.\n\nВведите текст сообщения, который увидит пользователь:", parse_mode=ParseMode.HTML)
    else:
        # Standard URL button
        inline_buttons = data.get('inline_buttons_list', [])
        inline_buttons.append({"text": inline_label, "url": message.text})
        await state.update_data(inline_buttons_list=inline_buttons)

        await state.set_state(AdminMenuStates.adding_inline_button_text)
        await message.answer(f"✅ Кнопка '<b>{inline_label}</b>' добавлена.\n\nВведите текст для следующей инлайн-кнопки или напишите '<b>завершить</b>':",
                             reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="завершить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True),
                             parse_mode=ParseMode.HTML)

@router.message(AdminMenuStates.creating_nested)
async def process_nested_content(message: types.Message, state: FSMContext):
    data = await state.get_data()
    parent_id = data.get('current_parent_id')
    inline_label = data.get('inline_label')

    # Unique ID for nested content
    nested_id = f"{parent_id}:{inline_label}"

    # Support for photo, caption, and HTML formatting
    photo_file_id = message.photo[-1].file_id if message.photo else None
    content = message.caption or message.text if message.photo else message.text

    # Process fonts/formatting - aiogram does this automatically if parse_mode is HTML
    # and the user uses Telegram's built-in formatting.

    # Save the nested content with full support (photo, content)
    await update_button_content(nested_id, content, photo_file_id=photo_file_id, parent_id=parent_id)

    # Add trigger button to parent's list
    inline_buttons = data.get('inline_buttons_list', [])
    inline_buttons.append({"text": inline_label, "url": "меню"})
    await state.update_data(inline_buttons_list=inline_buttons)

    await state.set_state(AdminMenuStates.adding_inline_button_text)
    await message.answer(f"✅ Вложенный раздел '<b>{inline_label}</b>' создан.\n\n"
                         "Теперь вы можете:\n"
                         "1. Ввести текст для следующей инлайн-кнопки (на этом же уровне)\n"
                         "2. Написать '<b>завершить</b>', чтобы сохранить всё меню",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="завершить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True),
                         parse_mode=ParseMode.HTML)

async def finalize_creation(message: types.Message, state: FSMContext, data: dict):
    label = data['label']
    content = data['content']
    photo = data.get('photo')
    inline_buttons = data.get('inline_buttons_list', [])

    # ПРИНУДИТЕЛЬНО добавляем кнопку в keyboard_buttons, чтобы она отображалась в меню
    print(f"[BOT_DEBUG] Finalizing creation for '{label}'. Adding to keyboard_buttons...")
    await add_keyboard_button(label)

    # Сохраняем контент
    await update_button_content(label, content, photo, json.dumps(inline_buttons) if inline_buttons else None)

    await message.answer(f"✅ Кнопка '{label}' создана со всеми подменю!")
    await manage_menu(message, state)

async def handle_all_text_messages(message: types.Message, state: FSMContext):
    label = message.text
    if not label: return

    print(f"\n[BOT_DEBUG_VERBOSE] === Global Handler Start === Label: '{label}'")

    # 0. Сначала проверяем системные кнопки возврата
    if label in ["🔙 Назад", "🔙 Назад в меню", "🔙 Выйти"]:
        print(f"[BOT_DEBUG_VERBOSE] System back button: '{label}'")
        return await cmd_start(message, state)

    # 1. Проверяем состояния FSM (ВЫСШИЙ ПРИОРИТЕТ ДЛЯ АДМИНКИ)
    current_state = await state.get_state()
    print(f"[BOT_DEBUG_VERBOSE] Current State: {current_state}")
    if current_state:
        state_str = str(current_state)
        # Если состояние содержит ключевые слова ожидания ввода - выходим, даем сработать другим хендлерам
        text_expecting_keywords = ["waiting", "adding", "editing", "creating", "confirming", "managing", "main"]
        if any(k in state_str.lower() for k in text_expecting_keywords):
            print(f"[BOT_DEBUG_VERBOSE] State '{state_str}' is active. Letting FSM handler proceed.")
            return

    # 2. Проверяем динамические кнопки (БД и статика)
    try:
        print(f"[BOT_DEBUG_VERBOSE] Trying handle_dynamic_buttons for '{label}'")
        handled = await handle_dynamic_buttons(message, state)
        if handled:
            print(f"[BOT_DEBUG_VERBOSE] ✅ Handled by handle_dynamic_buttons")
            return
        else:
            print(f"[BOT_DEBUG_VERBOSE] ❌ NOT handled by handle_dynamic_buttons: '{label}'")
    except Exception as e:
        print(f"[BOT_DEBUG_VERBOSE] ❌ CRITICAL Error in handle_dynamic_buttons: {e}")
        import traceback
        traceback.print_exc()

    # 3. Проверка статических команд
    if label.startswith("/"):
        print(f"[BOT_DEBUG_VERBOSE] Command detected, ignoring fallback.")
        return

    # 4. Если ничего не подошло, показываем меню
    print(f"[BOT_DEBUG_VERBOSE] Fallback: No match for '{label}'. Showing menu.")
    keyboard = await get_dynamic_keyboard(message.from_user.id)
    await message.answer("Пожалуйста, используйте кнопки меню для навигации.", reply_markup=keyboard)

@router.message(AdminMenuStates.managing_menu, F.text == "➕ Добавить кнопку")
async def add_btn_start(message: types.Message, state: FSMContext):
    await state.set_state(AdminMenuStates.adding_button_label)
    await message.answer("Введите название новой кнопки:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(AdminMenuStates.adding_button_label)
async def add_btn_finish(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена": return await manage_menu(message, state)
    label = message.text

    # Check if this button is already in MENU_STRUCTURE (static)
    if label in [m['label'] for m in MENU_STRUCTURE.values()]:
        await message.answer(f"❌ Кнопка '{label}' является системной и не может быть создана заново.")
        return await manage_menu(message, state)

    await add_keyboard_button(label)
    await update_button_content(label, f"Контент для кнопки {label}")
    await message.answer(f"✅ Кнопка '{label}' добавлена.")
    await manage_menu(message, state)

@router.message(AdminMenuStates.managing_menu, F.text == "❌ Удалить кнопку")
async def delete_btn_start(message: types.Message, state: FSMContext):
    buttons = await get_all_keyboard_buttons()
    if not buttons:
        await message.answer("Нет кнопок для удаления.")
        return
    kb = [[KeyboardButton(text=btn['label'])] for btn in buttons]
    kb.append([KeyboardButton(text="⬅️ Отмена")])
    await message.answer("Выберите кнопку для удаления:", reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True))

@router.message(AdminMenuStates.managing_menu, F.text == "⬅️ Назад")
async def back_to_admin_from_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await admin_button(message, state)

@router.message(F.text == "📝 Редактировать кнопки")
async def start_button_edit(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return

    # Get all buttons (top-level and nested)
    async with pool.acquire() as conn:
        all_items = await conn.fetch('SELECT button_id FROM button_content ORDER BY button_id')

    kb = []
    for item in all_items:
        kb.append([KeyboardButton(text=f"EDIT:{item['button_id']}")])

    kb.append([KeyboardButton(text="⬅️ Отмена")])
    await state.set_state(ButtonEditStates.selecting_button)
    await message.answer("📝 <b>Редактирование текстов</b>\n\nВыберите кнопку или подменю для изменения:", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
                         parse_mode=ParseMode.HTML)

@router.message(ButtonEditStates.selecting_button, F.text.startswith("EDIT:"))
async def select_edit(message: types.Message, state: FSMContext):
    label = message.text[5:]
    await state.update_data(editing_button_label=label)
    await state.set_state(ButtonEditStates.waiting_for_content)
    await message.answer(f"Редактируем '{label}'. Введите новый текст:")

@router.message(ButtonEditStates.selecting_button, F.text.startswith("BTN:"))
async def select_button_for_edit(message: types.Message, state: FSMContext):
    button_label = message.text[4:]
    await state.update_data(editing_button_label=button_label)
    await state.set_state(ButtonEditStates.waiting_for_content)
    await message.answer(f"Введите новый текст для кнопки '{button_label}':\n(Поддерживается HTML: <b></b>, <i></i>, <a href=''></a>)", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(ButtonEditStates.waiting_for_content)
async def process_button_content(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        await state.clear()
        return await admin_button(message, state)

    await state.update_data(new_content=message.text)
    await state.set_state(ButtonEditStates.waiting_for_photo)
    await message.answer("Отправьте фото для этой кнопки или 'пропустить':", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="пропустить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(ButtonEditStates.waiting_for_photo)
async def process_button_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id if message.photo else None
    await state.update_data(new_photo=photo_id)
    await state.set_state(ButtonEditStates.waiting_for_inline_buttons)
    await message.answer("Введите инлайн-кнопки в формате 'Название - Ссылка' (каждая с новой строки) или 'нет':",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="нет")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(ButtonEditStates.waiting_for_inline_buttons)
async def save_button_config(message: types.Message, state: FSMContext):
    data = await state.get_data()
    label = data['editing_button_label']
    content = data['new_content']
    photo = data['new_photo']

    inline_buttons = []
    if message.text != "нет":
        for line in message.text.split('\n'):
            if ' - ' in line:
                name, url = line.split(' - ', 1)
                inline_buttons.append({'text': name.strip(), 'url': url.strip()})

    success = await update_button_content(label, content, photo, json.dumps(inline_buttons) if inline_buttons else None)

    if success:
        await message.answer(f"✅ Кнопка '{label}' успешно обновлена!")
    else:
        await message.answer(f"❌ Ошибка при обновлении кнопки '{label}'.")

    await state.clear()
    await admin_button(message, state)

# ============ НОВЫЙ РЕДАКТОР КОНТЕНТА ============

# Вспомогательные функции для управления инлайн-кнопками
async def delete_inline_button(button_label: str, button_to_delete: dict) -> bool:
    """Удаляет инлайн-кнопку из меню (работает с кнопками из БД и статическими)"""
    try:
        # Получаем контент из БД
        db_content = await get_button_content(button_label)

        # Если контента нет в БД, но это статическое меню - создаем запись в БД
        if not db_content:
            # Пробуем найти статическое меню
            static_menu_info = find_static_menu_by_label(button_label)
            if static_menu_info:
                static_menu_data = static_menu_info['menu_data']
                # Получаем текст из статического меню
                if 'pages' in static_menu_data and static_menu_data['pages']:
                    text_content = static_menu_data['pages'][0].get('text', '')
                else:
                    text_content = static_menu_data.get('text', '')

                # Получаем статические кнопки
                static_buttons = []
                if static_menu_data.get('type') == 'inline' and static_menu_data.get('submenu'):
                    for submenu_id, submenu_data in static_menu_data['submenu'].items():
                        static_buttons.append({
                            'text': submenu_data.get('label', submenu_id),
                            'id': submenu_id
                        })

                # Создаем запись в БД с существующими статическими кнопками
                await update_button_content(button_label, text_content, None, json.dumps(static_buttons) if static_buttons else None, 'HTML', None)
                db_content = await get_button_content(button_label)

        if not db_content:
            return False

        # Получаем текущие кнопки
        buttons = []
        if db_content.get('buttons_json'):
            try:
                buttons = json.loads(db_content['buttons_json'])
            except:
                pass

        # Удаляем кнопку (работает для любых кнопок)
        buttons = [b for b in buttons if b.get('text') != button_to_delete['text']]

        # Сохраняем обновленный список
        success = await update_button_content(
            button_label,
            db_content.get('content'),
            db_content.get('photo_file_id'),
            json.dumps(buttons) if buttons else None,
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id')
        )

        return success
    except Exception as e:
        print(f"Error deleting inline button: {e}")
        return False

async def rename_inline_button(button_label: str, button_to_rename: dict, new_name: str) -> bool:
    """Переименовывает инлайн-кнопку (работает с кнопками из БД и статическими)"""
    try:
        # Получаем контент из БД
        db_content = await get_button_content(button_label)

        # Если контента нет в БД, но это статическое меню - создаем запись в БД
        if not db_content:
            # Пробуем найти статическое меню
            static_menu_info = find_static_menu_by_label(button_label)
            if static_menu_info:
                static_menu_data = static_menu_info['menu_data']
                # Получаем текст из статического меню
                if 'pages' in static_menu_data and static_menu_data['pages']:
                    text_content = static_menu_data['pages'][0].get('text', '')
                else:
                    text_content = static_menu_data.get('text', '')

                # Получаем статические кнопки
                static_buttons = []
                if static_menu_data.get('type') == 'inline' and static_menu_data.get('submenu'):
                    for submenu_id, submenu_data in static_menu_data['submenu'].items():
                        static_buttons.append({
                            'text': submenu_data.get('label', submenu_id),
                            'id': submenu_id
                        })

                # Создаем запись в БД с существующими статическими кнопками
                await update_button_content(button_label, text_content, None, json.dumps(static_buttons) if static_buttons else None, 'HTML', None)
                db_content = await get_button_content(button_label)

        if not db_content:
            return False

        # Получаем текущие кнопки
        buttons = []
        if db_content.get('buttons_json'):
            try:
                buttons = json.loads(db_content['buttons_json'])
            except:
                pass

        # Переименовываем кнопку
        if button_to_rename['source'] == 'db':
            # Ищем и переименовываем в списке кнопок БД
            for b in buttons:
                if b.get('text') == button_to_rename['text']:
                    b['text'] = new_name
                    break
        else:
            # Для статических кнопок - создаем override в БД
            # Находим статическую кнопку и добавляем её с новым именем
            button_found = False
            for b in buttons:
                if b.get('text') == button_to_rename['text']:
                    b['text'] = new_name
                    button_found = True
                    break

            # Если кнопка не найдена в БД списке (только статическая)
            if not button_found:
                # Добавляем переименованную кнопку в БД
                if button_to_rename.get('type') == '🔗 URL':
                    buttons.append({
                        'text': new_name,
                        'url': button_to_rename.get('url', '')
                    })
                else:
                    buttons.append({
                        'text': new_name,
                        'id': button_to_rename.get('id', '')
                    })

        # Сохраняем обновленный список
        success = await update_button_content(
            button_label,
            db_content.get('content'),
            db_content.get('photo_file_id'),
            json.dumps(buttons) if buttons else None,
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id')
        )

        return success
    except Exception as e:
        print(f"Error renaming inline button: {e}")
        return False

def find_static_menu_by_label(label, structure=None, parent_path=""):
    """Рекурсивно ищет меню по label в MENU_STRUCTURE"""
    if structure is None:
        structure = MENU_STRUCTURE

    for menu_id, menu_data in structure.items():
        current_path = f"{parent_path}:{menu_id}" if parent_path else menu_id

        if menu_data.get('label') == label:
            return {
                'menu_id': menu_id,
                'menu_data': menu_data,
                'path': current_path
            }

        # Рекурсивно ищем в подменю
        if 'submenu' in menu_data:
            result = find_static_menu_by_label(label, menu_data['submenu'], current_path)
            if result:
                return result

    return None

@router.message(F.text == "✏️ Редактор контента")
async def content_editor_start(message: types.Message, state: FSMContext):
    """Главное меню редактора контента - показываем статические кнопки и кнопки из БД"""
    if message.from_user.id != ADMIN_ID:
        return

    # Очищаем состояние при входе в редактор контента
    await state.clear()

    # Формируем список кнопок для выбора
    kb = []

    # Добавляем статические кнопки из MENU_STRUCTURE
    for menu_id, menu_data in MENU_STRUCTURE.items():
        label = menu_data.get('label', menu_id)
        kb.append([KeyboardButton(text=f"📝 {label}")])

    # Получаем кнопки клавиатуры из БД
    keyboard_buttons = await get_all_keyboard_buttons()

    # Добавляем кнопки из БД
    for btn in keyboard_buttons:
        label = btn.get('label', 'Без названия')
        kb.append([KeyboardButton(text=f"📝 {label}")])

    if not kb:
        await message.answer(
            "📋 <b>Редактор контента</b>\n\n"
            "Нет кнопок для редактирования.\n"
            "Создайте кнопки через '🏗 Управление меню'",
            parse_mode=ParseMode.HTML
        )
        return

    kb.append([KeyboardButton(text="⬅️ Назад")])

    await state.set_state(ContentEditorStates.selecting_menu)
    await message.answer(
        "✏️ <b>Редактор контента</b>\n\n"
        "Выберите кнопку для редактирования:",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.selecting_menu, F.text == "📝 Изменить текст")
async def content_editor_edit_text_handler(message: types.Message, state: FSMContext):
    """Начало редактирования текста кнопки"""
    await state.set_state(ContentEditorStates.editing_text)
    await message.answer(
        "✏️ <b>Редактирование текста</b>\n\n"
        "Введите новый текст. Поддерживается HTML форматирование:\n"
        "• <code>&lt;b&gt;жирный&lt;/b&gt;</code> → <b>жирный</b>\n"
        "• <code>&lt;i&gt;курсив&lt;/i&gt;</code> → <i>курсив</i>\n"
        "• <code>&lt;a href='URL'&gt;текст&lt;/a&gt;</code> → ссылка\n"
        "• <code>&lt;code&gt;код&lt;/code&gt;</code> → <code>код</code>",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.selecting_menu, F.text.startswith("📝 "))
async def content_editor_select(message: types.Message, state: FSMContext):
    """Обработка выбора кнопки для редактирования"""
    button_label = message.text[2:]  # Убираем "📝 "

    await state.update_data(editing_button_label=button_label)

    # После миграции все данные в БД, MENU_STRUCTURE пустой
    db_content = await get_button_content(button_label)

    # Определяем текст и фото
    if db_content:
        # Контент найден в БД
        current_text = db_content.get('content', 'Нет текста')
        has_photo = "✅" if db_content.get('photo_file_id') else "❌"
        await state.update_data(has_db_content=True, has_static_menu=False)
    else:
        # Контента нет в БД - создаём с пустым текстом
        # Это нормально если кнопка только добавлена и контент ещё не создан
        current_text = "<i>Текст ещё не задан. Нажмите '📝 Изменить текст' чтобы добавить контент.</i>"
        has_photo = "❌"
        await state.update_data(has_db_content=False, has_static_menu=False)
        print(f"[CONTENT_EDITOR] No content found for '{button_label}', will create on first edit")

    # Собираем инлайн-кнопки из БД
    all_buttons = []
    idx = 1

    # После миграции все инлайн-кнопки в БД (в buttons_json)
    if db_content and db_content.get('buttons_json'):
        try:
            buttons = json.loads(db_content['buttons_json'])

            for btn in buttons:
                btn_text = btn.get('text', 'Кнопка')

                if btn.get('url'):
                    all_buttons.append({
                        'index': idx,
                        'text': btn_text,
                        'type': '🔗 URL',
                        'source': 'db',
                        'url': btn['url']
                    })
                else:
                    submenu_id = btn.get('id', f"{button_label}:{btn_text}")
                    all_buttons.append({
                        'index': idx,
                        'text': btn_text,
                        'type': '📄 меню',
                        'source': 'db',
                        'goto': f"db:{submenu_id}",
                        'id': submenu_id
                    })
                idx += 1
        except Exception as e:
            print(f"[CONTENT_EDITOR] Error parsing buttons_json: {e}")
            pass

    # Сохраняем информацию о кнопках
    await state.update_data(all_inline_buttons=all_buttons)

    # Формируем меню управления
    kb = [
        [KeyboardButton(text="📝 Изменить текст")],
        [KeyboardButton(text="🖼 Изменить фото")],
        [KeyboardButton(text="✏️ Переименовать кнопку")],
    ]

    # Добавляем кнопку управления страницами если есть pages_json
    if db_content and db_content.get('pages_json'):
        try:
            pages = json.loads(db_content['pages_json'])
            if pages:
                print(f"[CONTENT_EDITOR] Adding pages button: {len(pages)} pages")
                kb.append([KeyboardButton(text=f"📄 Управление страницами ({len(pages)} стр.)")])
            else:
                print(f"[CONTENT_EDITOR] pages_json empty for '{button_label}'")
        except Exception as e:
            print(f"[CONTENT_EDITOR] Error parsing pages_json: {e}")
    else:
        print(f"[CONTENT_EDITOR] No pages_json for '{button_label}'")
        if db_content:
            print(f"[CONTENT_EDITOR] db_content keys: {db_content.keys()}")

    # Добавляем каждую инлайн-кнопку как отдельную кнопку в клавиатуре
    if all_buttons:
        kb.append([KeyboardButton(text="📋 Инлайн-кнопки:")])
        for btn in all_buttons:
            btn_type_icon = "🔗" if btn['type'] == '🔗 URL' else "📄"
            kb.append([KeyboardButton(text=f"🔘 {btn_type_icon} {btn['text']}")])

    kb.append([KeyboardButton(text="➕ Добавить инлайн-кнопку")])

    # Добавляем кнопку настройки расположения если есть инлайн кнопки
    if all_buttons:
        kb.append([KeyboardButton(text="⚙️ Расположение кнопок")])

    kb.append([KeyboardButton(text="⬅️ Назад")])

    text_preview = current_text[:300] + "..." if len(current_text) > 300 else current_text

    await message.answer(
        f"✏️ <b>Редактирование: {button_label}</b>\n\n"
        f"📄 <b>Текст:</b>\n{text_preview}\n\n"
        f"🖼 <b>Фото:</b> {has_photo}\n\n"
        f"💡 Нажмите на инлайн-кнопку для редактирования",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

# Обработка нажатия на инлайн-кнопку для управления
@router.message(ContentEditorStates.selecting_menu, F.text.startswith("🔘 "))
async def content_editor_manage_inline_button(message: types.Message, state: FSMContext):
    """Управление конкретной инлайн-кнопкой"""
    button_display = message.text[2:]  # Убираем "🔘 "

    # Извлекаем название кнопки (убираем иконки 🔗 или 📄)
    if button_display.startswith("🔗 "):
        button_name = button_display[2:].strip()
        btn_type = "url"
    elif button_display.startswith("📄 "):
        button_name = button_display[2:].strip()
        btn_type = "submenu"
    else:
        button_name = button_display.strip()
        btn_type = "unknown"

    data = await state.get_data()
    all_buttons = data.get('all_inline_buttons', [])

    # Находим эту кнопку в списке
    selected_button = None
    for btn in all_buttons:
        if btn['text'] == button_name:
            selected_button = btn
            break

    if not selected_button:
        await message.answer("❌ Кнопка не найдена")
        return

    # Сохраняем выбранную кнопку
    await state.update_data(selected_inline_button=selected_button)

    # Формируем меню управления
    kb = []

    if selected_button['type'] == '🔗 URL':
        info = f"🔗 <b>URL кнопка:</b> {selected_button['text']}\n\n"
        info += f"<b>Ссылка:</b> <code>{selected_button.get('url', 'N/A')}</code>\n\n"
        info += "Что хотите сделать?"

        kb.append([KeyboardButton(text="✏️ Изменить URL")])
    else:
        info = f"📄 <b>Кнопка подменю:</b> {selected_button['text']}\n\n"
        info += f"<b>ID подменю:</b> <code>{selected_button.get('id', 'N/A')}</code>\n\n"
        info += "Что хотите сделать?"

        kb.append([KeyboardButton(text="📝 Изменить текст внутри")])
        kb.append([KeyboardButton(text="📂 Открыть подменю")])

    kb.append([KeyboardButton(text="✏️ Переименовать")])
    kb.append([KeyboardButton(text="⚙️ Изменить ширину")])
    kb.append([KeyboardButton(text="🗑 Удалить")])
    kb.append([KeyboardButton(text="⬅️ Назад")])

    await state.set_state(ContentEditorStates.managing_inline_buttons)
    await message.answer(
        info,
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

# Обработка перехода к вложенному меню по GOTO:
@router.message(ContentEditorStates.selecting_menu, F.text.startswith("GOTO:db:"))
async def content_editor_goto_submenu(message: types.Message, state: FSMContext):
    """Переход к редактированию вложенного меню"""
    goto_path = message.text[8:]  # Убираем "GOTO:db:"

    # Переключаемся на редактирование этого подменю
    await state.update_data(editing_button_label=goto_path)

    # Получаем контент
    db_content = await get_button_content(goto_path)

    if not db_content:
        await message.answer("❌ Контент для этого подменю не найден")
        return await content_editor_start(message, state)

    # Показываем редактор для этого подменю
    fake_msg = message.model_copy(update={"text": f"📝 {goto_path}"})
    await content_editor_select(fake_msg, state)

@router.message(ContentEditorStates.editing_text)
async def content_editor_save_text(message: types.Message, state: FSMContext):
    """Сохранение нового текста"""
    if message.text == "⬅️ Отмена":
        await state.clear()
        return await content_editor_start(message, state)

    data = await state.get_data()
    editing_submenu_id = data.get('editing_submenu_id')
    button_label = data.get('editing_button_label')
    new_text = message.text

    # Проверяем, редактируем ли мы текст подменю
    if editing_submenu_id:
        # Редактируем текст инлайн-кнопки подменю
        db_content = await get_button_content(editing_submenu_id)

        if db_content:
            success = await update_button_content(
                editing_submenu_id,
                new_text,
                db_content.get('photo_file_id'),
                db_content.get('buttons_json'),
                db_content.get('parse_mode', 'HTML'),
                db_content.get('parent_id')
            )
        else:
            # Создаем новый контент для подменю
            success = await update_button_content(editing_submenu_id, new_text, None, None, 'HTML', button_label)

        if success:
            await message.answer("✅ Текст подменю успешно обновлен!")
        else:
            await message.answer("❌ Ошибка при обновлении")

        await state.clear()
        await admin_button(message, state)
        return

    # Обычное редактирование текста кнопки клавиатуры
    db_content = await get_button_content(button_label)

    if db_content:
        # Контент существует - обновляем его
        success = await update_button_content(
            button_label,
            new_text,
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id')
        )

        if success:
            await message.answer("✅ Текст успешно обновлен!")
        else:
            await message.answer("❌ Ошибка при обновлении")
    else:
        # Контента нет в БД - создаём новый
        # После миграции все кнопки должны иметь контент в БД
        # Если контента нет, создаём пустой контент
        print(f"[CONTENT_EDITOR] Creating new content for button: {button_label}")

        success = await update_button_content(
            button_label,
            new_text,
            None,  # photo_file_id
            None,  # buttons_json (будет пустой, можно добавить кнопки потом)
            'HTML',
            None   # parent_id
        )

        if success:
            await message.answer("✅ Контент создан! Теперь можете добавить инлайн-кнопки.")
        else:
            await message.answer("❌ Ошибка при создании контента")

    await state.clear()
    await admin_button(message, state)

@router.message(ContentEditorStates.selecting_menu, F.text == "✏️ Переименовать кнопку")
async def content_editor_rename_keyboard_button_start(message: types.Message, state: FSMContext):
    """Начало переименования кнопки клавиатуры"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')

    await state.set_state(ContentEditorStates.editing_keyboard_button_name)
    await message.answer(
        f"✏️ <b>Переименование кнопки</b>\n\n"
        f"Текущее название: <b>{button_label}</b>\n\n"
        f"Введите новое название для кнопки:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.editing_keyboard_button_name)
async def content_editor_rename_keyboard_button_save(message: types.Message, state: FSMContext):
    """Сохранение нового названия кнопки клавиатуры"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    new_name = message.text.strip()
    if not new_name:
        await message.answer("❌ Название не может быть пустым")
        return

    data = await state.get_data()
    old_label = data.get('editing_button_label')

    # Переименовываем в БД
    success = await rename_keyboard_button(old_label, new_name)

    if success:
        await message.answer(f"✅ Кнопка переименована: '{old_label}' → '{new_name}'")
        await state.update_data(editing_button_label=new_name)

        # Показываем обновленный редактор
        await state.set_state(ContentEditorStates.selecting_menu)
        fake_msg = message.model_copy(update={"text": f"📝 {new_name}"})
        return await content_editor_select(fake_msg, state)
    else:
        await message.answer("❌ Ошибка при переименовании кнопки")

@router.message(ContentEditorStates.selecting_menu, F.text == "⚙️ Расположение кнопок")
async def content_editor_set_buttons_layout(message: types.Message, state: FSMContext):
    """Настройка расположения инлайн-кнопок"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')

    # Получаем текущее значение из БД
    db_content = await get_button_content(button_label)
    current_layout = 1
    if db_content and db_content.get('buttons_per_row'):
        current_layout = db_content['buttons_per_row']

    kb = [
        [KeyboardButton(text="1️⃣ По 1 в ряду")],
        [KeyboardButton(text="2️⃣ По 2 в ряду")],
        [KeyboardButton(text="3️⃣ По 3 в ряду")],
        [KeyboardButton(text="4️⃣ По 4 в ряду")],
        [KeyboardButton(text="⬅️ Отмена")]
    ]

    await state.set_state(ContentEditorStates.setting_buttons_layout)
    await message.answer(
        f"⚙️ <b>Настройка расположения инлайн-кнопок</b>\n\n"
        f"Текущее: <b>{current_layout} кнопок в ряду</b>\n\n"
        f"Выберите сколько кнопок показывать в одном ряду:",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.setting_buttons_layout)
async def content_editor_save_buttons_layout(message: types.Message, state: FSMContext):
    """Сохранение настройки расположения"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    # Определяем количество кнопок в ряду
    layout_map = {
        "1️⃣ По 1 в ряду": 1,
        "2️⃣ По 2 в ряду": 2,
        "3️⃣ По 3 в ряду": 3,
        "4️⃣ По 4 в ряду": 4
    }

    buttons_per_row = layout_map.get(message.text)
    if not buttons_per_row:
        await message.answer("❌ Неверный выбор")
        return

    data = await state.get_data()
    button_label = data.get('editing_button_label')

    # Получаем текущий контент
    db_content = await get_button_content(button_label)

    if db_content:
        # Обновляем с новым параметром расположения
        success = await update_button_content(
            button_label,
            db_content.get('content'),
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            buttons_per_row
        )

        if success:
            await message.answer(f"✅ Расположение обновлено: {buttons_per_row} кнопок в ряду")
        else:
            await message.answer("❌ Ошибка при сохранении")
    else:
        await message.answer("❌ Контент не найден")

    # Возвращаемся в редактор
    await state.set_state(ContentEditorStates.selecting_menu)
    fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
    return await content_editor_select(fake_msg, state)
# ============= ОБРАБОТЧИКИ УПРАВЛЕНИЯ СТРАНИЦАМИ =============

@router.message(ContentEditorStates.selecting_menu, F.text.startswith("📄 Управление страницами"))
async def content_editor_manage_pages(message: types.Message, state: FSMContext):
    """Показать список страниц для редактирования"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')

    db_content = await get_button_content(button_label)

    if not db_content or not db_content.get('pages_json'):
        await message.answer("❌ У этой кнопки нет страниц")
        return

    try:
        pages = json.loads(db_content['pages_json'])
    except:
        await message.answer("❌ Ошибка при загрузке страниц")
        return

    # Сохраняем pages в state
    await state.update_data(pages=pages)
    await state.set_state(ContentEditorStates.managing_pages)

    # Формируем клавиатуру со списком страниц
    kb = []
    for i, page in enumerate(pages):
        page_preview = page.get('text', '')[:50] + "..." if len(page.get('text', '')) > 50 else page.get('text', '')
        kb.append([KeyboardButton(text=f"📄 {i+1}. {page_preview}")])

    kb.append([KeyboardButton(text="➕ Добавить новую страницу")])
    kb.append([KeyboardButton(text="⬅️ Назад")])

    await message.answer(
        f"📄 <b>Управление страницами: {button_label}</b>\n\n"
        f"Всего страниц: {len(pages)}\n\n"
        f"Выберите страницу для редактирования или удаления:",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.managing_pages, F.text.startswith("📄 "))
async def content_editor_select_page(message: types.Message, state: FSMContext):
    """Выбор страницы для редактирования"""
    try:
        # Извлекаем номер страницы из текста "📄 1. текст..."
        page_num = int(message.text.split(".")[0].replace("📄 ", "").strip()) - 1

        data = await state.get_data()
        pages = data.get('pages', [])

        if page_num < 0 or page_num >= len(pages):
            await message.answer("❌ Неверный номер страницы")
            return

        page = pages[page_num]
        page_text = page.get('text', '')

        # Сохраняем выбранную страницу
        await state.update_data(selected_page_index=page_num)

        # Показываем меню редактирования страницы
        kb = [
            [KeyboardButton(text="✏️ Редактировать текст")],
            [KeyboardButton(text="🗑 Удалить страницу")],
            [KeyboardButton(text="⬆️ Переместить вверх")] if page_num > 0 else [],
            [KeyboardButton(text="⬇️ Переместить вниз")] if page_num < len(pages) - 1 else [],
            [KeyboardButton(text="⬅️ Назад")]
        ]

        # Убираем пустые списки
        kb = [row for row in kb if row]

        text_preview = page_text[:500] + "..." if len(page_text) > 500 else page_text

        await message.answer(
            f"📄 <b>Страница {page_num + 1} из {len(pages)}</b>\n\n"
            f"{text_preview}\n\n"
            f"Выберите действие:",
            reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@router.message(ContentEditorStates.managing_pages, F.text == "✏️ Редактировать текст")
async def content_editor_edit_page_text(message: types.Message, state: FSMContext):
    """Начать редактирование текста страницы"""
    await state.set_state(ContentEditorStates.editing_page)
    await message.answer(
        "✏️ <b>Редактирование текста страницы</b>\n\n"
        "Введите новый текст. Поддерживается HTML форматирование:\n"
        "• <code>&lt;b&gt;жирный&lt;/b&gt;</code> → <b>жирный</b>\n"
        "• <code>&lt;i&gt;курсив&lt;/i&gt;</code> → <i>курсив</i>\n"
        "• <code>&lt;a href='URL'&gt;текст&lt;/a&gt;</code> → ссылка\n"
        "• <code>&lt;code&gt;код&lt;/code&gt;</code> → <code>код</code>",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.editing_page)
async def content_editor_save_page_text(message: types.Message, state: FSMContext):
    """Сохранить отредактированный текст страницы"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.managing_pages)
        return await content_editor_manage_pages(message, state)

    data = await state.get_data()
    button_label = data.get('editing_button_label')
    pages = data.get('pages', [])
    page_index = data.get('selected_page_index', 0)

    # Обновляем текст страницы
    pages[page_index]['text'] = message.text

    # Сохраняем в БД
    db_content = await get_button_content(button_label)

    if db_content:
        pages_json = json.dumps(pages)

        success = await update_button_content(
            button_label,
            pages[0]['text'],  # Первая страница для content
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            db_content.get('buttons_per_row'),
            pages_json
        )

        if success:
            await message.answer(f"✅ Страница {page_index + 1} обновлена!")
            await state.update_data(pages=pages)
        else:
            await message.answer("❌ Ошибка при сохранении")

    # Вернуться к списку страниц
    await state.set_state(ContentEditorStates.managing_pages)
    fake_msg = message.model_copy(update={"text": f"📄 Управление страницами"})
    return await content_editor_manage_pages(fake_msg, state)

@router.message(ContentEditorStates.managing_pages, F.text == "🗑 Удалить страницу")
async def content_editor_delete_page(message: types.Message, state: FSMContext):
    """Удалить страницу"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    pages = data.get('pages', [])
    page_index = data.get('selected_page_index', 0)

    if len(pages) <= 1:
        await message.answer("❌ Нельзя удалить последнюю страницу!")
        return

    # Удаляем страницу
    deleted_page = pages.pop(page_index)

    # Сохраняем в БД
    db_content = await get_button_content(button_label)

    if db_content:
        pages_json = json.dumps(pages)

        success = await update_button_content(
            button_label,
            pages[0]['text'],  # Первая страница для content
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            db_content.get('buttons_per_row'),
            pages_json
        )

        if success:
            await message.answer(f"✅ Страница {page_index + 1} удалена! Осталось страниц: {len(pages)}")
            await state.update_data(pages=pages)
        else:
            await message.answer("❌ Ошибка при сохранении")

    # Вернуться к списку страниц
    await state.set_state(ContentEditorStates.managing_pages)
    fake_msg = message.model_copy(update={"text": f"📄 Управление страницами"})
    return await content_editor_manage_pages(fake_msg, state)

@router.message(ContentEditorStates.managing_pages, F.text == "➕ Добавить новую страницу")
async def content_editor_add_page_prompt(message: types.Message, state: FSMContext):
    """Начать добавление новой страницы"""
    await state.set_state(ContentEditorStates.adding_page)
    await message.answer(
        "➕ <b>Добавление новой страницы</b>\n\n"
        "Введите текст для новой страницы. Поддерживается HTML форматирование:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.adding_page)
async def content_editor_add_page(message: types.Message, state: FSMContext):
    """Добавить новую страницу"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.managing_pages)
        return await content_editor_manage_pages(message, state)

    data = await state.get_data()
    button_label = data.get('editing_button_label')
    pages = data.get('pages', [])

    # Добавляем новую страницу
    pages.append({'text': message.text})

    # Сохраняем в БД
    db_content = await get_button_content(button_label)

    if db_content:
        pages_json = json.dumps(pages)

        success = await update_button_content(
            button_label,
            pages[0]['text'],  # Первая страница для content
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            db_content.get('buttons_per_row'),
            pages_json
        )

        if success:
            await message.answer(f"✅ Новая страница добавлена! Всего страниц: {len(pages)}")
            await state.update_data(pages=pages)
        else:
            await message.answer("❌ Ошибка при сохранении")

    # Вернуться к списку страниц
    await state.set_state(ContentEditorStates.managing_pages)
    fake_msg = message.model_copy(update={"text": f"📄 Управление страницами"})
    return await content_editor_manage_pages(fake_msg, state)

@router.message(ContentEditorStates.managing_pages, F.text == "⬆️ Переместить вверх")
async def content_editor_move_page_up(message: types.Message, state: FSMContext):
    """Переместить страницу вверх"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    pages = data.get('pages', [])
    page_index = data.get('selected_page_index', 0)

    if page_index == 0:
        await message.answer("❌ Это первая страница, нельзя переместить выше")
        return

    # Меняем местами с предыдущей страницей
    pages[page_index], pages[page_index - 1] = pages[page_index - 1], pages[page_index]

    # Сохраняем в БД
    db_content = await get_button_content(button_label)

    if db_content:
        pages_json = json.dumps(pages)

        success = await update_button_content(
            button_label,
            pages[0]['text'],
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            db_content.get('buttons_per_row'),
            pages_json
        )

        if success:
            await message.answer(f"✅ Страница перемещена вверх")
            await state.update_data(pages=pages, selected_page_index=page_index - 1)
        else:
            await message.answer("❌ Ошибка при сохранении")

    # Вернуться к списку страниц
    await state.set_state(ContentEditorStates.managing_pages)
    fake_msg = message.model_copy(update={"text": f"📄 Управление страницами"})
    return await content_editor_manage_pages(fake_msg, state)

@router.message(ContentEditorStates.managing_pages, F.text == "⬇️ Переместить вниз")
async def content_editor_move_page_down(message: types.Message, state: FSMContext):
    """Переместить страницу вниз"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    pages = data.get('pages', [])
    page_index = data.get('selected_page_index', 0)

    if page_index == len(pages) - 1:
        await message.answer("❌ Это последняя страница, нельзя переместить ниже")
        return

    # Меняем местами со следующей страницей
    pages[page_index], pages[page_index + 1] = pages[page_index + 1], pages[page_index]

    # Сохраняем в БД
    db_content = await get_button_content(button_label)

    if db_content:
        pages_json = json.dumps(pages)

        success = await update_button_content(
            button_label,
            pages[0]['text'],
            db_content.get('photo_file_id'),
            db_content.get('buttons_json'),
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id'),
            db_content.get('buttons_per_row'),
            pages_json
        )

        if success:
            await message.answer(f"✅ Страница перемещена вниз")
            await state.update_data(pages=pages, selected_page_index=page_index + 1)
        else:
            await message.answer("❌ Ошибка при сохранении")

    # Вернуться к списку страниц
    await state.set_state(ContentEditorStates.managing_pages)
    fake_msg = message.model_copy(update={"text": f"📄 Управление страницами"})
    return await content_editor_manage_pages(fake_msg, state)

@router.message(ContentEditorStates.managing_pages, F.text == "⬅️ Назад")
async def content_editor_pages_back(message: types.Message, state: FSMContext):
    """Вернуться из управления страницами"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')

    await state.set_state(ContentEditorStates.selecting_menu)
    fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
    return await content_editor_select(fake_msg, state)

@router.message(ContentEditorStates.selecting_menu, F.text == "➕ Добавить инлайн-кнопку")
async def content_editor_add_inline_button_start(message: types.Message, state: FSMContext):
    """Начало добавления новой инлайн-кнопки"""
    kb = [
        [KeyboardButton(text="🔗 Кнопка-ссылка (URL)")],
        [KeyboardButton(text="📄 Кнопка-меню (submenu)")],
        [KeyboardButton(text="⬅️ Отмена")]
    ]

    await state.set_state(ContentEditorStates.adding_inline_button)
    await message.answer(
        "➕ <b>Добавление инлайн-кнопки</b>\n\n"
        "Выберите тип кнопки:\n"
        "• 🔗 <b>Кнопка-ссылка</b> - открывает URL\n"
        "• 📄 <b>Кнопка-меню</b> - открывает текст с новыми кнопками",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.adding_inline_button, F.text == "🔗 Кнопка-ссылка (URL)")
async def content_editor_add_url_button(message: types.Message, state: FSMContext):
    """Добавление кнопки-ссылки"""
    await state.update_data(button_type='url')
    await state.set_state(ContentEditorStates.waiting_button_text)
    await message.answer(
        "Введите текст для кнопки:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        )
    )

@router.message(ContentEditorStates.adding_inline_button, F.text == "📄 Кнопка-меню (submenu)")
async def content_editor_add_menu_button(message: types.Message, state: FSMContext):
    """Добавление кнопки-меню"""
    await state.update_data(button_type='menu')
    await state.set_state(ContentEditorStates.waiting_button_text)
    await message.answer(
        "Введите текст для кнопки:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        )
    )

@router.message(ContentEditorStates.waiting_button_text)
async def content_editor_button_text_received(message: types.Message, state: FSMContext):
    """Получен текст кнопки"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    await state.update_data(button_text=message.text)
    data = await state.get_data()
    button_type = data.get('button_type')

    if button_type == 'url':
        await state.set_state(ContentEditorStates.waiting_button_url)
        await message.answer(
            f"✏️ Текст кнопки: <b>{message.text}</b>\n\n"
            f"Теперь введите URL (ссылку):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True
            ),
            parse_mode=ParseMode.HTML
        )
    else:  # menu
        # Для кнопки-меню спрашиваем текст содержимого
        await state.set_state(ContentEditorStates.waiting_submenu_content)
        await message.answer(
            f"✏️ <b>Создание кнопки подменю: {message.text}</b>\n\n"
            f"Введите текст для этой кнопки.\n\n"
            f"Поддерживается HTML форматирование:\n"
            f"• <code>&lt;b&gt;жирный&lt;/b&gt;</code> → <b>жирный</b>\n"
            f"• <code>&lt;i&gt;курсив&lt;/i&gt;</code> → <i>курсив</i>\n"
            f"• <code>&lt;a href='URL'&gt;текст&lt;/a&gt;</code> → ссылка\n"
            f"• <code>&lt;code&gt;код&lt;/code&gt;</code> → <code>код</code>",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True
            ),
            parse_mode=ParseMode.HTML
        )

@router.message(ContentEditorStates.waiting_submenu_content)
async def content_editor_submenu_content_received(message: types.Message, state: FSMContext):
    """Получен текст содержимого для нового подменю"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    # Получаем данные
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    button_text = data.get('button_text')
    submenu_content = message.text

    # Создаем ID для нового подменю
    submenu_id = f"{button_label}:{button_text}"

    # Получаем контент родительского меню из БД
    db_content = await get_button_content(button_label)

    if db_content:
        # Получаем текущие кнопки из БД
        try:
            buttons = json.loads(db_content['buttons_json']) if db_content.get('buttons_json') else []
        except:
            buttons = []

        # Сначала спрашиваем о ширине кнопки перед добавлением
        await state.update_data(
            submenu_id=submenu_id,
            submenu_content=submenu_content,
            adding_new_button=True,
            button_type='menu'
        )
        await state.set_state(ContentEditorStates.waiting_button_width)

        kb = [
            [KeyboardButton(text="1️⃣ На весь ряд (большая)")],
            [KeyboardButton(text="2️⃣ По 2 в ряду")],
            [KeyboardButton(text="3️⃣ По 3 в ряду")],
            [KeyboardButton(text="4️⃣ По 4 в ряду")],
            [KeyboardButton(text="⬅️ Отмена")]
        ]

        await message.answer(
            f"⚙️ <b>Ширина кнопки '{button_text}'</b>\n\n"
            f"Выберите сколько таких кнопок помещается в один ряд:\n"
            f"• <b>1</b> - кнопка на весь ряд (большая)\n"
            f"• <b>2</b> - по 2 кнопки в ряду (половина)\n"
            f"• <b>3</b> - по 3 кнопки в ряду (треть)\n"
            f"• <b>4</b> - по 4 кнопки в ряду (маленькая)",
            reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
            parse_mode=ParseMode.HTML
        )
        return
    else:
        # Если контента нет в БД (статическое меню), создаем новый контент
        has_static_menu = data.get('has_static_menu', False)

        if has_static_menu:
            # Берем текст и ВСЕ статические кнопки из статического меню
            static_menu_info = find_static_menu_by_label(button_label)
            if static_menu_info:
                static_menu_data = static_menu_info['menu_data']
                if 'pages' in static_menu_data and static_menu_data['pages']:
                    text_content = static_menu_data['pages'][0].get('text', '')
                else:
                    text_content = static_menu_data.get('text', '')

                # Копируем ВСЕ статические кнопки
                buttons = []
                if static_menu_data.get('type') == 'inline' and static_menu_data.get('submenu'):
                    for submenu_key, submenu_data in static_menu_data['submenu'].items():
                        buttons.append({
                            'text': submenu_data.get('label', submenu_key),
                            'id': submenu_key
                        })
                if 'buttons' in static_menu_data:
                    for btn in static_menu_data['buttons']:
                        if btn.get('url'):
                            buttons.append({
                                'text': btn['text'],
                                'url': btn['url']
                            })
            else:
                text_content = ''
                buttons = []
        else:
            text_content = ''
            buttons = []

        # ДОБАВЛЯЕМ новую кнопку к существующим
        buttons.append({
            'text': button_text,
            'id': submenu_id
        })

        # Сохраняем в БД
        success = await update_button_content(
            button_label,
            text_content,
            None,  # photo_file_id
            json.dumps(buttons),
            'HTML',
            None  # parent_id
        )

        if not success:
            await message.answer("❌ Ошибка при создании контента в БД")
            await state.clear()
            return await admin_button(message, state)

    # Создаем контент для подменю с пользовательским текстом
    await update_button_content(
        submenu_id,
        submenu_content,  # Используем текст от пользователя
        None,
        None,
        'HTML',
        button_label  # parent_id
    )

    await message.answer(f"✅ Кнопка-меню '{button_text}' добавлена!")

    # Сразу открываем редактор для этой кнопки
    await state.set_state(ContentEditorStates.selecting_menu)
    await state.update_data(editing_button_label=submenu_id)
    fake_msg = message.model_copy(update={"text": f"📝 {submenu_id}"})
    return await content_editor_select(fake_msg, state)

@router.message(ContentEditorStates.waiting_button_url)
async def content_editor_button_url_received(message: types.Message, state: FSMContext):
    """Получен URL кнопки (добавление новой или изменение существующей)"""
    if message.text == "⬅️ Отмена":
        data = await state.get_data()
        selected_button = data.get('selected_inline_button')

        if selected_button:
            # Возврат к управлению кнопкой
            await state.set_state(ContentEditorStates.managing_inline_buttons)
            button_label = data.get('editing_button_label')
            fake_msg = message.model_copy(update={"text": f"🔘 🔗 {selected_button['text']}"})
            return await content_editor_manage_inline_button(fake_msg, state)
        else:
            await state.set_state(ContentEditorStates.selecting_menu)
            return await content_editor_start(message, state)

    data = await state.get_data()
    button_label = data.get('editing_button_label')
    selected_button = data.get('selected_inline_button')
    button_url = message.text

    # Добавляем https:// если не указано
    if not button_url.startswith('http'):
        button_url = f'https://{button_url}'

    # Получаем контент из БД
    db_content = await get_button_content(button_label)

    if db_content:
        # Получаем текущие кнопки
        try:
            buttons = json.loads(db_content['buttons_json']) if db_content.get('buttons_json') else []
        except:
            buttons = []

        if selected_button:
            # Изменяем URL существующей кнопки
            button_found = False
            for btn in buttons:
                if btn.get('text') == selected_button['text'] and btn.get('url'):
                    btn['url'] = button_url
                    button_found = True
                    break

            if button_found:
                # Сохраняем
                success = await update_button_content(
                    button_label,
                    db_content.get('content'),
                    db_content.get('photo_file_id'),
                    json.dumps(buttons),
                    db_content.get('parse_mode', 'HTML'),
                    db_content.get('parent_id')
                )

                if success:
                    await message.answer(f"✅ URL кнопки '{selected_button['text']}' изменен!")
                    await state.set_state(ContentEditorStates.selecting_menu)
                    fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
                    return await content_editor_select(fake_msg, state)
                else:
                    await message.answer("❌ Ошибка при изменении URL")
            else:
                # Если не нашли кнопку в БД, возможно это статическая кнопка - добавим её с новым URL
                if selected_button and selected_button.get('source') == 'static':
                    buttons.append({
                        'text': selected_button['text'],
                        'url': button_url
                    })

                    success = await update_button_content(
                        button_label,
                        db_content.get('content'),
                        db_content.get('photo_file_id'),
                        json.dumps(buttons),
                        db_content.get('parse_mode', 'HTML'),
                        db_content.get('parent_id')
                    )

                    if success:
                        await message.answer(f"✅ URL кнопки '{selected_button['text']}' изменен!")
                        await state.set_state(ContentEditorStates.selecting_menu)
                        fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
                        return await content_editor_select(fake_msg, state)
                    else:
                        await message.answer("❌ Ошибка при изменении URL")
                else:
                    await message.answer("❌ Кнопка не найдена")
        else:
            # Добавляем новую кнопку - сначала спрашиваем о ширине
            button_text = data.get('button_text')
            await state.update_data(button_url=button_url, adding_new_button=True)
            await state.set_state(ContentEditorStates.waiting_button_width)

            kb = [
                [KeyboardButton(text="1️⃣ На весь ряд (большая)")],
                [KeyboardButton(text="2️⃣ По 2 в ряду")],
                [KeyboardButton(text="3️⃣ По 3 в ряду")],
                [KeyboardButton(text="4️⃣ По 4 в ряду")],
                [KeyboardButton(text="⬅️ Отмена")]
            ]

            await message.answer(
                f"⚙️ <b>Ширина кнопки '{button_text}'</b>\n\n"
                f"Выберите сколько таких кнопок помещается в один ряд:\n"
                f"• <b>1</b> - кнопка на весь ряд (большая)\n"
                f"• <b>2</b> - по 2 кнопки в ряду (половина)\n"
                f"• <b>3</b> - по 3 кнопки в ряду (треть)\n"
                f"• <b>4</b> - по 4 кнопки в ряду (маленькая)",
                reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
                parse_mode=ParseMode.HTML
            )
            return
    else:
        # Если контента нет в БД (статическое меню), создаем новый контент
        has_static_menu = data.get('has_static_menu', False)

        if has_static_menu:
            # Берем текст и кнопки из статического меню
            static_menu_info = find_static_menu_by_label(button_label)
            if static_menu_info:
                static_menu_data = static_menu_info['menu_data']
                if 'pages' in static_menu_data and static_menu_data['pages']:
                    text_content = static_menu_data['pages'][0].get('text', '')
                else:
                    text_content = static_menu_data.get('text', '')

                # Получаем статические кнопки
                buttons = []
                if static_menu_data.get('type') == 'inline' and static_menu_data.get('submenu'):
                    for submenu_id, submenu_data in static_menu_data['submenu'].items():
                        buttons.append({
                            'text': submenu_data.get('label', submenu_id),
                            'id': submenu_id
                        })
                if 'buttons' in static_menu_data:
                    for btn in static_menu_data['buttons']:
                        if btn.get('url'):
                            buttons.append({
                                'text': btn['text'],
                                'url': btn['url']
                            })
            else:
                text_content = ''
                buttons = []
        else:
            text_content = ''
            buttons = []

        if selected_button:
            # Изменяем URL существующей статической кнопки
            button_found = False
            for btn in buttons:
                if btn.get('text') == selected_button['text']:
                    btn['url'] = button_url
                    button_found = True
                    break

            if not button_found:
                # Если не нашли, добавляем новую
                buttons.append({
                    'text': selected_button['text'],
                    'url': button_url
                })
        else:
            # Добавляем новую кнопку
            button_text = data.get('button_text')
            buttons.append({
                'text': button_text,
                'url': button_url
            })

        # Сохраняем в БД
        success = await update_button_content(
            button_label,
            text_content,
            None,  # photo_file_id
            json.dumps(buttons),
            'HTML',
            None  # parent_id
        )

        if success:
            if selected_button:
                await message.answer(f"✅ URL кнопки '{selected_button['text']}' изменен!")
                await state.set_state(ContentEditorStates.selecting_menu)
                fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
                return await content_editor_select(fake_msg, state)
            else:
                await message.answer(f"✅ Кнопка-ссылка добавлена!")
        else:
            await message.answer("❌ Ошибка при создании контента в БД")

    await state.clear()
    await admin_button(message, state)

@router.message(ContentEditorStates.waiting_button_width)
async def content_editor_button_width_received(message: types.Message, state: FSMContext):
    """Получен выбор ширины кнопки"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    # Определяем row_width из выбора пользователя
    width_map = {
        "1️⃣ На весь ряд (большая)": 1,
        "2️⃣ По 2 в ряду": 2,
        "3️⃣ По 3 в ряду": 3,
        "4️⃣ По 4 в ряду": 4
    }

    row_width = width_map.get(message.text)
    if not row_width:
        await message.answer("❌ Неверный выбор")
        return

    # Получаем данные из state
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    editing_existing = data.get('editing_button_width', False)

    # Если изменяем существующую кнопку
    if editing_existing:
        selected_button = data.get('selected_inline_button')
        if not selected_button:
            await message.answer("❌ Кнопка не найдена")
            return

        # Получаем контент из БД
        db_content = await get_button_content(button_label)

        if db_content and db_content.get('buttons_json'):
            try:
                buttons = json.loads(db_content['buttons_json'])

                # Находим кнопку и меняем её row_width
                button_found = False
                for btn in buttons:
                    if btn.get('text') == selected_button['text']:
                        btn['row_width'] = row_width
                        button_found = True
                        break

                if button_found:
                    # Сохраняем
                    success = await update_button_content(
                        button_label,
                        db_content.get('content'),
                        db_content.get('photo_file_id'),
                        json.dumps(buttons),
                        db_content.get('parse_mode', 'HTML'),
                        db_content.get('parent_id')
                    )

                    if success:
                        width_text = {1: "на весь ряд", 2: "по 2 в ряду", 3: "по 3 в ряду", 4: "по 4 в ряду"}
                        await message.answer(f"✅ Ширина кнопки '{selected_button['text']}' изменена на '{width_text[row_width]}'!")
                        await state.set_state(ContentEditorStates.selecting_menu)
                        fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
                        return await content_editor_select(fake_msg, state)
                    else:
                        await message.answer("❌ Ошибка при сохранении")
                        return
                else:
                    await message.answer("❌ Кнопка не найдена в БД")
                    return
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")
                return
        else:
            await message.answer("❌ Контент не найден в БД")
            return

    # Иначе добавляем новую кнопку
    button_text = data.get('button_text')
    button_type = data.get('button_type', 'url')

    # Получаем контент из БД
    db_content = await get_button_content(button_label)

    if db_content:
        # Получаем текущие кнопки
        try:
            buttons = json.loads(db_content['buttons_json']) if db_content.get('buttons_json') else []
        except:
            buttons = []

        # Создаем новую кнопку с row_width
        if button_type == 'url':
            button_url = data.get('button_url')
            new_button = {
                'text': button_text,
                'url': button_url,
                'row_width': row_width
            }
            buttons.append(new_button)
            print(f"[DEBUG] Добавляем URL кнопку: {new_button}")
        else:  # menu
            submenu_id = data.get('submenu_id')
            submenu_content = data.get('submenu_content')
            new_button = {
                'text': button_text,
                'id': submenu_id,
                'row_width': row_width
            }
            buttons.append(new_button)
            print(f"[DEBUG] Добавляем меню кнопку: {new_button}")

            # Создаем контент для подменю
            await update_button_content(
                submenu_id,
                submenu_content,
                None,  # photo_file_id
                None,  # buttons_json
                'HTML',
                button_label  # parent_id
            )

        # Сохраняем обновленные кнопки родительского меню
        buttons_json = json.dumps(buttons)
        print(f"[DEBUG] Сохраняем buttons_json: {buttons_json}")

        success = await update_button_content(
            button_label,
            db_content.get('content'),
            db_content.get('photo_file_id'),
            buttons_json,
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id')
        )

        if success:
            width_text = {1: "на весь ряд", 2: "по 2 в ряду", 3: "по 3 в ряду", 4: "по 4 в ряду"}
            await message.answer(f"✅ Кнопка '{button_text}' добавлена ({width_text[row_width]})!")
            await state.set_state(ContentEditorStates.selecting_menu)
            fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
            return await content_editor_select(fake_msg, state)
        else:
            await message.answer("❌ Ошибка при добавлении кнопки")
    else:
        # Если контента нет в БД (статическое меню), создаем новый контент
        has_static_menu = data.get('has_static_menu', False)

        if has_static_menu:
            # Берем текст из статического меню и создаем с новой кнопкой
            static_menu_info = find_static_menu_by_label(button_label)
            if static_menu_info:
                static_menu_data = static_menu_info['menu_data']
                if 'pages' in static_menu_data and static_menu_data['pages']:
                    text_content = static_menu_data['pages'][0].get('text', '')
                else:
                    text_content = static_menu_data.get('text', '')

                # Копируем статические кнопки
                buttons = []
                if static_menu_data.get('type') == 'inline' and static_menu_data.get('submenu'):
                    for submenu_key, submenu_data in static_menu_data['submenu'].items():
                        buttons.append({
                            'text': submenu_data.get('label', submenu_key),
                            'id': submenu_key
                        })

                # Добавляем новую кнопку с row_width
                if button_type == 'url':
                    button_url = data.get('button_url')
                    buttons.append({
                        'text': button_text,
                        'url': button_url,
                        'row_width': row_width
                    })
                else:  # menu
                    submenu_id = data.get('submenu_id')
                    submenu_content = data.get('submenu_content')
                    buttons.append({
                        'text': button_text,
                        'id': submenu_id,
                        'row_width': row_width
                    })

                    # Создаем контент для подменю
                    await update_button_content(
                        submenu_id,
                        submenu_content,
                        None,
                        None,
                        'HTML',
                        button_label
                    )

                # Сохраняем в БД
                success = await update_button_content(
                    button_label,
                    text_content,
                    None,
                    json.dumps(buttons),
                    'HTML',
                    None
                )

                if success:
                    width_text = {1: "на весь ряд", 2: "по 2 в ряду", 3: "по 3 в ряду", 4: "по 4 в ряду"}
                    await message.answer(f"✅ Кнопка '{button_text}' добавлена ({width_text[row_width]})!")
                    await state.set_state(ContentEditorStates.selecting_menu)
                    fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
                    return await content_editor_select(fake_msg, state)
                else:
                    await message.answer("❌ Ошибка при сохранении")

    await state.clear()
    await admin_button(message, state)

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "⬅️ Назад")
async def content_editor_back_from_button_management(message: types.Message, state: FSMContext):
    """Возврат к редактированию кнопки"""
    await state.set_state(ContentEditorStates.selecting_menu)
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
    return await content_editor_select(fake_msg, state)

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "🗑 Удалить")
async def content_editor_delete_inline_button(message: types.Message, state: FSMContext):
    """Удаление инлайн-кнопки"""
    data = await state.get_data()
    button_label = data.get('editing_button_label')
    selected_button = data.get('selected_inline_button')

    if not selected_button:
        await message.answer("❌ Кнопка не выбрана")
        return

    # Удаляем кнопку
    # Для статических кнопок создаем override в БД
    success = await delete_inline_button(button_label, selected_button)

    if success:
        await message.answer(f"✅ Кнопка '{selected_button['text']}' удалена!")
        await state.set_state(ContentEditorStates.selecting_menu)
        fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
        return await content_editor_select(fake_msg, state)
    else:
        await message.answer("❌ Ошибка при удалении кнопки")

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "✏️ Переименовать")
async def content_editor_rename_inline_button_start(message: types.Message, state: FSMContext):
    """Начало переименования инлайн-кнопки"""
    data = await state.get_data()
    selected_button = data.get('selected_inline_button')

    if not selected_button:
        await message.answer("❌ Кнопка не выбрана")
        return

    # Можно переименовать любую кнопку (изменения сохраняются в БД)
    await state.set_state(ContentEditorStates.editing_inline_button_name)
    await message.answer(
        f"✏️ <b>Переименование кнопки</b>\n\n"
        f"Текущее название: <b>{selected_button['text']}</b>\n\n"
        f"Введите новое название:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.editing_inline_button_name)
async def content_editor_rename_inline_button_save(message: types.Message, state: FSMContext):
    """Сохранение нового названия инлайн-кнопки"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        data = await state.get_data()
        button_label = data.get('editing_button_label')
        fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
        return await content_editor_select(fake_msg, state)

    new_name = message.text.strip()
    if not new_name:
        await message.answer("❌ Название не может быть пустым")
        return

    data = await state.get_data()
    button_label = data.get('editing_button_label')
    selected_button = data.get('selected_inline_button')

    # Переименовываем кнопку
    success = await rename_inline_button(button_label, selected_button, new_name)

    if success:
        await message.answer(f"✅ Кнопка переименована: '{selected_button['text']}' → '{new_name}'")
        await state.set_state(ContentEditorStates.selecting_menu)
        fake_msg = message.model_copy(update={"text": f"📝 {button_label}"})
        return await content_editor_select(fake_msg, state)
    else:
        await message.answer("❌ Ошибка при переименовании кнопки")

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "⚙️ Изменить ширину")
async def content_editor_change_button_width_start(message: types.Message, state: FSMContext):
    """Начало изменения ширины инлайн-кнопки"""
    data = await state.get_data()
    selected_button = data.get('selected_inline_button')
    button_label = data.get('editing_button_label')

    if not selected_button:
        await message.answer("❌ Кнопка не выбрана")
        return

    # Получаем текущую ширину кнопки
    db_content = await get_button_content(button_label)
    current_width = 1  # Дефолтное значение

    if db_content and db_content.get('buttons_json'):
        try:
            buttons = json.loads(db_content['buttons_json'])
            for btn in buttons:
                if btn.get('text') == selected_button['text']:
                    current_width = btn.get('row_width', 1)
                    break
        except:
            pass

    # Сохраняем в state что это изменение существующей кнопки
    await state.update_data(editing_button_width=True)

    kb = [
        [KeyboardButton(text="1️⃣ На весь ряд (большая)")],
        [KeyboardButton(text="2️⃣ По 2 в ряду")],
        [KeyboardButton(text="3️⃣ По 3 в ряду")],
        [KeyboardButton(text="4️⃣ По 4 в ряду")],
        [KeyboardButton(text="⬅️ Отмена")]
    ]

    width_text = {1: "на весь ряд", 2: "по 2 в ряду", 3: "по 3 в ряду", 4: "по 4 в ряду"}
    await state.set_state(ContentEditorStates.waiting_button_width)
    await message.answer(
        f"⚙️ <b>Ширина кнопки '{selected_button['text']}'</b>\n\n"
        f"Текущая ширина: <b>{width_text.get(current_width, 'не задана')}</b>\n\n"
        f"Выберите новую ширину:\n"
        f"• <b>1</b> - кнопка на весь ряд (большая)\n"
        f"• <b>2</b> - по 2 кнопки в ряду (половина)\n"
        f"• <b>3</b> - по 3 кнопки в ряду (треть)\n"
        f"• <b>4</b> - по 4 кнопки в ряду (маленькая)",
        reply_markup=ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "✏️ Изменить URL")
async def content_editor_change_url_start(message: types.Message, state: FSMContext):
    """Начало изменения URL инлайн-кнопки"""
    data = await state.get_data()
    selected_button = data.get('selected_inline_button')

    if not selected_button or selected_button['type'] != '🔗 URL':
        await message.answer("❌ Это не URL кнопка")
        return

    # Можно изменить URL любой кнопки (изменения сохраняются в БД)
    await state.set_state(ContentEditorStates.waiting_button_url)
    await message.answer(
        f"🔗 <b>Изменение URL</b>\n\n"
        f"Кнопка: <b>{selected_button['text']}</b>\n"
        f"Текущий URL: <code>{selected_button.get('url', 'N/A')}</code>\n\n"
        f"Введите новый URL:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "📝 Изменить текст внутри")
async def content_editor_edit_submenu_text(message: types.Message, state: FSMContext):
    """Редактирование текста внутри инлайн-кнопки подменю"""
    data = await state.get_data()
    selected_button = data.get('selected_inline_button')

    if not selected_button or selected_button['type'] != '📄 меню':
        await message.answer("❌ Это не кнопка подменю")
        return

    # Получаем ID подменю
    submenu_id = selected_button.get('id')
    if not submenu_id:
        await message.answer("❌ ID подменю не найден")
        return

    # Сохраняем контекст
    await state.update_data(editing_submenu_id=submenu_id)
    await state.set_state(ContentEditorStates.editing_text)

    # Получаем текущий текст
    db_content = await get_button_content(submenu_id)
    if db_content:
        current_text = db_content.get('content', 'Нет текста')
    else:
        # Пробуем найти в статике
        static_menu_info = find_static_menu_by_label(selected_button['text'])
        if static_menu_info:
            static_menu_data = static_menu_info['menu_data']
            if 'pages' in static_menu_data and static_menu_data['pages']:
                current_text = static_menu_data['pages'][0].get('text', 'Нет текста')
            else:
                current_text = static_menu_data.get('text', 'Нет текста')
        else:
            current_text = 'Нет текста'

    await message.answer(
        f"✏️ <b>Редактирование текста внутри: {selected_button['text']}</b>\n\n"
        f"<b>Текущий текст:</b>\n{current_text[:200]}...\n\n"
        f"Введите новый текст. Поддерживается HTML форматирование:\n"
        f"• <code>&lt;b&gt;жирный&lt;/b&gt;</code> → <b>жирный</b>\n"
        f"• <code>&lt;i&gt;курсив&lt;/i&gt;</code> → <i>курсив</i>\n"
        f"• <code>&lt;a href='URL'&gt;текст&lt;/a&gt;</code> → ссылка\n"
        f"• <code>&lt;code&gt;код&lt;/code&gt;</code> → <code>код</code>",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode=ParseMode.HTML
    )

@router.message(ContentEditorStates.managing_inline_buttons, F.text == "📂 Открыть подменю")
async def content_editor_open_submenu(message: types.Message, state: FSMContext):
    """Открытие подменю для редактирования - показывает инлайн-кнопки внутри"""
    data = await state.get_data()
    selected_button = data.get('selected_inline_button')

    if not selected_button or selected_button['type'] != '📄 меню':
        await message.answer("❌ Это не кнопка подменю")
        return

    # Переходим к редактированию подменю - показываем ЕГО инлайн-кнопки
    await state.set_state(ContentEditorStates.selecting_menu)

    # Если это статическая кнопка
    if selected_button.get('source') == 'static':
        # Извлекаем menu_path для статических кнопок
        menu_path = selected_button.get('menu_path', '')

        # Разбираем путь (например: "garant_checker:info")
        path_parts = menu_path.split(':')
        if len(path_parts) >= 2:
            parent_menu_id = path_parts[0]
            submenu_id = path_parts[1]

            # Находим подменю в MENU_STRUCTURE
            if parent_menu_id in MENU_STRUCTURE:
                parent_menu = MENU_STRUCTURE[parent_menu_id]
                if 'submenu' in parent_menu and submenu_id in parent_menu['submenu']:
                    submenu_data = parent_menu['submenu'][submenu_id]
                    submenu_label = submenu_data.get('label', submenu_id)

                    # Открываем подменю по его label
                    fake_msg = message.model_copy(update={"text": f"📝 {submenu_label}"})
                    return await content_editor_select(fake_msg, state)

        await message.answer("❌ Статическое подменю не найдено")
        return

    # Если это кнопка из БД
    submenu_id = selected_button.get('id')
    if not submenu_id:
        await message.answer("❌ ID подменю не найден")
        return

    await state.update_data(editing_button_label=submenu_id)

    # Показываем редактор для подменю
    # content_editor_select сам разберется: есть в БД или статическое
    fake_msg = message.model_copy(update={"text": f"📝 {submenu_id}"})
    await content_editor_select(fake_msg, state)

@router.message(ContentEditorStates.editing_inline_buttons)
async def content_editor_save_inline_buttons(message: types.Message, state: FSMContext):
    """Сохранение инлайн-кнопок"""
    if message.text == "⬅️ Отмена":
        await state.set_state(ContentEditorStates.selecting_menu)
        return await content_editor_start(message, state)

    data = await state.get_data()
    menu_id = data.get('editing_button_label')

    if not menu_id.startswith('db:'):
        await message.answer("⚠️ Инлайн-кнопки можно редактировать только для кнопок из БД")
        await state.clear()
        return await admin_button(message, state)

    button_label = menu_id[3:]
    db_content = await get_button_content(button_label)

    if not db_content:
        await message.answer("❌ Контент не найден в БД")
        await state.clear()
        return await admin_button(message, state)

    # Обработка удаления всех кнопок
    if message.text.lower() == "удалить все":
        success = await update_button_content(
            button_label,
            db_content.get('content'),
            db_content.get('photo_file_id'),
            None,  # Удаляем все кнопки
            db_content.get('parse_mode', 'HTML'),
            db_content.get('parent_id')
        )
        if success:
            await message.answer("✅ Все инлайн-кнопки удалены!")
        else:
            await message.answer("❌ Ошибка при удалении")
        await state.clear()
        return await admin_button(message, state)

    # Парсинг новых кнопок
    new_buttons = []
    lines = message.text.strip().split('\n')

    for line in lines:
        if '|' in line:
            parts = line.split('|', 1)
            text = parts[0].strip()
            url = parts[1].strip()

            if text and url:
                new_buttons.append({
                    'text': text,
                    'url': url if url.startswith('http') else f'https://{url}'
                })

    if not new_buttons:
        await message.answer("❌ Не удалось распознать кнопки. Используйте формат: Текст | URL")
        return

    # Сохраняем
    success = await update_button_content(
        button_label,
        db_content.get('content'),
        db_content.get('photo_file_id'),
        json.dumps(new_buttons),
        db_content.get('parse_mode', 'HTML'),
        db_content.get('parent_id')
    )

    if success:
        await message.answer(f"✅ Сохранено {len(new_buttons)} инлайн-кнопок!")
    else:
        await message.answer("❌ Ошибка при сохранении")

    await state.clear()
    await admin_button(message, state)

@router.message(ContentEditorStates.selecting_menu, F.text == "⬅️ Назад")
async def content_editor_back(message: types.Message, state: FSMContext):
    """Возврат в админ-панель"""
    await state.clear()
    await admin_button(message, state)

# ============ КОНЕЦ НОВОГО РЕДАКТОРА ============

@router.message(F.text == "📊 Статистика")
async def show_statistics(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    stats = await get_stats()
    text = f"📊 <b>Статистика бота</b>\n\n"
    text += f"👥 Всего пользователей: <code>{stats['user_count']}</code>\n\n"

    # Keyboard buttons stats
    text += f"⌨️ <b>Топ нажатий на кнопки меню:</b>\n"
    # После миграции все кнопки в БД, получаем их оттуда
    keyboard_buttons = await get_all_keyboard_buttons()
    keyboard_labels = [btn['label'] for btn in keyboard_buttons]

    # Также добавляем все button_id из button_content (для инлайн кнопок и подменю)
    # Просто берем все клики без фильтрации, т.к. все данные теперь в БД
    kb_clicks = stats['clicks']

    if kb_clicks:
        for i, row in enumerate(kb_clicks, 1):
            text += f"{i}. {row['button_name']}: <code>{row['click_count']}</code>\n"
    else:
        text += "Данных пока нет.\n"

    await message.answer(text, parse_mode=ParseMode.HTML)


@router.message(F.text == "📢 Рассылка")
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас нет доступа.")
        return

    await state.set_state(BroadcastStates.waiting_for_text)
    await message.answer(
        "Введите текст рассылки (поддерживается HTML форматирование: <b>жирный</b>, <i>наклонный</i>, <u>подчеркнутый</u>):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True))


@router.message(F.text == "⬅️ Отмена")
async def cancel_broadcast(message: types.Message, state: FSMContext):
    await state.clear()
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📢 Рассылка")],
                  [KeyboardButton(text="📊 Статистика")],
                  [KeyboardButton(text="📝 Редактировать кнопки")],
                  [KeyboardButton(text="🔙 Выйти")]],
        resize_keyboard=True)
    await message.answer("Рассылка отменена.", reply_markup=keyboard)


@router.message(BroadcastStates.waiting_for_text)
async def process_broadcast_text(message: types.Message, state: FSMContext):
    await state.update_data(text_content=message.text)
    await state.set_state(BroadcastStates.waiting_for_photo)
    await message.answer(
        "Отправьте фото для рассылки или напишите 'пропустить' чтобы продолжить без фото:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="пропустить")],
                      [KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True))


@router.message(BroadcastStates.waiting_for_photo)
async def process_broadcast_photo(message: types.Message, state: FSMContext):
    if message.text == "пропустить":
        await state.update_data(photo_file_id=None)
        await state.set_state(BroadcastStates.waiting_for_buttons_menu)
        await message.answer(
            "Хотите добавить кнопки?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Добавить кнопки")],
                          [KeyboardButton(text="❌ Без кнопок")],
                          [KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True))
    elif message.photo:
        photo_file_id = message.photo[-1].file_id
        await state.update_data(photo_file_id=photo_file_id)
        await state.set_state(BroadcastStates.waiting_for_buttons_menu)
        await message.answer(
            "Хотите добавить кнопки?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Добавить кнопки")],
                          [KeyboardButton(text="❌ Без кнопок")],
                          [KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True))
    else:
        await message.answer("Отправьте фото или напишите 'пропустить'.")


@router.message(BroadcastStates.waiting_for_buttons_menu)
async def process_buttons_menu(message: types.Message, state: FSMContext):
    if message.text == "✅ Добавить кнопки":
        await state.set_state(BroadcastStates.waiting_for_button_name)
        await state.update_data(buttons=[])
        await message.answer("Введите название кнопки:",
                             reply_markup=ReplyKeyboardMarkup(
                                 keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
                                 resize_keyboard=True))
    elif message.text == "❌ Без кнопок":
        await proceed_to_confirm(message, state)
    else:
        await message.answer("Выберите один из вариантов.")


@router.message(BroadcastStates.waiting_for_button_name)
async def process_button_name(message: types.Message, state: FSMContext):
    await state.update_data(button_name=message.text)
    await state.set_state(BroadcastStates.waiting_for_button_url)
    await message.answer(
        "Введите ссылку для кнопки (например: https://t.me/...):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True))


@router.message(BroadcastStates.waiting_for_button_url)
async def process_button_url(message: types.Message, state: FSMContext):
    data = await state.get_data()
    button_name = data.get('button_name', '')
    buttons = data.get('buttons', [])

    buttons.append({'text': button_name, 'url': message.text})

    await state.update_data(buttons=buttons)
    await state.set_state(BroadcastStates.waiting_for_buttons_menu)

    buttons_list = "\n".join(
        [f"• {btn['text']}: {btn['url']}" for btn in buttons])

    await message.answer(
        f"✅ Кнопка добавлена!\n\nДобавленные кнопки:\n{buttons_list}\n\nХотите добавить ещё одну?",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="➕ Добавить ещё")],
                      [KeyboardButton(text="✅ Готово")],
                      [KeyboardButton(text="⬅️ Отмена")]],
            resize_keyboard=True))


async def proceed_to_confirm(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await state.set_state(BroadcastStates.confirm_broadcast)

    preview_text = data['text_content']
    if data.get('photo_file_id'):
        preview_text += "\n\n📸 (Фото будет отправлено)"

    buttons = data.get('buttons', [])
    if buttons:
        buttons_list = "\n".join(
            [f"🔘 {btn['text']}: {btn['url']}" for btn in buttons])
        preview_text += f"\n\n<b>Кнопки:</b>\n{buttons_list}"

    confirm_keyboard = ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="✅ Отправить"),
        KeyboardButton(text="❌ Отмена")
    ]],
                                           resize_keyboard=True)

    await message.answer(f"<b>Предпросмотр рассылки:</b>\n\n{preview_text}",
                         reply_markup=confirm_keyboard,
                         parse_mode=ParseMode.HTML)


@router.message(F.text == "➕ Добавить ещё")
async def add_another_button(message: types.Message, state: FSMContext):
    await state.set_state(BroadcastStates.waiting_for_button_name)
    await message.answer("Введите название кнопки:",
                         reply_markup=ReplyKeyboardMarkup(
                             keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
                             resize_keyboard=True))


@router.message(F.text == "✅ Готово")
async def buttons_done(message: types.Message, state: FSMContext):
    await proceed_to_confirm(message, state)


@router.message(BroadcastStates.confirm_broadcast)
async def confirm_and_send_broadcast(message: types.Message,
                                     state: FSMContext):
    if message.text == "✅ Отправить":
        data = await state.get_data()
        users = await get_all_users()

        # Build inline keyboard from buttons
        buttons_keyboard = None
        buttons = data.get('buttons', [])
        if buttons:
            keyboard_buttons = []
            for btn in buttons:
                keyboard_buttons.append(
                    InlineKeyboardButton(text=btn['text'], url=btn['url']))
            buttons_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[keyboard_buttons])

        sent_count = 0
        tasks = []
        for user_id in users:
            try:
                if data.get('photo_file_id'):
                    tasks.append(bot.send_photo(user_id,
                                         data['photo_file_id'],
                                         caption=data['text_content'],
                                         parse_mode=ParseMode.HTML,
                                         reply_markup=buttons_keyboard))
                else:
                    tasks.append(bot.send_message(user_id,
                                           data['text_content'],
                                           parse_mode=ParseMode.HTML,
                                           reply_markup=buttons_keyboard))

                if len(tasks) >= 30:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for res in results:
                        if not isinstance(res, Exception):
                            sent_count += 1
                        else:
                            logger.error(f"Broadcast error: {res}")
                    tasks = []
                    await asyncio.sleep(1) # Respect Telegram rate limits (30 msg/sec)
            except Exception as e:
                logger.error(f"Error preparing for {user_id}: {e}")

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if not isinstance(res, Exception):
                    sent_count += 1
                else:
                    logger.error(f"Broadcast error: {res}")

        await state.clear()
        admin_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📢 Рассылка")],
                      [KeyboardButton(text="🔙 Выйти")]],
            resize_keyboard=True)
        await message.answer(
            f"✅ Рассылка отправлена {sent_count} пользователям!",
            reply_markup=admin_keyboard)
    else:
        await state.clear()
        admin_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📢 Рассылка")],
                      [KeyboardButton(text="🔙 Выйти")]],
            resize_keyboard=True)
        await message.answer("Рассылка отменена.", reply_markup=admin_keyboard)



    await state.set_state(ChatsContinuationStates.selecting_chat_section)
    keyboard = ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="Инфобизнес"),
        KeyboardButton(text="Общие [админ]")
    ], [KeyboardButton(text="Тематические [админ]")
        ], [KeyboardButton(text="⬅️ Отмена")]],
                                   resize_keyboard=True)
    await message.answer("Выберите раздел чатов для редактирования:",
                         reply_markup=keyboard)


@router.message(ChatsContinuationStates.selecting_chat_section)
async def select_chat_section(message: types.Message, state: FSMContext):
    section_map = {
        "Инфобизнес": "infobusiness",
        "Общие [админ]": "general_admin",
        "Тематические [админ]": "thematic_admin"
    }

    if message.text == "⬅️ Отмена":
        await state.clear()
        admin_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📢 Рассылка")],
                      [KeyboardButton(text="📊 Статистика")],
                      [KeyboardButton(text="🏗 Управление меню")],
                      [KeyboardButton(text="📝 Редактировать кнопки")],
                      [KeyboardButton(text="🔙 Выйти")]],
            resize_keyboard=True)
        await message.answer("Отменено.", reply_markup=admin_keyboard)
        return

    section_key = section_map.get(message.text)
    if not section_key:
        await message.answer("Выберите раздел из предложенных.")
        return

    lines = CHATS_CONTINUATION.get(section_key, [])
    lines_text = "\n".join([f"{i+1}. {line}" for i, line in enumerate(lines)])

    await state.update_data(section_key=section_key)
    await state.set_state(ChatsContinuationStates.managing_lines)

    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="➕ Добавить строку")],
                  [KeyboardButton(text="✏️ Редактировать")],
                  [KeyboardButton(text="⬅️ Назад")]],
        resize_keyboard=True)

    await message.answer(
        f"<b>Редактирование: {message.text}</b>\n\n<b>Текущие строки:</b>\n{lines_text}\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML)


@router.message(ChatsContinuationStates.managing_lines)
async def manage_chat_lines(message: types.Message, state: FSMContext):
    data = await state.get_data()
    section_key = data.get('section_key')

    if message.text == "⬅️ Назад":
        await state.clear()
        admin_keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📢 Рассылка")],
                      [KeyboardButton(text="📊 Статистика")],
                      [KeyboardButton(text="🏗 Управление меню")],
                      [KeyboardButton(text="📝 Редактировать кнопки")],
                      [KeyboardButton(text="🔙 Выйти")]],
            resize_keyboard=True)
        await message.answer("Вернулись в админ-панель.",
                             reply_markup=admin_keyboard)
        return

    if message.text == "➕ Добавить строку":
        await state.set_state(ChatsContinuationStates.editing_line)
        await state.update_data(editing_action="add", accumulated_text="")
        await message.answer(
            "📝 Введите текст (можно многострочный):\n\nВ Telegram используйте Shift+Enter для переноса строк.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Готово")],
                          [KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True))

    elif message.text == "✏️ Редактировать":
        lines = CHATS_CONTINUATION.get(section_key, [])
        if not lines:
            await message.answer("Нет строк для редактирования.")
            return

        lines_text = "\n".join(
            [f"{i+1}. {line}" for i, line in enumerate(lines)])
        await state.set_state(ChatsContinuationStates.editing_line)
        await state.update_data(editing_action="edit")

        await message.answer(
            f"Какую строку редактировать?\n\n{lines_text}\n\nВведите номер (1, 2, 3...):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
                resize_keyboard=True))


@router.message(ChatsContinuationStates.editing_line)
async def save_chat_line(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        data = await state.get_data()
        section_key = data.get('section_key')
        lines = CHATS_CONTINUATION.get(section_key, [])
        lines_text = "\n".join(
            [f"{i+1}. {line}" for i, line in enumerate(lines)])

        await state.set_state(ChatsContinuationStates.managing_lines)
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="➕ Добавить строку")],
                      [KeyboardButton(text="✏️ Редактировать")],
                      [KeyboardButton(text="⬅️ Назад")]],
            resize_keyboard=True)

        await message.answer(f"<b>Текущие строки:</b>\n{lines_text}",
                             reply_markup=keyboard,
                             parse_mode=ParseMode.HTML)
        return

    data = await state.get_data()
    section_key = data.get('section_key')
    editing_action = data.get('editing_action')
    lines = CHATS_CONTINUATION.get(section_key, [])
    accumulated_text = data.get('accumulated_text', '')

    if editing_action == "add":
        if message.text == "✅ Готово":
            if accumulated_text:
                lines.append(accumulated_text)
                CHATS_CONTINUATION[section_key] = lines
                save_chats_continuation()

                lines_text = "\n".join(
                    [f"{i+1}. {line}" for i, line in enumerate(lines)])
                await state.set_state(ChatsContinuationStates.managing_lines)
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="➕ Добавить строку")],
                              [KeyboardButton(text="✏️ Редактировать")],
                              [KeyboardButton(text="⬅️ Назад")]],
                    resize_keyboard=True)

                await message.answer(
                    f"✅ Текст добавлен!\n\n<b>Текущие строки:</b>\n{lines_text}",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML)
            else:
                await message.answer(
                    "Текст пуст. Введите текст перед нажатием 'Готово'.")
        else:
            # Накапливаем текст
            new_accumulated = accumulated_text + message.text if accumulated_text else message.text
            await state.update_data(accumulated_text=new_accumulated)

            await message.answer(
                f"📝 Текст сохранён (всего символов: {len(new_accumulated)})\n\nПродолжайте вводить текст или нажмите ✅ Готово:",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="✅ Готово")],
                              [KeyboardButton(text="⬅️ Отмена")]],
                    resize_keyboard=True))

    elif editing_action == "edit":
        try:
            line_num = int(message.text) - 1
            if 0 <= line_num < len(lines):
                await state.update_data(line_num=line_num,
                                        accumulated_text=lines[line_num])
                await state.set_state(ChatsContinuationStates.editing_line)
                await state.update_data(editing_action="update")

                await message.answer(
                    f"Текущий текст строки {line_num + 1}:\n<code>{lines[line_num]}</code>\n\nВведите новый текст или нажмите ✅ Готово:",
                    reply_markup=ReplyKeyboardMarkup(
                        keyboard=[[KeyboardButton(text="✅ Готово")],
                                  [KeyboardButton(text="⬅️ Отмена")]],
                        resize_keyboard=True),
                    parse_mode=ParseMode.HTML)
            else:
                await message.answer("Неверный номер строки.")
        except ValueError:
            await message.answer("Введите число (1, 2, 3...).")

    elif editing_action == "update":
        line_num = data.get('line_num')

        if message.text == "✅ Готово":
            if accumulated_text:
                lines[line_num] = accumulated_text
                CHATS_CONTINUATION[section_key] = lines
                save_chats_continuation()

                lines_text = "\n".join(
                    [f"{i+1}. {line}" for i, line in enumerate(lines)])
                await state.set_state(ChatsContinuationStates.managing_lines)
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="➕ Добавить строку")],
                              [KeyboardButton(text="✏️ Редактировать")],
                              [KeyboardButton(text="⬅️ Назад")]],
                    resize_keyboard=True)

                await message.answer(
                    f"✅ Текст обновлен!\n\n<b>Текущие строки:</b>\n{lines_text}",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML)
        else:
            # Накапливаем текст при редактировании
            new_accumulated = accumulated_text + message.text if accumulated_text else message.text
            await state.update_data(accumulated_text=new_accumulated)

            await message.answer(
                f"📝 Текст обновлён (всего символов: {len(new_accumulated)})\n\nПродолжайте вводить текст или нажмите ✅ Готово:",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="✅ Готово")],
                              [KeyboardButton(text="⬅️ Отмена")]],
                    resize_keyboard=True))


async def manage_menu(message: types.Message, state: FSMContext):
    buttons = await get_all_keyboard_buttons()
    # buttons - это список записей, где b['label'] - текст кнопки
    keyboard_buttons = []
    for b in buttons:
        keyboard_buttons.append([KeyboardButton(text=b['label'])])

    keyboard_buttons.append([KeyboardButton(text="➕ Создать кнопку")])
    keyboard_buttons.append([KeyboardButton(text="⬅️ Назад")])

    kb = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
    await state.set_state(AdminMenuStates.managing_menu)
    await message.answer("🛠 <b>Управление меню</b>\n\nНажмите на кнопку, чтобы <b>удалить</b> её, или используйте '➕ Создать кнопку'.", reply_markup=kb, parse_mode=ParseMode.HTML)

# Дубликат удален - используется версия на строке 503

async def add_button_label(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    clean_label = message.text.strip()
    print(f"[BOT_DEBUG] User provided button label: '{clean_label}'")
    await state.update_data(label=clean_label)
    await state.set_state(AdminMenuStates.adding_button_content)
    await message.answer(f"Отлично! Теперь введите текст для кнопки '{clean_label}':")

@router.message(AdminMenuStates.adding_button_content)
async def add_button_content(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    content = message.text
    print(f"[BOT_DEBUG] User provided button content (length: {len(content)})")
    await state.update_data(content=content)
    await state.set_state(AdminMenuStates.adding_button_photo)
    await message.answer("Пришлите фото или нажмите 'Пропустить':", 
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Пропустить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(AdminMenuStates.adding_button_photo)
async def add_button_photo(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    if message.photo:
        await state.update_data(photo=message.photo[-1].file_id)

    await state.set_state(AdminMenuStates.adding_inline_button_text)
    await message.answer("Введите текст для инлайн-кнопки (или 'Пропустить'):",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Пропустить")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))

@router.message(AdminMenuStates.adding_inline_button_text)
async def add_inline_text(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    if message.text == "Пропустить":
        data = await state.get_data()
        await finalize_button_creation(message, state, data)
        return

    await state.update_data(inline_text=message.text)
    await state.set_state(AdminMenuStates.adding_inline_button_url)
    await message.answer("Введите ссылку для инлайн-кнопки:")

@router.callback_query(F.data.startswith("page:"))
async def handle_page_navigation(query: types.CallbackQuery):
    """Обработчик навигации по страницам"""
    try:
        # Парсим callback_data: "page:short_id:page_num"
        parts = query.data.split(":", 2)
        if len(parts) != 3:
            await query.answer("Ошибка навигации")
            return

        short_id = parts[1]
        page_num = int(parts[2])

        print(f"[PAGES] Navigating to page {page_num}, short_id: '{short_id}'")

        # Получаем контент из БД по короткому ID
        db_content = await get_button_by_short_id(short_id)

        if not db_content:
            print(f"[PAGES] Button not found by short_id, trying as full button_id...")
            db_content = await get_button_content(short_id)

        if not db_content or not db_content.get('pages_json'):
            await query.answer("❌ Страницы не найдены")
            return

        button_id = db_content['button_id']
        print(f"[PAGES] Found button: '{button_id}'")

        # Парсим страницы
        pages = json.loads(db_content['pages_json'])

        if page_num < 0 or page_num >= len(pages):
            await query.answer("❌ Неверный номер страницы")
            return

        # Текст нужной страницы
        page_text = pages[page_num].get('text', 'Нет текста')

        # Создаём клавиатуру с кнопками навигации
        inline_keyboard_list = []

        # Инлайн-кнопки из buttons_json (если есть)
        if db_content.get('buttons_json'):
            try:
                btns = json.loads(db_content['buttons_json'])
                button_objects = []

                for b in btns:
                    btn_text = b.get('text', '???')

                    # Пропускаем старые кнопки назад
                    if b.get('url') == 'меню' or btn_text in ['🔙 Назад', '🔙 В начало']:
                        continue

                    if b.get('url'):
                        button_objects.append(InlineKeyboardButton(text=btn_text, url=b['url']))
                    else:
                        target_id = b.get('id') or f"{button_id}:{btn_text}"
                        button_objects.append(InlineKeyboardButton(text=btn_text, callback_data=make_callback_data(target_id)))

                # Группируем кнопки по рядам
                default_per_row = db_content.get('buttons_per_row', 1)
                inline_keyboard_list = group_buttons_by_row(button_objects, btns, default_per_row)
            except Exception as e:
                print(f"[PAGES] Error parsing buttons_json: {e}")

        # Добавляем кнопки навигации по страницам
        nav_buttons = create_page_navigation_buttons(button_id, page_num, len(pages))
        inline_keyboard_list.append(nav_buttons)

        # Кнопка "Назад" к родителю (если есть)
        if db_content.get('parent_id'):
            parent_id = db_content['parent_id']
            inline_keyboard_list.append([InlineKeyboardButton(text="🔙 Назад", callback_data=make_callback_data(parent_id))])

        kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list)

        # Обновляем сообщение
        photo = db_content.get('photo_file_id')

        if photo:
            await query.message.edit_media(
                media=types.InputMediaPhoto(media=photo, caption=page_text, parse_mode=ParseMode.HTML),
                reply_markup=kb
            )
        else:
            await query.message.edit_text(
                page_text,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True)
            )

        await query.answer(f"📄 Страница {page_num + 1}/{len(pages)}")

    except Exception as e:
        print(f"[PAGES] Error: {e}")
        await query.answer("❌ Ошибка при переключении страницы")

@router.callback_query(F.data.startswith("dyn:"))
async def process_dynamic_inline(query: types.CallbackQuery, state: FSMContext):
    short_id = query.data[4:]  # Извлекаем short_id (12-символьный хеш)
    print(f"\n[BOT_DEBUG_VERBOSE] === process_dynamic_inline Start ===")
    print(f"[BOT_DEBUG_VERBOSE] Callback Data: '{query.data}'")
    print(f"[BOT_DEBUG_VERBOSE] Short ID from data: '{short_id}'")

    # Ищем кнопку по короткому ID
    db_content = await get_button_by_short_id(short_id)
    button_id = db_content['button_id'] if db_content else None

    if not db_content:
        # Попробуем найти по полному ID (для обратной совместимости)
        print(f"[BOT_DEBUG_VERBOSE] Not found by short_id, trying as full button_id...")
        db_content = await get_button_content(short_id)
        button_id = short_id if db_content else None

    if not db_content:
        # Попробуем найти по тексту кнопки, если ID не совпал напрямую
        print(f"[BOT_DEBUG_VERBOSE] DB Content NOT found, attempting fallback fuzzy search...")
        all_btns = await get_all_keyboard_buttons()
        print(f"[BOT_DEBUG_VERBOSE] Searching through {len(all_btns)} labels...")
        for b in all_btns:
            b_lbl = b.get('label') if isinstance(b, dict) else (getattr(b, 'label', None) or b['label'] if hasattr(b, '__getitem__') else None)
            if b_lbl and b_lbl.strip().lower() == short_id.strip().lower():
                print(f"[BOT_DEBUG_VERBOSE] ✅ Fallback Match Found: '{b_lbl}'")
                db_content = await get_button_content(b_lbl)
                if db_content:
                    print(f"[BOT_DEBUG_VERBOSE] Successfully loaded content for fuzzy match '{b_lbl}'")
                    break

        # Если всё ещё не нашли - ищем в MENU_STRUCTURE
        if not db_content:
            print(f"[BOT_DEBUG_VERBOSE] Searching in MENU_STRUCTURE for '{button_id}'...")

            def find_in_menu_structure(target_id, structure=None):
                """Ищет меню по ID в MENU_STRUCTURE"""
                if structure is None:
                    structure = MENU_STRUCTURE

                for menu_id, menu_data in structure.items():
                    if menu_id == target_id:
                        return menu_data

                    if 'submenu' in menu_data:
                        result = find_in_menu_structure(target_id, menu_data['submenu'])
                        if result:
                            return result
                return None

            found_menu = find_in_menu_structure(button_id)
            if found_menu:
                print(f"[BOT_DEBUG_VERBOSE] ✅ Found in MENU_STRUCTURE: '{button_id}'")
                # Создаём временный объект как если бы это был из БД
                db_content = {
                    'content': found_menu.get('text', 'Нет описания'),
                    'photo_file_id': None,
                    'buttons_json': None,
                    'parent_id': None
                }

                # Если есть submenu - создаём кнопки
                if found_menu.get('type') == 'inline' and found_menu.get('submenu'):
                    buttons = []
                    for sub_id, sub_data in found_menu['submenu'].items():
                        buttons.append({
                            'text': sub_data.get('label', sub_id),
                            'id': sub_id
                        })
                    db_content['buttons_json'] = json.dumps(buttons)
                    print(f"[BOT_DEBUG_VERBOSE] Created {len(buttons)} buttons from MENU_STRUCTURE submenu")

                # Если есть buttons - добавляем их
                elif found_menu.get('buttons'):
                    buttons = []
                    for btn in found_menu['buttons']:
                        if btn.get('url'):
                            buttons.append({'text': btn['text'], 'url': btn['url']})
                        elif btn.get('callback'):
                            # Извлекаем ID из callback (inline_xxx -> xxx)
                            callback_id = btn['callback'].replace('inline_', '')
                            buttons.append({'text': btn['text'], 'id': callback_id})
                    db_content['buttons_json'] = json.dumps(buttons)
                    print(f"[BOT_DEBUG_VERBOSE] Created {len(buttons)} buttons from MENU_STRUCTURE buttons array")

    if db_content:
        print(f"[BOT_DEBUG_VERBOSE] ✅ SUCCESS: Content found for '{button_id}'")
        print(f"[BOT_DEBUG_VERBOSE] DB Parent ID: '{db_content.get('parent_id')}'")
        msg_text = db_content.get('content', 'Нет содержимого')
        photo = db_content.get('photo_file_id')
        kb = None
        inline_keyboard_list = []

        if db_content.get('buttons_json'):
            print(f"[BOT_DEBUG_VERBOSE] Found inline buttons JSON: {db_content['buttons_json']}")
            try:
                btns = json.loads(db_content['buttons_json'])
                print(f"[BOT_DEBUG_VERBOSE] Parsed {len(btns)} buttons from JSON")

                # Получаем настройку расположения (старая система, используется как дефолт)
                default_buttons_per_row = db_content.get('buttons_per_row', 1)
                print(f"[BOT_DEBUG_VERBOSE] Default buttons per row: {default_buttons_per_row}")

                # Создаём список кнопок
                button_objects = []
                has_back_button = False  # Отслеживаем наличие кнопки назад в buttons_json

                for i, b in enumerate(btns):
                    btn_text = b.get('text', '???')
                    row_width = b.get('row_width', default_buttons_per_row)
                    print(f"[BOT_DEBUG_VERBOSE] Button {i+1}: '{btn_text}' (row_width={row_width})")

                    # Проверяем на кнопку назад из миграции (url='меню')
                    if b.get('url') == 'меню' or btn_text in ['🔙 Назад', '🔙 В начало']:
                        has_back_button = True
                        print(f"[BOT_DEBUG_VERBOSE] -> Found back button in buttons_json: '{btn_text}', skipping (will add based on parent_id)")
                        continue  # Пропускаем старые кнопки назад

                    if b.get('url'):
                        print(f"[BOT_DEBUG_VERBOSE] -> URL: {b['url']}")
                        button_objects.append(InlineKeyboardButton(text=btn_text, url=b['url']))
                    else:
                        # Если ID не задан в JSON, формируем его
                        target_id = b.get('id') or f"{button_id}:{btn_text}"
                        print(f"[BOT_DEBUG_VERBOSE] -> Submenu ID: {target_id}")
                        button_objects.append(InlineKeyboardButton(text=btn_text, callback_data=make_callback_data(target_id)))

                # Группируем кнопки с учётом индивидуальной ширины
                inline_keyboard_list = group_buttons_by_row(button_objects, btns, default_buttons_per_row)

            except Exception as e:
                print(f"[BOT_DEBUG_VERBOSE] ❌ ERROR parsing inline buttons JSON: {e}")
        else:
            print(f"[BOT_DEBUG_VERBOSE] No buttons_json (no inline buttons from buttons)")

        # Проверяем pages_json независимо от buttons_json
        if db_content.get('pages_json'):
            try:
                pages = json.loads(db_content['pages_json'])
                if len(pages) > 1:
                    # Добавляем кнопки навигации для первой страницы
                    nav_buttons = create_page_navigation_buttons(button_id, 0, len(pages))
                    inline_keyboard_list.append(nav_buttons)
                    print(f"[BOT_DEBUG_VERBOSE] Added page navigation: {len(pages)} pages")
            except Exception as e:
                print(f"[BOT_DEBUG_VERBOSE] Error adding page navigation: {e}")

        # Добавляем кнопку назад только если есть parent_id (не первый уровень)
        if db_content.get('parent_id'):
            parent_id = db_content['parent_id']
            print(f"[BOT_DEBUG_VERBOSE] Adding 'Back' button -> dyn:{parent_id}")
            inline_keyboard_list.append([InlineKeyboardButton(text="🔙 Назад", callback_data=make_callback_data(parent_id))])
        else:
            print(f"[BOT_DEBUG_VERBOSE] No parent_id (first level menu), no back button needed")

        # Создаем клавиатуру только если есть кнопки
        if inline_keyboard_list:
            kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list)
            print(f"[BOT_DEBUG_VERBOSE] Created keyboard with {len(inline_keyboard_list)} rows")

        try:
            if photo:
                print(f"[BOT_DEBUG_VERBOSE] Updating message as Media (Photo: {photo[:15]}...)")
                await query.message.edit_media(
                    media=types.InputMediaPhoto(media=photo, caption=msg_text, parse_mode=ParseMode.HTML),
                    reply_markup=kb
                )
            else:
                print(f"[BOT_DEBUG_VERBOSE] Updating message as Text")
                await query.message.edit_text(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML,
                                            link_preview_options=LinkPreviewOptions(is_disabled=True))
            print(f"[BOT_DEBUG_VERBOSE] ✅ Message updated successfully")
        except Exception as e:
            if "message is not modified" in str(e):
                print("[BOT_DEBUG_VERBOSE] Message content is identical, nothing to update.")
            else:
                print(f"[BOT_DEBUG_VERBOSE] ❌ ERROR updating message: {e}")
                # Fallback to answer if edit fails
                if photo:
                    await query.message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                else:
                    await query.message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML,
                                             link_preview_options=LinkPreviewOptions(is_disabled=True))
    else:
        print(f"[BOT_DEBUG_VERBOSE] ❌ FAIL: Content NOT found in DB for ID: '{button_id}'")
        await query.answer("❌ Раздел не найден в базе данных", show_alert=True)

    await query.answer()

@router.message(AdminMenuStates.adding_inline_button_url)
async def add_inline_url(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Отмена":
        return await manage_menu(message, state)

    data = await state.get_data()
    inline_text = data.get('inline_text')

    if message.text == "Кнопка с меню":
        # Переход к созданию вложенного меню
        await state.update_data(is_nested=True)
        await state.set_state(AdminMenuStates.adding_button_label)
        await message.answer(f"Создаем вложенное меню для кнопки '{inline_text}'.\nВведите название (label) для этого подраздела:")
        return

    if not (message.text.startswith("http") or message.text.startswith("tg://")):
        await message.answer("❌ Ошибка! Ссылка должна начинаться с http или tg://. Либо выберите 'Кнопка с меню':",
                             reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Кнопка с меню")], [KeyboardButton(text="⬅️ Отмена")]], resize_keyboard=True))
        return

    await state.update_data(inline_url=message.text)
    data = await state.get_data()
    await finalize_button_creation(message, state, data)

async def finalize_button_creation(message: types.Message, state: FSMContext, data: dict):
    label = data['label']
    content = data['content']
    photo = data.get('photo')
    inline_text = data.get('inline_text')
    inline_url = data.get('inline_url')
    parent_id = data.get('parent_id')
    is_nested = data.get('is_nested', False)

    print(f"\n[BOT_DEBUG] === finalize_button_creation Start ===")
    print(f"[BOT_DEBUG] Target: '{label}', Parent: '{parent_id}', Nested: {is_nested}")
    print(f"[BOT_DEBUG] Inline Text: '{inline_text}', URL: '{inline_url}'")

    buttons_json = None
    if inline_text:
        btn_data = {"text": inline_text}
        if is_nested:
            # Формируем ID: родитель:название
            nested_id = f"{label}:{inline_text}"
            btn_data["id"] = nested_id
            print(f"[BOT_DEBUG] Generated nested ID: '{nested_id}'")
            # Регистрируем в списке разрешенных
            await add_keyboard_button(nested_id)
            # Сохраняем пустую заглушку для вложенного меню
            await update_button_content(nested_id, f"Содержимое раздела '{inline_text}'", parent_id=label)
        else:
            btn_data["url"] = inline_url

        # Обновляем список кнопок у родителя
        existing = await get_button_content(label)
        current_btns = []
        if existing and existing.get('buttons_json'):
            try:
                current_btns = json.loads(existing['buttons_json'])
            except: pass

        # Добавляем или обновляем
        found = False
        for i, b in enumerate(current_btns):
            if b['text'] == inline_text:
                current_btns[i] = btn_data
                found = True
                break
        if not found:
            current_btns.append(btn_data)

        buttons_json = json.dumps(current_btns)

    # Сохраняем в Reply клавиатуру если это корень
    if not parent_id:
        print(f"[BOT_DEBUG] Adding root button '{label}' to Reply Keyboard")
        await add_keyboard_button(label)

    # Сохраняем контент в БД
    if await update_button_content(label, content, photo, buttons_json, parent_id=parent_id):
        print(f"[BOT_DEBUG] ✅ finalized successfully")
        await message.answer(f"✅ Кнопка '{label}' сохранена!")
    else:
        print(f"[BOT_DEBUG] ❌ Failed to update content in DB")
        await message.answer("❌ Ошибка при сохранении.")

    await state.clear()
    await manage_menu(message, state)

@router.message(F.text == "📝 Управление меню")
async def cmd_manage_menu(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await manage_menu(message, state)


@router.message(F.text == "🔙 Назад в меню")
async def back_to_menu(message: types.Message, state: FSMContext):
    data = await state.get_data()
    current_menu = data.get('current_menu')
    current_submenu = data.get('current_submenu')

    # If in nested submenu, go back to parent submenu
    if current_submenu and current_menu:
        menu = MENU_STRUCTURE.get(current_menu)
        if menu and 'submenu' in menu:
            sub_menu = menu['submenu'].get(current_submenu)
            if sub_menu and 'submenu' in sub_menu:
                # Go back to submenu selection
                await state.update_data(current_submenu=None)
                keyboard = get_submenu_keyboard(current_menu, current_submenu)
                await message.answer(
                    sub_menu.get('text', 'Выберите опцию:'),
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
                return

    # If in main submenu, go back to main menu
    if current_menu:
        await state.clear()
        keyboard = get_main_keyboard(message.from_user.id)
        await message.answer("Главное меню:", reply_markup=keyboard)
        return

    # Default - show main menu
    await state.clear()
    keyboard = await get_dynamic_keyboard_async(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=keyboard)


@router.callback_query(F.data.startswith("main:"))
async def callback_main_section(query: types.CallbackQuery):
    """Обработчик для кнопок из inline результатов"""
    if not query.message:
        await query.answer("Ошибка доступа к сообщению")
        return

    # Получаем ключ раздела из callback_data (main:section_key)
    section_key = query.data[5:]  # Удаляем префикс "main:"

    # Ищем раздел в главном меню
    section = MENU_STRUCTURE.get(section_key)

    if not section:
        await query.answer("Раздел не найден", show_alert=True)
        return

    # Log click statistics
    await log_click(section.get('label', section_key))

    try:
        # Если есть submenu, показываем его
        if 'submenu' in section:
            # Для inline режима всегда используем inline клавиатуру
            keyboard = get_submenu_inline_keyboard(section_key)

            if query.message and isinstance(query.message, types.Message):
                await query.message.edit_text(
                    section.get('text', 'Выберите опцию:'),
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
            else:
                await query.answer()
        # Если есть pages, показываем первую страницу
        elif 'pages' in section:
            text_content = section['pages'][0].get('text', '')
            keyboard = get_nav_keyboard_inline(section_key, '', 0)

            if keyboard is None:
                # Fallback если keyboard не создана
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="🔙 Назад",
                                         callback_data="back_nav")
                ]])

            if query.message and isinstance(query.message, types.Message):
                await query.message.edit_text(
                    text_content,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
            else:
                await query.answer()
        # Иначе показываем просто текст с back кнопкой
        elif 'text' in section:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Назад", callback_data="back_nav")
            ]])
            await query.message.edit_text(
                section['text'],
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True))
    except Exception as e:
        logger.error(f"Error in callback_main_section: {e}")
        await query.answer("Произошла ошибка", show_alert=True)

    await query.answer()


@router.callback_query(F.data == "main_menu")
async def callback_main_menu(query: types.CallbackQuery):
    if not query.message:
        return
    keyboard = get_main_keyboard(query.from_user.id)
    try:
        await query.message.edit_text("Главное меню:")
    except Exception:
        pass
    await query.answer()


@router.callback_query(F.data == "noop")
async def callback_noop(query: types.CallbackQuery):
    await query.answer()




@router.callback_query(F.data == "back_nav")
async def callback_back_nav(query: types.CallbackQuery):
    # If no message, we're in inline mode - use inline_message_id
    if not query.message:
        logger.debug("Inline mode for back_nav")
        if query.inline_message_id:
            try:
                keyboard = get_main_keyboard(query.from_user.id)
                await bot.edit_message_text(
                    inline_message_id=query.inline_message_id,
                    text="Главное меню:",
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML)
                logger.info(f"✅ Inline back_nav edited")
            except Exception as e:
                logger.error(f"Error in back_nav (inline): {e}")
        await query.answer()
        return

    # Extract parent from message text or go to main menu
    try:
        keyboard = get_main_keyboard(query.from_user.id)
        await query.message.edit_text("Главное меню:",
                                      reply_markup=keyboard,
                                      parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in back_nav: {e}")
    await query.answer()


@router.callback_query(F.data.startswith("back_inline:"))
async def callback_back_inline(query: types.CallbackQuery):
    parent_key = query.data[12:]  # Extract parent key after "back_inline:"

    # Check if parent_key is a main menu item (first level)
    if parent_key in MENU_STRUCTURE:
        found_menu = MENU_STRUCTURE[parent_key]
        found_in_parent = None
        if found_menu.get("type") == "inline" and "submenu" in found_menu:
            # Add back button to return to main menu for first-level inline menus
            kb = get_inline_keyboard(found_menu["submenu"],
                                     parent_key,
                                     add_back_button=False)
            try:
                await edit_message_safe(
                    query, found_menu.get('text', 'Выберите опцию:'), kb)
            except Exception as e:
                logger.error(f"Error in back_inline (first level): {e}")
            await query.answer()
            return

    # Deep search to find the parent menu in submenus
    found_menu = None
    found_in_parent = None

    # Specific fix for CPM and PDP range back buttons
    if parent_key == "info":
        found_in_parent = "garant_checker"
        found_menu = MENU_STRUCTURE["garant_checker"]["submenu"]["info"]
    elif parent_key == "chats":
        found_in_parent = None
        found_menu = MENU_STRUCTURE["chats"]
    elif parent_key in [
            "thematic_admin", "infobusiness", "general_admin", "business_chats"
    ]:
        found_in_parent = "chats"
        found_menu = MENU_STRUCTURE["chats"]["submenu"][parent_key]
    elif parent_key == "cpm_pdp":
        found_in_parent = None
        found_menu = MENU_STRUCTURE["cpm_pdp"]
    elif parent_key == "cpm_prices":
        found_in_parent = "cpm_pdp"
        found_menu = MENU_STRUCTURE["cpm_pdp"]["submenu"]["cpm_prices"]
    elif parent_key == "pdp_prices":
        found_in_parent = "cpm_pdp"
        found_menu = MENU_STRUCTURE["cpm_pdp"]["submenu"]["pdp_prices"]
    elif parent_key == "inline_cpm_pdp":
        found_in_parent = None
        found_menu = MENU_STRUCTURE["cpm_pdp"]
    elif parent_key in ["cpm_range_1", "cpm_range_2", "cpm_range_3"]:
        found_in_parent = "cpm_prices"
        found_menu = MENU_STRUCTURE["cpm_pdp"]["submenu"]["cpm_prices"][
            "submenu"][parent_key]
    elif parent_key in ["pdp_range_1", "pdp_range_2", "pdp_range_3"]:
        found_in_parent = "pdp_prices"
        found_menu = MENU_STRUCTURE["cpm_pdp"]["submenu"]["pdp_prices"][
            "submenu"][parent_key]
    else:
        for menu_key, menu_data in MENU_STRUCTURE.items():
            if "submenu" in menu_data:
                if parent_key in menu_data['submenu']:
                    found_menu = menu_data['submenu'][parent_key]
                    found_in_parent = menu_key
                    break
                # Search in nested submenus (second level)
                for sub_key, sub_menu in menu_data['submenu'].items():
                    if "submenu" in sub_menu and parent_key in sub_menu[
                            'submenu']:
                        found_menu = sub_menu['submenu'][parent_key]
                        found_in_parent = sub_key
                        break
            if found_menu:
                break

    if found_menu and found_menu.get(
            "type") == "inline" and "submenu" in found_menu:
        # Always show back button to return to parent
        show_back = True
        kb = get_inline_keyboard(found_menu["submenu"],
                                 parent_key,
                                 add_back_button=show_back)

        # Check if link preview should be disabled for this section
        is_link_preview_disabled = found_menu.get("link_preview") is False

        try:
            await edit_message_safe(
                query,
                found_menu.get('text', 'Выберите опцию:'),
                kb,
                link_preview_disabled=is_link_preview_disabled)
        except Exception as e:
            # Ignore "message is not modified" errors - content and buttons are already correct
            if "message is not modified" not in str(e):
                logger.error(f"Error in back_inline (submenu): {e}")
        await query.answer()
        return

    # If we found the menu (but it's a leaf node/text content)
    if found_menu:
        # Create buttons
        buttons_list = []
        if found_menu.get('buttons'):
            for btn in found_menu['buttons']:
                buttons_list.append([
                    InlineKeyboardButton(text=btn['text'],
                                         callback_data=btn['callback'])
                ])
        else:
            # Default back button
            back_callback = f"back_inline:{found_in_parent}" if found_in_parent else "back_nav"
            buttons_list.append([
                InlineKeyboardButton(text="🔙 Назад",
                                     callback_data=back_callback)
            ])

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons_list)

        try:
            await edit_message_safe(query,
                                    found_menu.get('text', 'Нет описания'),
                                    keyboard)
        except Exception as e:
            # Ignore "message is not modified" errors
            if "message is not modified" not in str(e):
                logger.error(f"Error in back_inline (no submenu): {e}")
    else:
        # Parent menu not found, go to main menu
        keyboard = get_main_keyboard(query.from_user.id)
        try:
            await edit_message_safe(query, "Главное меню:", keyboard)
        except Exception as e:
            logger.error(f"Error in back_inline (main menu): {e}")

    await query.answer()


@router.callback_query(F.data.startswith("page:"))
async def callback_page_nav(query: types.CallbackQuery):
    logger.info(
        f"📄 Page navigation: {query.data}, has_message: {query.message is not None}, inline_id: {query.inline_message_id if hasattr(query, 'inline_message_id') else 'N/A'}"
    )

    if not query.data:
        logger.warning("Query data is None for page_nav")
        await query.answer()
        return

    parts = query.data.split(":")
    if len(parts) >= 4:
        menu_key = parts[1]
        sub_key = parts[2]
        try:
            page_index = int(parts[3])
        except (ValueError, IndexError):
            await query.answer()
            return

        logger.debug(
            f"Page nav: menu_key={menu_key}, sub_key={sub_key}, page_index={page_index}"
        )

        # Deep search for menu data
        menu = None

        # First try to get from main MENU_STRUCTURE by menu_key
        if menu_key and menu_key in MENU_STRUCTURE:
            menu = MENU_STRUCTURE[menu_key]
            # If this has pages and no sub_key specified, use it directly
            if 'pages' in menu and not sub_key:
                logger.debug(f"Found main menu with pages: {menu_key}")
            # Otherwise try to get submenu
            elif sub_key and 'submenu' in menu and sub_key in menu['submenu']:
                menu = menu['submenu'][sub_key]
                logger.debug(f"Found submenu: {menu_key}/{sub_key}")
        else:
            # Deep search for sub_key if menu_key not found
            logger.debug(f"Deep searching for sub_key: {sub_key}")
            for m_key, m_data in MENU_STRUCTURE.items():
                if m_key == sub_key:
                    menu = m_data
                    break
                if 'submenu' in m_data:
                    if sub_key in m_data['submenu']:
                        menu = m_data['submenu'][sub_key]
                        break
                    for s_key, s_data in m_data['submenu'].items():
                        if 'submenu' in s_data and sub_key in s_data['submenu']:
                            menu = s_data['submenu'][sub_key]
                            break
                if menu: break

        if not menu or 'pages' not in menu:
            logger.warning(
                f"Menu not found or no pages for menu_key={menu_key}, sub_key={sub_key}"
            )
            logger.debug(
                f"Available keys in MENU_STRUCTURE: {list(MENU_STRUCTURE.keys())}"
            )
            await query.answer()
            return

        pages = menu['pages']

        if 0 <= page_index < len(pages):
            page = pages[page_index]
            text = page.get('text') if isinstance(page, dict) else page
            keyboard = get_nav_keyboard_inline(menu_key, sub_key, page_index)

            logger.debug(
                f"Page content length: {len(text) if text else 0}, total pages: {len(pages)}"
            )

            # Handle both regular messages and inline messages
            if query.message:
                logger.debug("Editing regular message")
                try:
                    await query.message.edit_text(
                        text,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        link_preview_options=LinkPreviewOptions(
                            is_disabled=True))
                    logger.info(
                        f"✅ Regular message edited, page {page_index + 1}/{len(pages)}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error editing regular message: {type(e).__name__}: {e}"
                    )
            elif query.inline_message_id:
                logger.debug(
                    f"Editing inline message: {query.inline_message_id}")
                try:
                    await bot.edit_message_text(
                        inline_message_id=query.inline_message_id,
                        text=text,
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        link_preview_options=LinkPreviewOptions(
                            is_disabled=True))
                    logger.info(
                        f"✅ Inline message edited, page {page_index + 1}/{len(pages)}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error editing inline message: {type(e).__name__}: {e}"
                    )
            else:
                logger.warning("No message or inline_message_id to edit")

    await query.answer()


def get_inline_keyboard(submenu_data,
                        parent_key="",
                        add_back_button=True,
                        first_button_full_width=False):
    keyboard = []

    # Custom layout for Garant Checker
    if parent_key == "garant_checker":
        # Info (Full width)
        if "info" in submenu_data:
            keyboard.append([
                InlineKeyboardButton(
                    text=submenu_data["info"]["label"],
                    callback_data="inline_garant_checker:info")
            ])
    elif parent_key == "info":
        # If we're inside info submenu, show exchanges and mammont buttons
        # Exchanges (Full width)
        if "exchanges" in submenu_data:
            keyboard.append([
                InlineKeyboardButton(text=submenu_data["exchanges"]["label"],
                                     callback_data="inline_info:exchanges")
            ])

        # Mammont 1 & 2 (Two columns)
        row = []
        if "mammontav1" in submenu_data:
            row.append(
                InlineKeyboardButton(text=submenu_data["mammontav1"]["label"],
                                     callback_data="inline_info:mammontav1"))
        if "mammontav2" in submenu_data:
            row.append(
                InlineKeyboardButton(text=submenu_data["mammontav2"]["label"],
                                     callback_data="inline_info:mammontav2"))
        if row:
            keyboard.append(row)
    elif parent_key == "chats":
        # Custom layout for Chats:
        # 1. Тематические [админ] (Full width)
        # 2. Инфобиз и общие [админ] (Two columns)
        # 3. Бизнес (Full width)

        # Тематические [админ]
        if "thematic_admin" in submenu_data:
            keyboard.append([
                InlineKeyboardButton(
                    text=submenu_data["thematic_admin"]["label"],
                    callback_data="inline_chats:thematic_admin")
            ])

        # Инфобиз и общие [админ] in one row
        row = []
        if "infobusiness" in submenu_data:
            row.append(
                InlineKeyboardButton(
                    text=submenu_data["infobusiness"]["label"],
                    callback_data="inline_chats:infobusiness"))
        if "general_admin" in submenu_data:
            row.append(
                InlineKeyboardButton(
                    text=submenu_data["general_admin"]["label"],
                    callback_data="inline_chats:general_admin"))
        if row:
            keyboard.append(row)

        # Бизнес
        if "business_chats" in submenu_data:
            keyboard.append([
                InlineKeyboardButton(
                    text=submenu_data["business_chats"]["label"],
                    callback_data="inline_chats:business_chats")
            ])
    else:
        # Default layout logic
        row = []
        items = list(submenu_data.items())

        for i, (sub_key, sub_menu) in enumerate(items):
            # Check if this is a URL button (like for stickers)
            if "url" in sub_menu:
                keyboard.append([
                    InlineKeyboardButton(text=sub_menu['label'],
                                         url=sub_menu['url'])
                ])
                continue

            callback_str = f"inline_{parent_key}:{sub_key}" if parent_key else f"inline_{sub_key}"
            button = InlineKeyboardButton(text=sub_menu['label'],
                                          callback_data=callback_str)

            if first_button_full_width and i == 0:
                keyboard.append([button])
            else:
                row.append(button)
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
        if row:
            keyboard.append(row)

    # Add back button only if requested
    if add_back_button:
        # Special case: Remove back button when we are in the 'cpm_pdp' main menu
        if parent_key == "cpm_pdp":
            return InlineKeyboardMarkup(inline_keyboard=keyboard)

        # Ensure back button from prices leads to 'что вас интересует' (cpm_pdp)
        if parent_key in ["cpm_prices", "pdp_prices", "sticker_prices"]:
            back_callback = "back_inline:cpm_pdp"
        # Back button from info submenu should go to garant_checker
        elif parent_key == "info":
            back_callback = "back_inline:garant_checker"
        else:
            back_callback = f"back_inline:{parent_key}" if parent_key else "back_nav"

        keyboard.append([
            InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)
        ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_submenu_inline_keyboard(menu_key, parent_sub_key=None):
    """Получить inline клавиатуру для submenu (для inline режима)"""
    menu = MENU_STRUCTURE.get(menu_key)
    if not menu or 'submenu' not in menu:
        return None

    # If parent_sub_key is specified, get the nested submenu
    if parent_sub_key:
        sub_menu = menu['submenu'].get(parent_sub_key)
        if not sub_menu or 'submenu' not in sub_menu:
            return None
        submenu_dict = sub_menu['submenu']
        # For nested menus, we go back to the top-level menu item
        back_callback = f"inline_{menu_key}"
    else:
        submenu_dict = menu['submenu']
        # For top-level submenus, we go back to the main navigation (main menu)
        back_callback = "back_nav"

    keyboard = []
    row = []
    for sub_key, sub_menu in submenu_dict.items():
        callback_str = f"inline_{menu_key}:{sub_key}" if menu_key else f"inline_{sub_key}"
        row.append(
            InlineKeyboardButton(text=sub_menu['label'],
                                 callback_data=callback_str))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # Add back button ALWAYS for this function
    keyboard.append(
        [InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


@router.callback_query(F.data.startswith("support:"))
async def callback_support_button(query: types.CallbackQuery):
    """Обработчик кнопок поддержки и промокодов"""
    support_data = query.data[8:]  # Remove 'support:' prefix

    support_texts = {
        "tgstat":
        "🤖 <b>Поддержка</b>: @TGStatSupportBot\n🎁 <b>Промокод</b>: <code>Lambarin</code> [5%]\n\n<a href=\"https://tgstat.ru/x/XXd7V\">Перейти на TGStat →</a>",
        "telemetr":
        "🤖 <b>Поддержка</b>: @TelemetrSupport\n🎁 <b>Промокод</b>: <code>Lambarin</code> [10%]\n\n<a href=\"https://telemetr.me/\">Перейти на Telemetr →</a>",
        "trustat":
        "🤖 <b>Поддержка</b>: @TrustatSupport\n\n<a href=\"https://t.me/trustat\">Перейти на Trustat →</a>",
        "botstat":
        "🤖 <b>Поддержка</b>: @botstatcontact\n\n<a href=\"https://botstat.io/\">Перейти на BotStat →</a>"
    }

    support_text = support_texts.get(support_data, "Поддержка не найдена")
    back_callback = f"inline_{support_data}"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔙 Назад к описанию",
                             callback_data=back_callback)
    ]])

    try:
        await edit_message_safe(query,
                                support_text,
                                keyboard,
                                link_preview_disabled=True)
    except Exception as e:
        logger.error(f"Error in support button: {e}")

    await query.answer()


@router.callback_query(F.data.startswith("inline_"))
async def callback_inline_button(query: types.CallbackQuery,
                                 state: FSMContext):
    logger.info(
        f"🔘 Inline button pressed: {query.data}, user_id: {query.from_user.id}, has_message: {query.message is not None}"
    )

    # Parse callback data - could be "inline_child" or "inline_parent:child"
    callback_data = query.data[7:]  # Remove 'inline_' prefix
    logger.debug(f"Parsed callback_data: {callback_data}")

    if ':' in callback_data:
        parent_key, sub_key = callback_data.split(':', 1)
    else:
        parent_key = ""
        sub_key = callback_data

    logger.debug(f"parent_key: {parent_key}, sub_key: {sub_key}")

    # Сначала пробуем найти в БД
    db_content = await get_button_content(sub_key)
    found_menu = None
    found_hierarchy = None
    top_parent = ""
    path = []
    effective_parent = parent_key if parent_key else "nav"

    if db_content:
        # Нашли в БД - используем контент из БД
        logger.debug(f"Found content in DB for {sub_key}")
        found_menu = {
            'label': sub_key,
            'text': db_content.get('content', 'Нет описания'),
            'type': 'db_content'
        }

        # Добавляем инлайн-кнопки из БД если есть
        if db_content.get('buttons_json'):
            try:
                buttons = json.loads(db_content['buttons_json'])
                if buttons:
                    found_menu['type'] = 'inline'
                    found_menu['db_buttons'] = buttons  # Сохраняем кнопки из БД
            except:
                pass

        # Логируем клик
        await log_click(sub_key)
    else:
        # Если в БД нет - ищем в MENU_STRUCTURE
        # Deep search function to find menu data by key and track hierarchy
        def find_hierarchy(submenu_data, target_key, path=None):
            if path is None: path = []
            if not submenu_data:
                return None
            if target_key in submenu_data:
                return {"menu": submenu_data[target_key], "path": path}
            for key, value in submenu_data.items():
                if isinstance(value, dict) and "submenu" in value:
                    res = find_hierarchy(value["submenu"], target_key,
                                         path + [key])
                    if res:
                        return res
            return None

        for menu_key, menu_data in MENU_STRUCTURE.items():
            if "submenu" in menu_data:
                found_hierarchy = find_hierarchy(menu_data["submenu"], sub_key)
                if found_hierarchy:
                    top_parent = menu_key
                    break

        if not found_hierarchy:
            logger.warning(f"Menu not found for sub_key: {sub_key}")
            await query.answer("Раздел не найден", show_alert=True)
            return

        found_menu = found_hierarchy["menu"]
        path = found_hierarchy["path"]

        # Log click statistics
        await log_click(found_menu.get('label', sub_key))

    # Determine the real parent for the back button
    if path:
        # If we have a path, the parent is the last item in the path
        effective_parent = path[-1]
    else:
        # If no path, the parent is the top-level MENU_STRUCTURE key
        effective_parent = top_parent

    logger.debug(
        f"Hierarchy found. Top: {top_parent}, Path: {path}, Effective Parent: {effective_parent}"
    )

    logger.debug(
        f"Found menu, has_pages: {'pages' in found_menu}, has_submenu: {'submenu' in found_menu}"
    )

    # If no message, we're in inline mode - try to edit inline message
    if not query.message:
        logger.debug(f"No message - checking for inline_message_id")
        logger.debug(
            f"inline_message_id: {query.inline_message_id if hasattr(query, 'inline_message_id') else 'N/A'}"
        )

        # Get text content based on menu type
        if 'pages' in found_menu:
            logger.debug("Using pages content")
            text_content = found_menu['pages'][0].get('text', 'Нет описания')
            keyboard = get_nav_keyboard_inline('', sub_key, 0)
        elif found_menu.get("type") == "inline" and "db_buttons" in found_menu:
            # Контент из БД с инлайн-кнопками
            logger.debug("Using DB inline buttons")
            text_content = found_menu.get('text', 'Выберите опцию:')
            buttons_list = []
            for btn in found_menu['db_buttons']:
                if btn.get('url'):
                    buttons_list.append([
                        InlineKeyboardButton(text=btn['text'], url=btn['url'])
                    ])
                else:
                    # Submenu button
                    callback_str = f"inline_{btn.get('id', btn['text'])}"
                    buttons_list.append([
                        InlineKeyboardButton(text=btn['text'], callback_data=callback_str)
                    ])
            # Добавляем кнопку назад
            back_callback = f"back_inline:{effective_parent}"
            buttons_list.append([
                InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)
            ])
            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons_list)
        elif found_menu.get("type") == "inline" and "submenu" in found_menu:
            logger.debug("Using inline submenu")
            text_content = found_menu.get('text', 'Выберите опцию:')
            # Only show back button if this is NOT a first-level menu item
            show_back = sub_key not in MENU_STRUCTURE
            keyboard = get_inline_keyboard(found_menu["submenu"],
                                           sub_key,
                                           add_back_button=show_back)
        else:
            logger.debug("Using simple text content")
            logger.debug("Using simple text content")
            text_content = found_menu.get('text', 'Нет описания')
            # Create buttons
            buttons_list = []
            if found_menu.get('buttons'):
                for btn in found_menu['buttons']:
                    buttons_list.append([
                        InlineKeyboardButton(text=btn['text'],
                                             callback_data=btn['callback'])
                    ])
            # Add back button ONLY if not already present in found_menu['buttons']
            back_callback = f"back_inline:{effective_parent}"
            has_back = False
            if found_menu.get('buttons'):
                for btn in found_menu['buttons']:
                    if "Назад" in btn.get('text', '') or "back" in btn.get(
                            'callback', ''):
                        has_back = True
                        break

            if not has_back:
                buttons_list.append([
                    InlineKeyboardButton(text="🔙 Назад",
                                         callback_data=back_callback)
                ])
            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons_list)

        # Try to edit inline message if we have inline_message_id
        if query.inline_message_id:
            logger.info(f"📝 Editing inline message: {query.inline_message_id}")
            try:
                await bot.edit_message_text(
                    inline_message_id=query.inline_message_id,
                    text=text_content,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
                logger.info(f"✅ Inline message edited successfully")
            except Exception as e:
                logger.error(
                    f"❌ Error editing inline message: {type(e).__name__}: {e}")
        else:
            logger.debug(f"No inline_message_id available, just answering")

        await query.answer()
        return

    # If found_menu has pages, show navigation
    if 'pages' in found_menu:
        text_content = found_menu['pages'][0].get('text', '')
        keyboard = get_nav_keyboard_inline('', sub_key, 0)

        if not keyboard:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Назад", callback_data="back_nav")
            ]])

        try:
            await query.message.edit_text(
                text_content,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            logger.error(f"Error editing message in pages: {e}")
            try:
                await query.answer("Ошибка при редактировании сообщения",
                                   show_alert=True)
            except:
                pass
    # Check if the found menu item itself has DB inline buttons
    elif found_menu.get("type") == "inline" and "db_buttons" in found_menu:
        # Контент из БД с инлайн-кнопками
        text_content = found_menu.get('text', 'Выберите опцию:')
        buttons_list = []
        for btn in found_menu['db_buttons']:
            if btn.get('url'):
                buttons_list.append([
                    InlineKeyboardButton(text=btn['text'], url=btn['url'])
                ])
            else:
                # Submenu button
                callback_str = f"inline_{btn.get('id', btn['text'])}"
                buttons_list.append([
                    InlineKeyboardButton(text=btn['text'], callback_data=callback_str)
                ])
        # Добавляем кнопку назад
        back_callback = f"back_inline:{effective_parent}"
        buttons_list.append([
            InlineKeyboardButton(text="🔙 Назад", callback_data=back_callback)
        ])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons_list)
        try:
            await query.message.edit_text(
                text_content,
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            logger.error(f"Error editing message with DB buttons: {e}")
            try:
                await query.answer("Ошибка при редактировании сообщения",
                                   show_alert=True)
            except:
                pass
    # Check if the found menu item itself has an inline submenu to show
    elif found_menu.get("type") == "inline" and "submenu" in found_menu:
        # Only show back button if this is NOT a first-level menu item
        show_back = sub_key not in MENU_STRUCTURE
        kb = get_inline_keyboard(found_menu["submenu"],
                                 sub_key,
                                 add_back_button=show_back)
        try:
            await query.message.edit_text(
                found_menu.get('text', 'Выберите опцию:'),
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            logger.error(f"Error editing message in submenu: {e}")
            try:
                await query.answer("Ошибка при редактировании сообщения",
                                   show_alert=True)
            except:
                pass
    else:
        text = found_menu.get('text', 'Нет описания')
        # Handle markdown-style links if they exist
        import re
        text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
        text = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2">\1</a>', text)

        # Create buttons - support buttons + back button
        buttons_list = []
        if found_menu.get('buttons'):
            for btn in found_menu['buttons']:
                buttons_list.append([
                    InlineKeyboardButton(text=btn['text'],
                                         callback_data=btn['callback'])
                ])

        # Add back button ONLY if not already present in found_menu['buttons']
        back_callback = f"back_inline:{effective_parent}"
        has_back = False
        if found_menu.get('buttons'):
            for btn in found_menu['buttons']:
                if "Назад" in btn.get('text', '') or "back" in btn.get(
                        'callback', ''):
                    has_back = True
                    break

        if not has_back:
            buttons_list.append([
                InlineKeyboardButton(text="🔙 Назад",
                                     callback_data=back_callback)
            ])

        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons_list)
        try:
            await query.message.edit_text(
                text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                link_preview_options=LinkPreviewOptions(is_disabled=True))
        except Exception as e:
            logger.error(f"Error editing message in text: {e}")
            try:
                await query.answer("Ошибка при редактировании сообщения",
                                   show_alert=True)
            except:
                pass

    await query.answer()


async def handle_button_click(message: types.Message, state: FSMContext):
    if not message or not message.text:
        return

    current_state = await state.get_state()
    if current_state:
        return

    text = message.text

    # Check if it's a main menu button
    for menu_key, menu_data in MENU_STRUCTURE.items():
        if menu_data.get('label') and text.strip().lower(
        ) == menu_data['label'].strip().lower():
            # Log click statistics
            await log_click(menu_data['label'])

            # Check for dynamic content
            db_content = await get_button_content(menu_data['label'])
            if db_content:
                msg_text = db_content['content']
                photo = db_content['photo_file_id']
                kb = None
                if db_content['buttons_json']:
                    btns = json.loads(db_content['buttons_json'])
                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=b['text'], url=b['url'])] for b in btns])

                if photo:
                    await message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                else:
                    await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                return

            await state.set_state(None)

            # If has submenu, show submenu
            if 'submenu' in menu_data:
                if menu_data.get("type") == "inline":
                    # Don't add back button for first-level menus (sites, chats, bots, cpm_pdp, garant_checker, analytics_services)
                    # For garant_checker, make first button (exchanges) full width
                    first_button_full_width = (menu_key == "garant_checker")
                    keyboard = get_inline_keyboard(
                        menu_data["submenu"],
                        menu_key,
                        add_back_button=False,
                        first_button_full_width=first_button_full_width)
                    await message.answer(
                        menu_data.get('text', 'Выберите опцию:'),
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        link_preview_options=LinkPreviewOptions(
                            is_disabled=True))
                else:
                    keyboard = get_submenu_keyboard(menu_key)
                    await state.update_data(current_menu=menu_key)
                    await message.answer(
                        menu_data.get('text', 'Выберите опцию:'),
                        reply_markup=keyboard,
                        parse_mode=ParseMode.HTML,
                        link_preview_options=LinkPreviewOptions(
                            is_disabled=True))
                return

            # If has pages, show first page with navigation
            elif 'pages' in menu_data:
                text_content = menu_data['pages'][0].get('text', '')
                keyboard = get_nav_keyboard_inline(menu_key, '', 0)
                await message.answer(
                    text_content,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
                return

            # Otherwise just show text
            elif 'text' in menu_data:
                keyboard = get_main_keyboard(message.from_user.id)
                await message.answer(
                    menu_data['text'],
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True))
                return

    # Check if it's a submenu button
    data = await state.get_data()
    current_menu = data.get('current_menu')
    current_submenu = data.get('current_submenu')

    if current_menu:
        menu = MENU_STRUCTURE.get(current_menu)
        if menu and 'submenu' in menu:
            # If we're in a nested submenu level
            if current_submenu:
                parent_sub = menu['submenu'].get(current_submenu)
                if parent_sub and 'submenu' in parent_sub:
                    for sub_key, sub_menu in parent_sub['submenu'].items():
                        if sub_menu.get('label') and text.strip().lower(
                        ) == sub_menu['label'].strip().lower():
                            # Log click statistics
                            await log_click(sub_menu['label'])

                            # Check for dynamic content
                            db_content = await get_button_content(sub_menu['label'])
                            if db_content:
                                msg_text = db_content['content']
                                photo = db_content['photo_file_id']
                                kb = None
                                if db_content['buttons_json']:
                                    btns = json.loads(db_content['buttons_json'])
                                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=b['text'], url=b['url'])] for b in btns])

                                if photo:
                                    await message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                                else:
                                    await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                                return

                            # If nested submenu has pages, show navigation
                            if 'pages' in sub_menu:
                                text_content = sub_menu['pages'][0].get(
                                    'text', '')
                                keyboard = get_nav_keyboard_inline(
                                    current_menu, sub_key, 0)
                                await message.answer(
                                    text_content,
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    link_preview_options=LinkPreviewOptions(
                                        is_disabled=True))
                                return

                            # Otherwise just show text
                            elif 'text' in sub_menu:
                                keyboard = get_submenu_keyboard(
                                    current_menu, current_submenu)
                                await message.answer(
                                    sub_menu['text'],
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    link_preview_options=LinkPreviewOptions(
                                        is_disabled=True))
                                return
            else:
                # First level submenu
                for sub_key, sub_menu in menu['submenu'].items():
                    if sub_menu.get('label') and text.strip().lower(
                    ) == sub_menu['label'].strip().lower():
                        # Log click statistics
                        await log_click(sub_menu['label'])

                        # Check for dynamic content
                        db_content = await get_button_content(sub_menu['label'])
                        if db_content:
                            msg_text = db_content['content']
                            photo = db_content['photo_file_id']
                            kb = None
                            if db_content['buttons_json']:
                                btns = json.loads(db_content['buttons_json'])
                                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=b['text'], url=b['url'])] for b in btns])

                            if photo:
                                await message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                            else:
                                await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                            return

                        # If this submenu has nested submenus, show them
                        if 'submenu' in sub_menu:
                            if sub_menu.get("type") == "inline":
                                # Don't add back button for first-level submenus
                                keyboard = get_inline_keyboard(
                                    sub_menu["submenu"],
                                    sub_key,
                                    add_back_button=False)
                                await message.answer(
                                    sub_menu.get('text', f"Выберите опцию:"),
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    link_preview_options=LinkPreviewOptions(
                                        is_disabled=True))
                            else:
                                keyboard = get_submenu_keyboard(
                                    current_menu, sub_key)
                                await state.update_data(current_submenu=sub_key
                                                        )
                                await message.answer(
                                    sub_menu.get('text', f"Выберите опцию:"),
                                    reply_markup=keyboard,
                                    parse_mode=ParseMode.HTML,
                                    link_preview_options=LinkPreviewOptions(
                                        is_disabled=True))
                            return

                        # If submenu has pages, show navigation
                        elif 'pages' in sub_menu:
                            text_content = sub_menu['pages'][0].get('text', '')
                            keyboard = get_nav_keyboard_inline(
                                current_menu, sub_key, 0)
                            await message.answer(
                                text_content,
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                link_preview_options=LinkPreviewOptions(
                                    is_disabled=True))
                            return

                        # Otherwise just show text
                        elif 'text' in sub_menu:
                            keyboard = get_submenu_keyboard(current_menu)
                            await message.answer(
                                sub_menu['text'],
                                reply_markup=keyboard,
                                parse_mode=ParseMode.HTML,
                                link_preview_options=LinkPreviewOptions(
                                    is_disabled=True))
                            return

    # If not recognized, show main menu
    await state.clear()
    keyboard = await get_dynamic_keyboard(message.from_user.id)
    await message.answer("Пожалуйста, используйте кнопки меню:",
                         reply_markup=keyboard)


@router.inline_query()
async def inline_query_handler(inline_query: InlineQuery):
    """Обработчик inline режима когда пользователь пишет @bot_username"""
    import re

    query = inline_query.query.lower().strip()
    results = []

    # Получаем все кнопки клавиатуры из БД
    keyboard_buttons = await get_all_keyboard_buttons()

    if not keyboard_buttons:
        # Если нет кнопок в БД, отправляем пустой результат
        await inline_query.answer([], cache_time=0, is_personal=True)
        return

    for kb_button in keyboard_buttons:
        button_label = kb_button.get('label') if isinstance(kb_button, dict) else kb_button.label

        # Поиск по названию кнопки
        if query and query not in button_label.lower():
            continue

        # Получаем контент кнопки из БД
        db_content = await get_button_content(button_label)

        if not db_content:
            continue

        # Получаем текст
        full_text = db_content.get('content', '')

        # Если есть pages_json, берём первую страницу
        if db_content.get('pages_json'):
            try:
                pages = json.loads(db_content['pages_json'])
                if pages:
                    full_text = pages[0].get('text', full_text)
            except:
                pass

        # Если текста нет, используем название
        if not full_text or not full_text.strip():
            full_text = button_label

        # Убираем HTML теги только для описания (preview)
        clean_text = re.sub(r'<[^>]+>', '', full_text)
        description = clean_text[:100] if clean_text else button_label

        # Создаем клавиатуру с инлайн-кнопками из buttons_json
        inline_keyboard_list = []

        if db_content.get('buttons_json'):
            try:
                buttons = json.loads(db_content['buttons_json'])
                button_objects = []

                for b in buttons:
                    btn_text = b.get('text', '???')

                    # Пропускаем кнопки назад
                    if b.get('url') == 'меню' or btn_text in ['🔙 Назад', '🔙 В начало']:
                        continue

                    if b.get('url'):
                        button_objects.append(InlineKeyboardButton(text=btn_text, url=b['url']))
                    else:
                        target_id = b.get('id') or f"{button_label}:{btn_text}"
                        button_objects.append(InlineKeyboardButton(text=btn_text, callback_data=make_callback_data(target_id)))

                # Группируем кнопки
                default_per_row = db_content.get('buttons_per_row', 1)
                inline_keyboard_list = group_buttons_by_row(button_objects, buttons, default_per_row)
            except Exception as e:
                print(f"[INLINE] Error parsing buttons for {button_label}: {e}")

        # Добавляем навигацию по страницам если есть
        if db_content.get('pages_json'):
            try:
                pages = json.loads(db_content['pages_json'])
                if len(pages) > 1:
                    nav_buttons = create_page_navigation_buttons(button_label, 0, len(pages))
                    inline_keyboard_list.append(nav_buttons)
            except:
                pass

        # Создаём клавиатуру
        keyboard = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list) if inline_keyboard_list else None

        # Уникальный id
        unique_id = f"{button_label}_{hash(query or 'all')}"
        if len(unique_id) > 64:
            unique_id = unique_id[:64]

        try:
            result = InlineQueryResultArticle(
                id=unique_id,
                title=button_label,
                description=description if description else button_label,
                input_message_content=InputTextMessageContent(
                    message_text=full_text,
                    parse_mode=ParseMode.HTML,
                    link_preview_options=LinkPreviewOptions(is_disabled=True)
                ),
                reply_markup=keyboard
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Error creating inline result for {button_label}: {e}")
            continue

    # Отвечаем на inline запрос
    await inline_query.answer(
        results,
        cache_time=0,
        is_personal=True
    )


# Вспомогательная функция для обработки динамических кнопок (НЕ handler!)
async def handle_dynamic_buttons(message: types.Message, state: FSMContext):
    # Принудительный вывод в консоль для отладки
    print(f"\n[BOT_DEBUG] === handle_dynamic_buttons Start ===")
    print(f"[BOT_DEBUG] Text: '{message.text}'")
    print(f"[BOT_DEBUG] User ID: {message.from_user.id}")

    label = message.text
    if not label:
        print("[BOT_DEBUG] Message has no text, skipping.")
        return False

    # Проверка режима удаления кнопок в админке
    current_state = await state.get_state()
    print(f"[BOT_DEBUG] Current State: {current_state}")

    # 1. Проверяем динамические кнопки из БД (приоритет)
    try:
        print(f"\n[BOT_DEBUG_VERBOSE] --- Step 1: DB Lookup ---")
        print(f"[BOT_DEBUG_VERBOSE] Searching content for label: '{label}'")
        # Сначала пробуем точное совпадение
        db_content = await get_button_content(label)

        # Если не нашли, пробуем найти по всем зарегистрированным кнопкам клавиатуры
        menu_key = None  # Ключ из MENU_STRUCTURE
        if not db_content:
            print(f"[BOT_DEBUG_VERBOSE] No exact match in button_content table for '{label}'")
            print(f"[BOT_DEBUG_VERBOSE] Fetching all registered keyboard labels...")
            all_btns = await get_all_keyboard_buttons()
            print(f"[BOT_DEBUG_VERBOSE] Total registered buttons in keyboard_buttons: {len(all_btns)}")
            for b in all_btns:
                b_lbl = b.get('label') if isinstance(b, dict) else (getattr(b, 'label', None) or b['label'] if hasattr(b, '__getitem__') else None)
                if b_lbl:
                    is_match = b_lbl.strip().lower() == label.strip().lower()
                    if is_match:
                        print(f"[BOT_DEBUG_VERBOSE] ✅ Found match in keyboard_buttons: '{label}' -> '{b_lbl}'")
                        menu_key = b.get('menu_key') or b_lbl  # Берём menu_key если есть
                        print(f"[BOT_DEBUG_VERBOSE] Menu key: '{menu_key}'")
                        # Ищем контент по menu_key (ключ из MENU_STRUCTURE)
                        db_content = await get_button_content(menu_key)
                        if db_content:
                            print(f"[BOT_DEBUG_VERBOSE] Successfully fetched content for menu_key '{menu_key}'")
                        break
                    else:
                        # Log non-matches only in very verbose mode or skip
                        pass

        if db_content:
            print(f"[BOT_DEBUG_VERBOSE] ✅ SUCCESS: Found DB entry for '{label}'")
            print(f"[BOT_DEBUG_VERBOSE] DB Button ID: '{db_content.get('button_id')}'")
            print(f"[BOT_DEBUG_VERBOSE] DB Parent ID: '{db_content.get('parent_id')}'")
            print(f"[BOT_DEBUG_VERBOSE] Content length: {len(db_content.get('content', ''))}")

            btn_id = db_content.get('button_id') or label
            await log_click(btn_id)

            msg_text = db_content.get('content', '')
            photo = db_content.get('photo_file_id')
            kb = None
            inline_keyboard_list = []

            if db_content.get('buttons_json'):
                print(f"[BOT_DEBUG_VERBOSE] Found inline buttons JSON: {db_content['buttons_json']}")
                try:
                    btns = json.loads(db_content['buttons_json'])
                    print(f"[BOT_DEBUG_VERBOSE] Parsed {len(btns)} inline buttons")

                    # Получаем настройку расположения (старая система, используется как дефолт)
                    default_buttons_per_row = db_content.get('buttons_per_row', 1)
                    print(f"[BOT_DEBUG_VERBOSE] Default buttons per row: {default_buttons_per_row}")

                    # Создаём список кнопок
                    button_objects = []
                    has_back_button = False  # Отслеживаем наличие кнопки назад в buttons_json

                    for i, b in enumerate(btns):
                        btn_text = b.get('text', '???')
                        row_width = b.get('row_width', default_buttons_per_row)
                        print(f"[BOT_DEBUG_VERBOSE] Button {i+1}: '{btn_text}' (row_width={row_width})")

                        # Проверяем на кнопку назад из миграции (url='меню')
                        if b.get('url') == 'меню' or btn_text in ['🔙 Назад', '🔙 В начало']:
                            has_back_button = True
                            print(f"[BOT_DEBUG_VERBOSE] -> Found back button in buttons_json: '{btn_text}', skipping (will add based on parent_id)")
                            continue  # Пропускаем старые кнопки назад

                        if b.get('url'):
                            print(f"[BOT_DEBUG_VERBOSE] -> URL: {b['url']}")
                            button_objects.append(InlineKeyboardButton(text=btn_text, url=b['url']))
                        else:
                            target_id = b.get('id') or f"{btn_id}:{btn_text}"
                            print(f"[BOT_DEBUG_VERBOSE] -> Submenu ID: {target_id}")
                            button_objects.append(InlineKeyboardButton(text=btn_text, callback_data=make_callback_data(target_id)))

                    # Группируем кнопки с учётом индивидуальной ширины
                    inline_keyboard_list = group_buttons_by_row(button_objects, btns, default_buttons_per_row)

                except Exception as e:
                    print(f"[BOT_DEBUG_VERBOSE] ❌ ERROR parsing buttons_json: {e}")
            else:
                print(f"[BOT_DEBUG_VERBOSE] No buttons_json (no inline buttons from buttons)")

            # Проверяем pages_json независимо от buttons_json
            if db_content.get('pages_json'):
                try:
                    pages = json.loads(db_content['pages_json'])
                    if len(pages) > 1:
                        # Добавляем кнопки навигации для первой страницы
                        nav_buttons = create_page_navigation_buttons(btn_id, 0, len(pages))
                        inline_keyboard_list.append(nav_buttons)
                        print(f"[BOT_DEBUG_VERBOSE] Added page navigation: {len(pages)} pages")
                except Exception as e:
                    print(f"[BOT_DEBUG_VERBOSE] Error adding page navigation: {e}")

            # Добавляем кнопку назад только если есть parent_id (не первый уровень)
            if db_content.get('parent_id'):
                parent_id = db_content['parent_id']
                print(f"[BOT_DEBUG_VERBOSE] Adding 'Back' button to parent: '{parent_id}'")
                inline_keyboard_list.append([InlineKeyboardButton(text="🔙 Назад", callback_data=make_callback_data(parent_id))])
            else:
                print(f"[BOT_DEBUG_VERBOSE] No parent_id (first level menu), no back button needed")

            # Создаем клавиатуру только если есть кнопки
            if inline_keyboard_list:
                kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list)
                print(f"[BOT_DEBUG_VERBOSE] Created keyboard with {len(inline_keyboard_list)} rows")

            if photo:
                print(f"[BOT_DEBUG_VERBOSE] Sending Photo response (File ID: {photo[:15]}...)")
                await message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
            else:
                print(f"[BOT_DEBUG_VERBOSE] Sending Text response")
                await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML,
                                   link_preview_options=LinkPreviewOptions(is_disabled=True))
            return True
        else:
            print(f"[BOT_DEBUG_VERBOSE] ❌ FAIL: Button '{label}' not found in button_content table after all attempts")
            # Если есть menu_key, попробуем найти в MENU_STRUCTURE
            if menu_key:
                print(f"[BOT_DEBUG_VERBOSE] Trying to find in MENU_STRUCTURE by menu_key: '{menu_key}'")
    except Exception as e:
        print(f"[BOT_DEBUG_VERBOSE] ❌ CRITICAL ERROR in handle_dynamic_buttons: {e}")
        import traceback
        traceback.print_exc()

    # 2. Проверяем статическую структуру меню
    print(f"[BOT_DEBUG] Step 2: Checking static menu structure")

    # Используем menu_key если есть, иначе label
    search_key = menu_key if menu_key else label
    print(f"[BOT_DEBUG] Search key: '{search_key}'")

    for key, item in MENU_STRUCTURE.items():
        # Ищем по ключу (menu_key) или по label
        if key == search_key or label.strip().lower() == item.get('label', '').strip().lower():
            print(f"[BOT_DEBUG] Found static match: {key}")
            await log_click(item.get('label'))

            # Проверяем есть ли override в БД
            db_content = await get_button_content(key)

            # Определяем текст: из БД если есть, иначе из статики
            if db_content and db_content.get('content'):
                msg_text = db_content['content']
                print(f"[BOT_DEBUG] Using text from DB override")
            elif 'pages' in item:
                msg_text = item['pages'][0]['text']
                print(f"[BOT_DEBUG] Using text from static pages")
            else:
                msg_text = item['text']
                print(f"[BOT_DEBUG] Using text from static item")

            # Собираем инлайн-кнопки из обоих источников
            inline_keyboard_list = []

            # 1. Сначала добавляем кнопки из MENU_STRUCTURE
            if item.get('type') == 'inline' and item.get('submenu'):
                for skey, sub in item.get('submenu', {}).items():
                    inline_keyboard_list.append([
                        InlineKeyboardButton(text=sub['label'], callback_data=f"inline_{key}:{skey}")
                    ])

            # 2. Затем добавляем кнопки из БД (если есть)
            if db_content and db_content.get('buttons_json'):
                print(f"[BOT_DEBUG] Found DB inline buttons for static menu '{key}'")
                try:
                    btns = json.loads(db_content['buttons_json'])
                    for b in btns:
                        btn_text = b.get('text', '???')
                        if b.get('url') and b.get('url') != 'меню':
                            inline_keyboard_list.append([InlineKeyboardButton(text=btn_text, url=b['url'])])
                        else:
                            target_id = b.get('id') or f"{key}:{btn_text}"
                            inline_keyboard_list.append([InlineKeyboardButton(text=btn_text, callback_data=make_callback_data(target_id))])
                except Exception as e:
                    print(f"[BOT_DEBUG] Error parsing DB buttons for static menu: {e}")

            # Формируем ответ
            if inline_keyboard_list:
                kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list)
                await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML,
                                   link_preview_options=LinkPreviewOptions(is_disabled=True))
            elif 'pages' in item:
                kb = get_nav_keyboard_inline(key, "", 0)
                await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML,
                                   link_preview_options=LinkPreviewOptions(is_disabled=True))
            else:
                await message.answer(msg_text, parse_mode=ParseMode.HTML,
                                   link_preview_options=LinkPreviewOptions(is_disabled=True))
            return True

    print(f"[BOT_DEBUG] === handle_dynamic_buttons End (No Match) ===")
    return False

    # 3. Резервный поиск по всем кнопкам БД
    try:
        dynamic_btns = await get_all_keyboard_buttons()
        labels = [b['label'] for b in dynamic_btns]
        if label in labels:
            db_content = await get_button_content(label)
            if db_content:
                 # Повтор логики отображения
                 msg_text = db_content['content']
                 photo = db_content['photo_file_id']
                 kb = None
                 if db_content['buttons_json']:
                     try:
                         btns = json.loads(db_content['buttons_json'])
                         inline_kb = []
                         for b in btns:
                             if b.get('url') and b.get('url') != 'меню':
                                 inline_kb.append([InlineKeyboardButton(text=b['text'], url=b['url'])])
                             else:
                                 b_id = b.get('id') or f"{label}:{b['text']}"
                                 inline_kb.append([InlineKeyboardButton(text=b['text'], callback_data=make_callback_data(b_id))])
                         kb = InlineKeyboardMarkup(inline_keyboard=inline_kb)
                     except: pass

                 if photo:
                     await message.answer_photo(photo, caption=msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                 else:
                     await message.answer(msg_text, reply_markup=kb, parse_mode=ParseMode.HTML)
                 return True
    except Exception as e:
        logger.error(f"Error in handle_dynamic_buttons (fallback search): {e}")
    return False

async def main():
    print("Starting bot...")
    await init_db()
    load_chats_continuation()

    # Один главный обработчик для всего текста
    router.message.register(handle_all_text_messages, F.text)

    # Register handlers
    dp.include_router(router)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())