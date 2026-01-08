#!/usr/bin/env python3
"""
Файл для добавления функционала листания страниц в bot.py

Добавляет:
1. Обработчик callback'ов для навигации по страницам
2. Функцию для создания кнопок навигации
3. Обновление показа контента с поддержкой pages
"""

import json

# ===== КОД ДЛЯ ВСТАВКИ В bot.py =====

# 1. Добавить эту функцию после group_buttons_by_row():

def create_page_navigation_buttons(button_id, current_page, total_pages):
    """
    Создаёт кнопки навигации для многостраничного контента

    Args:
        button_id: ID кнопки (для callback_data)
        current_page: Текущая страница (0-indexed)
        total_pages: Общее количество страниц

    Returns:
        List[InlineKeyboardButton]: Список кнопок навигации
    """
    buttons = []

    # Кнопка "Назад" если не первая страница
    if current_page > 0:
        buttons.append(
            InlineKeyboardButton(
                text="◀️ Назад",
                callback_data=f"page:{button_id}:{current_page - 1}"
            )
        )

    # Индикатор страницы (некликабельная кнопка)
    buttons.append(
        InlineKeyboardButton(
            text=f"📄 {current_page + 1}/{total_pages}",
            callback_data=f"page_info:{button_id}:{current_page}"
        )
    )

    # Кнопка "Вперёд" если не последняя страница
    if current_page < total_pages - 1:
        buttons.append(
            InlineKeyboardButton(
                text="▶️ Далее",
                callback_data=f"page:{button_id}:{current_page + 1}"
            )
        )

    return buttons


# 2. Добавить этот обработчик где-то после @router.callback_query(F.data.startswith("dyn:")):

@router.callback_query(F.data.startswith("page:"))
async def handle_page_navigation(query: types.CallbackQuery):
    """Обработчик навигации по страницам"""
    try:
        # Парсим callback_data: "page:button_id:page_num"
        parts = query.data.split(":", 2)
        if len(parts) != 3:
            await query.answer("Ошибка навигации")
            return

        button_id = parts[1]
        page_num = int(parts[2])

        print(f"[PAGES] Navigating to page {page_num} of '{button_id}'")

        # Получаем контент из БД
        db_content = await get_button_content(button_id)

        if not db_content or not db_content.get('pages_json'):
            await query.answer("❌ Страницы не найдены")
            return

        # Парсим страницы
        pages = json.loads(db_content['pages_json'])

        if page_num < 0 or page_num >= len(pages):
            await query.answer("❌ Неверный номер страницы")
            return

        # Текст нужной страницы
        page_text = pages[page_num].get('text', 'Нет текста')

        # Создаём клавиатуру с кнопками навигации
        keyboard = []

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
                        button_objects.append(InlineKeyboardButton(text=btn_text, callback_data=f"dyn:{target_id}"))

                # Группируем кнопки по рядам
                default_per_row = db_content.get('buttons_per_row', 1)
                keyboard = group_buttons_by_row(button_objects, btns, default_per_row)
            except Exception as e:
                print(f"[PAGES] Error parsing buttons_json: {e}")

        # Добавляем кнопки навигации по страницам
        nav_buttons = create_page_navigation_buttons(button_id, page_num, len(pages))
        keyboard.append(nav_buttons)

        # Кнопка "Назад" к родителю (если есть)
        if db_content.get('parent_id'):
            parent_id = db_content['parent_id']
            keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"dyn:{parent_id}")])

        kb = InlineKeyboardMarkup(inline_keyboard=keyboard)

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


# 3. Обновить обработчик process_dynamic_inline() - добавить после строки с kb = InlineKeyboardMarkup():

# В функции process_dynamic_inline(), после создания kb из buttons_json,
# но ПЕРЕД добавлением кнопки "Назад", добавить:

# Если есть pages_json, показываем навигацию по страницам
if db_content.get('pages_json'):
    try:
        pages = json.loads(db_content['pages_json'])
        if len(pages) > 1:
            # Добавляем кнопки навигации
            nav_buttons = create_page_navigation_buttons(button_id, 0, len(pages))
            inline_keyboard_list.append(nav_buttons)
            print(f"[BOT_DEBUG_VERBOSE] Added page navigation: {len(pages)} pages")
    except Exception as e:
        print(f"[BOT_DEBUG_VERBOSE] Error adding page navigation: {e}")


print("""
==============================================
✅ Код для добавления листания страниц готов!

ИНСТРУКЦИЯ ПО УСТАНОВКЕ:

1. Откройте bot.py

2. Найдите функцию group_buttons_by_row() (примерно строка 130)
   После неё вставьте функцию create_page_navigation_buttons()

3. Найдите обработчик @router.callback_query(F.data.startswith("dyn:"))
   После него вставьте обработчик @router.callback_query(F.data.startswith("page:"))

4. В функции process_dynamic_inline() (строка ~560):
   - Найдите место где создаётся kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard_list)
   - ПЕРЕД добавлением кнопки "Назад" вставьте код проверки pages_json

5. Сохраните bot.py

6. Запустите миграцию чтобы сохранить pages в БД:
   py migrate_menu_to_db.py

7. Запустите бота:
   py bot.py

Готово! Теперь в "Терминологии" будут кнопки ◀️ Назад и ▶️ Далее
==============================================
""")
