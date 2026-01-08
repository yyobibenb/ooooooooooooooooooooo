#!/usr/bin/env python3
"""
Скрипт миграции меню из MENU_STRUCTURE в базу данных.
Переносит все кнопки клавиатуры, тексты, инлайн-кнопки и вложенные подменю в БД.
"""

import asyncio
import json
from bot import MENU_STRUCTURE
from database import init_db, add_keyboard_button, update_button_content

async def migrate_submenu(menu_id, menu_data, parent_id=None, full_path=""):
    """
    Рекурсивно мигрирует подменю в БД.

    Args:
        menu_id: ID меню (используется как button_id в БД)
        menu_data: Данные меню из MENU_STRUCTURE
        parent_id: ID родительского меню (для кнопки "Назад")
        full_path: Полный путь в иерархии (для уникальности ID)
    """
    # Определяем текст контента и pages
    pages_json = None
    if 'pages' in menu_data and menu_data['pages']:
        # Многостраничное меню - берём первую страницу для content, все для pages_json
        text_content = menu_data['pages'][0].get('text', '')
        # Сохраняем все страницы в JSON
        pages_json = json.dumps([{'text': page.get('text', '')} for page in menu_data['pages']])
    else:
        text_content = menu_data.get('text', '')

    # Собираем инлайн-кнопки
    buttons = []

    # Кнопки из submenu
    if menu_data.get('type') == 'inline' and menu_data.get('submenu'):
        for submenu_key, submenu_data in menu_data['submenu'].items():
            if isinstance(submenu_data, dict):
                # Создаём уникальный ID: ВСЕГДА parent:child для consistency
                submenu_label = submenu_data.get('label', submenu_key)
                submenu_full_id = f"{menu_id}:{submenu_label}"

                # Обычное подменю
                buttons.append({
                    'text': submenu_label,
                    'id': submenu_full_id
                })
            else:
                print(f"[WARNING] Unexpected submenu data for {submenu_key}: {submenu_data}")

    # Кнопки из buttons массива
    if 'buttons' in menu_data:
        for btn in menu_data['buttons']:
            if btn.get('url'):
                buttons.append({
                    'text': btn['text'],
                    'url': btn['url']
                })
            elif btn.get('callback'):
                # Кнопки с callback (например "Назад")
                buttons.append({
                    'text': btn['text'],
                    'url': 'меню'  # Специальное значение для кнопки назад
                })

    # Если есть URL в самом меню (для кнопок-ссылок)
    if menu_data.get('url'):
        # Это кнопка-ссылка, не создаём контент
        print(f"[INFO] Skipping URL button: {menu_id}")
        return

    # Сохраняем контент
    buttons_json = json.dumps(buttons) if buttons else None

    success = await update_button_content(
        menu_id,
        text_content,
        None,  # photo_file_id
        buttons_json,
        'HTML',
        parent_id,
        None,  # buttons_per_row
        pages_json  # pages_json
    )

    if success:
        print(f"✅ Migrated: {menu_id} (parent: {parent_id})")
    else:
        print(f"❌ Failed to migrate: {menu_id}")

    # Рекурсивно мигрируем вложенные подменю
    if menu_data.get('submenu'):
        for submenu_key, submenu_data in menu_data['submenu'].items():
            if isinstance(submenu_data, dict) and not submenu_data.get('url'):
                # Создаём полный путь для вложенного меню: ВСЕГДА parent:child
                submenu_label = submenu_data.get('label', submenu_key)
                submenu_full_id = f"{menu_id}:{submenu_label}"
                new_full_path = f"{full_path}/{submenu_key}" if full_path else submenu_key

                await migrate_submenu(
                    submenu_full_id,
                    submenu_data,
                    parent_id=menu_id,
                    full_path=new_full_path
                )

async def migrate_all():
    """Мигрирует всё меню из MENU_STRUCTURE в БД"""
    print("🚀 Starting migration from MENU_STRUCTURE to database...")
    print(f"📊 Total top-level menu items: {len(MENU_STRUCTURE)}")

    # Инициализируем БД
    await init_db()

    # Позиция для кнопок клавиатуры (по 2 в ряду)
    row_index = 0
    col_index = 0

    # Мигрируем каждую кнопку верхнего уровня
    for menu_key, menu_data in MENU_STRUCTURE.items():
        label = menu_data.get('label', menu_key)

        print(f"\n📝 Processing: {label} ({menu_key})")

        # 1. Добавляем кнопку клавиатуры (используем label как menu_key для совместимости)
        await add_keyboard_button(label, row=row_index, col=col_index, menu_key=label)
        print(f"   ✅ Added keyboard button: {label}")

        # 2. Мигрируем контент и подменю (используем label как button_id)
        await migrate_submenu(label, menu_data, parent_id=None)

        # Обновляем позицию (по 2 кнопки в ряду)
        col_index += 1
        if col_index >= 2:
            col_index = 0
            row_index += 1

    print("\n✅ Migration completed successfully!")
    print("\n📋 Summary:")
    print(f"   • Keyboard buttons created: {len(MENU_STRUCTURE)}")
    print(f"   • All content and submenus migrated to database")
    print("\n💡 Now the bot will load everything from the database!")

if __name__ == "__main__":
    asyncio.run(migrate_all())
