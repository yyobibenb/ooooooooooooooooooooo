#!/usr/bin/env python3
"""
Проверяет и исправляет parent_id для всех кнопок в БД.
Запускает migrate_full_menu.py для полной миграции.
"""

import asyncio
import sys
from database import init_db, pool

async def check_database():
    """Проверяет состояние БД"""
    print("🔍 Проверка базы данных...\n")

    await init_db()

    async with pool.acquire() as conn:
        # Получаем все кнопки
        all_buttons = await conn.fetch("""
            SELECT button_id, parent_id,
                   LENGTH(buttons_json) as buttons_len,
                   LENGTH(pages_json) as pages_len
            FROM button_content
            ORDER BY button_id
        """)

        print(f"📊 Всего кнопок в БД: {len(all_buttons)}\n")

        # Проверяем проблемные кнопки
        problematic = []
        for btn in all_buttons:
            button_id = btn['button_id']
            parent_id = btn['parent_id']

            # Если ID содержит ":", значит это вложенная кнопка и должен быть parent_id
            if ':' in button_id:
                # Вычисляем ожидаемый parent_id
                parts = button_id.rsplit(':', 1)
                expected_parent = parts[0]

                if parent_id != expected_parent:
                    problematic.append({
                        'button_id': button_id,
                        'current_parent': parent_id,
                        'expected_parent': expected_parent
                    })
                    print(f"❌ {button_id}")
                    print(f"   Текущий parent_id: {parent_id}")
                    print(f"   Ожидается: {expected_parent}\n")
                else:
                    print(f"✅ {button_id} (parent: {parent_id})")
            else:
                # Кнопка верхнего уровня - parent_id должен быть None
                if parent_id is None:
                    print(f"✅ {button_id} (корневая кнопка)")
                else:
                    problematic.append({
                        'button_id': button_id,
                        'current_parent': parent_id,
                        'expected_parent': None
                    })
                    print(f"❌ {button_id}")
                    print(f"   Текущий parent_id: {parent_id}")
                    print(f"   Ожидается: None\n")

        print(f"\n{'='*60}")
        if problematic:
            print(f"❌ Найдено проблем: {len(problematic)}")
            print(f"\n💡 Решение: Запустите migrate_full_menu.py чтобы исправить:")
            print(f"   py migrate_full_menu.py")
        else:
            print(f"✅ Все parent_id установлены правильно!")
        print(f"{'='*60}\n")

if __name__ == "__main__":
    try:
        asyncio.run(check_database())
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        print(f"\n💡 Убедитесь что PostgreSQL запущен и база данных доступна")
        sys.exit(1)
