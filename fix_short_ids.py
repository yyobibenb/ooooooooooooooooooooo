#!/usr/bin/env python3
"""
Обновляет все записи в БД и добавляет short_id
"""

import asyncio
from database import init_db, pool, generate_short_id

async def fix_all_short_ids():
    await init_db()

    async with pool.acquire() as conn:
        # Получаем все кнопки без short_id
        buttons = await conn.fetch("""
            SELECT button_id FROM button_content
            WHERE short_id IS NULL OR short_id = ''
        """)

        print(f"🔧 Найдено {len(buttons)} кнопок без short_id")
        print("Добавляю short_id...\n")

        for btn in buttons:
            button_id = btn['button_id']
            short_id = generate_short_id(button_id)

            await conn.execute("""
                UPDATE button_content
                SET short_id = $1
                WHERE button_id = $2
            """, short_id, button_id)

            print(f"  ✓ {button_id[:50]}... -> {short_id}")

        print(f"\n✅ Готово! Обновлено {len(buttons)} записей")

        # Проверяем
        total = await conn.fetchval("SELECT COUNT(*) FROM button_content")
        with_short = await conn.fetchval("SELECT COUNT(*) FROM button_content WHERE short_id IS NOT NULL")

        print(f"\nСтатистика:")
        print(f"  Всего кнопок: {total}")
        print(f"  С short_id: {with_short}")

        if with_short == total:
            print(f"\n🎉 Все кнопки имеют short_id!")
        else:
            print(f"\n⚠️  Еще {total - with_short} кнопок без short_id")

if __name__ == "__main__":
    asyncio.run(fix_all_short_ids())
