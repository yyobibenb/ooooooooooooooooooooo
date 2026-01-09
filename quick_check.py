#!/usr/bin/env python3
import asyncio
import database
from database import init_db

async def quick_check():
    await init_db()
    async with database.pool.acquire() as conn:
        result = await conn.fetchrow("""
            SELECT
                button_id,
                LENGTH(pages_json) as pages_len,
                short_id
            FROM button_content
            WHERE button_id = '📖 Урок Терминологии'
        """)

        print("=" * 60)
        print("ТЕРМИНОЛОГИЯ В БД:")
        print("=" * 60)
        if result:
            print(f"button_id: {result['button_id']}")
            print(f"pages_json длина: {result['pages_len']} байт")
            print(f"short_id: {result['short_id']}")

            if result['pages_len'] and result['pages_len'] > 0:
                print("\n✅ pages_json ЕСТЬ в БД")
            else:
                print("\n❌ pages_json ОТСУТСТВУЕТ в БД - нужно запустить complete_migration_fix.py")

            if result['short_id']:
                print("✅ short_id ЕСТЬ")
            else:
                print("❌ short_id ОТСУТСТВУЕТ")
        else:
            print("❌ Терминология НЕ НАЙДЕНА в БД!")
        print("=" * 60)

asyncio.run(quick_check())
