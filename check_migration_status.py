#!/usr/bin/env python3
"""
Проверяет результаты миграции
"""

import asyncio
import database
from database import init_db

async def check():
    await init_db()

    async with database.pool.acquire() as conn:
        print("=" * 60)
        print("ПРОВЕРКА ПОСЛЕ МИГРАЦИИ")
        print("=" * 60)

        # 1. Проверяем Терминологию
        print("\n📖 ТЕРМИНОЛОГИЯ:")
        term = await conn.fetchrow("""
            SELECT button_id, parent_id, short_id,
                   LENGTH(pages_json) as pages_len
            FROM button_content
            WHERE button_id = '📖 Урок Терминологии'
        """)
        if term:
            print(f"  ✓ Найдена")
            print(f"    parent_id: {term['parent_id']}")
            print(f"    short_id: {term['short_id']}")
            print(f"    pages_json: {term['pages_len']} байт")
            if term['pages_len']:
                print(f"    ✅ Есть pages_json")
            else:
                print(f"    ❌ НЕТ pages_json!")
        else:
            print("  ❌ НЕ НАЙДЕНА!")

        # 2. Проверяем проблемные кнопки
        print("\n🔍 ПРОБЛЕМНЫЕ КНОПКИ (должны иметь parent_id):")
        problem_buttons = [
            "📚 Полезные сайты:Написать статью",
            "📚 Полезные сайты:Телеграм",
            "🤖 Различные боты:Для чатов",
            "🤖 Различные боты:Авто-Постинг",
            "🤖 Различные боты:Сервисы аналитики",
            "🛡 Garant Checker:ℹ️ Информация",
            "🛡 Garant Checker:ℹ️ Информация:📺 БИРЖИ КАНАЛОВ",
        ]

        for btn_id in problem_buttons:
            btn = await conn.fetchrow("""
                SELECT button_id, parent_id, short_id
                FROM button_content
                WHERE button_id = $1
            """, btn_id)

            if btn:
                has_parent = btn['parent_id'] is not None
                has_short = btn['short_id'] is not None
                status = "✅" if (has_parent and has_short) else "❌"
                print(f"\n  {status} {btn_id}")
                print(f"      parent_id: {btn['parent_id']}")
                print(f"      short_id: {btn['short_id']}")

                if not has_parent:
                    print(f"      ⚠️  ПРОБЛЕМА: нет parent_id!")
                if not has_short:
                    print(f"      ⚠️  ПРОБЛЕМА: нет short_id!")
            else:
                print(f"\n  ❌ {btn_id}")
                print(f"      НЕ НАЙДЕНА В БД!")

        # 3. Проверяем кнопку которая РАБОТАЕТ
        print("\n\n✅ КНОПКА КОТОРАЯ РАБОТАЕТ (для сравнения):")
        work_btns = [
            "🔍 Сервисы аналитики:TGStat",
            "🔍 Сервисы аналитики:Telemetr",
        ]

        for btn_id in work_btns:
            btn = await conn.fetchrow("""
                SELECT button_id, parent_id, short_id
                FROM button_content
                WHERE button_id = $1
            """, btn_id)

            if btn:
                print(f"\n  ✓ {btn_id}")
                print(f"      parent_id: {btn['parent_id']}")
                print(f"      short_id: {btn['short_id']}")

        # 4. Общая статистика
        print("\n" + "=" * 60)
        print("СТАТИСТИКА:")
        total = await conn.fetchval("SELECT COUNT(*) FROM button_content")
        with_parent = await conn.fetchval("SELECT COUNT(*) FROM button_content WHERE parent_id IS NOT NULL")
        with_short = await conn.fetchval("SELECT COUNT(*) FROM button_content WHERE short_id IS NOT NULL")
        with_pages = await conn.fetchval("SELECT COUNT(*) FROM button_content WHERE pages_json IS NOT NULL")

        print(f"  Всего кнопок: {total}")
        print(f"  С parent_id: {with_parent}")
        print(f"  С short_id: {with_short}")
        print(f"  С pages_json: {with_pages}")

        if with_short < total:
            print(f"\n  ⚠️  {total - with_short} кнопок БЕЗ short_id - нужна повторная миграция!")
        if with_pages == 0:
            print(f"\n  ⚠️  НЕТ кнопок с pages_json - Терминология не мигрирована!")

        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(check())
