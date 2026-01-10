#!/usr/bin/env python3
"""
Скрипт для очистки настроек /start из базы данных
"""
import asyncio
import database

async def clear_start_settings():
    """Удаляет настройки start_text и start_photo_file_id из БД"""
    print("🔄 Подключение к базе данных...")
    await database.init_db()

    # Получаем pool
    pool = database.pool
    if not pool:
        print("❌ Не удалось подключиться к БД")
        return

    async with pool.acquire() as conn:
        # Удаляем start_text
        result1 = await conn.execute(
            "DELETE FROM settings WHERE key = 'start_text'"
        )
        print(f"✅ Удалено start_text (изменено строк: {result1.split()[-1]})")

        # Удаляем start_photo_file_id
        result2 = await conn.execute(
            "DELETE FROM settings WHERE key = 'start_photo_file_id'"
        )
        print(f"✅ Удалено start_photo_file_id (изменено строк: {result2.split()[-1]})")

    await database.close_pool()
    print("\n✅ Настройки /start очищены из БД")
    print("ℹ️  Теперь будет использоваться дефолтный текст из кода")

if __name__ == "__main__":
    asyncio.run(clear_start_settings())
