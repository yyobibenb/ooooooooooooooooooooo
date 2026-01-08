import asyncpg
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

DATABASE_URL = "postgresql://postgres:yobibenb@localhost:5432/o"

pool = None

async def init_db():
    """Initialize database and create tables"""
    global pool
    try:
        # Load DB URL
        db_url = "postgresql://postgres:yobibenb@localhost:5432/o"
        print(f"✅ Подключение к локальной БД: {db_url.split('@')[-1]}")

        try:
            pool = await asyncpg.create_pool(
                db_url, 
                min_size=1, 
                max_size=10
            )
            print("✓ Подключение успешно")
        except Exception as e:
            print(f"❌ Ошибка подключения к локальной БД: {e}")
            return

        async with pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    broadcast_id SERIAL PRIMARY KEY,
                    admin_id BIGINT NOT NULL,
                    text_content TEXT,
                    photo_file_id VARCHAR(500),
                    buttons_json TEXT,
                    parse_mode VARCHAR(20) DEFAULT 'HTML',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_recipients (
                    broadcast_id INT NOT NULL,
                    user_id BIGINT NOT NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (broadcast_id, user_id)
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS statistics (
                    button_name VARCHAR(255) PRIMARY KEY,
                    click_count BIGINT DEFAULT 0
                )
            ''')

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS button_content (
                    button_id VARCHAR(255) PRIMARY KEY,
                    content TEXT NOT NULL,
                    photo_file_id VARCHAR(500),
                    buttons_json TEXT,
                    parse_mode VARCHAR(20) DEFAULT 'HTML',
                    parent_id VARCHAR(255),
                    pages_json TEXT
                )
            ''')

            # Migration: add parent_id if it doesn't exist
            try:
                await conn.execute('ALTER TABLE button_content ADD COLUMN IF NOT EXISTS parent_id VARCHAR(255)')
            except:
                pass

            # Migration: add buttons_per_row if it doesn't exist
            try:
                await conn.execute('ALTER TABLE button_content ADD COLUMN IF NOT EXISTS buttons_per_row INT DEFAULT 1')
            except:
                pass

            # Migration: add pages_json if it doesn't exist
            try:
                await conn.execute('ALTER TABLE button_content ADD COLUMN IF NOT EXISTS pages_json TEXT')
            except:
                pass

            await conn.execute('''
                CREATE TABLE IF NOT EXISTS keyboard_buttons (
                    id SERIAL PRIMARY KEY,
                    label VARCHAR(255) UNIQUE NOT NULL,
                    menu_key VARCHAR(255),
                    row_index INT DEFAULT 0,
                    col_index INT DEFAULT 0
                )
            ''')

            # Migration: add menu_key if it doesn't exist
            try:
                await conn.execute('ALTER TABLE keyboard_buttons ADD COLUMN IF NOT EXISTS menu_key VARCHAR(255)')
            except:
                pass

        print("✓ Database initialized successfully")
    except Exception as e:
        print(f"✗ Database initialization error: {e}")
        raise

async def get_pool():
    """Get the connection pool"""
    return pool

async def close_pool():
    """Close the connection pool"""
    global pool
    if pool:
        await pool.close()

async def add_user(user_id, username, first_name, last_name):
    """Add or update a user"""
    if pool is None:
        print("Database pool not initialized. Skipping add_user.")
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id, username, first_name, last_name)
    except Exception as e:
        print(f"Error adding user: {e}")

async def log_click(button_name):
    """Log a button click"""
    if pool is None:
        print("Database pool not initialized. Skipping log_click.")
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO statistics (button_name, click_count)
                VALUES ($1, 1)
                ON CONFLICT (button_name) DO UPDATE SET click_count = statistics.click_count + 1
            ''', button_name)
    except Exception as e:
        print(f"Error logging click: {e}")

async def get_stats():
    """Get overall statistics"""
    if pool is None:
        print("Database pool not initialized. Returning empty stats.")
        return {'user_count': 0, 'clicks': []}
    try:
        async with pool.acquire() as conn:
            user_count = await conn.fetchval('SELECT COUNT(*) FROM users')
            clicks = await conn.fetch('SELECT button_name, click_count FROM statistics ORDER BY click_count DESC LIMIT 10')
            return {
                'user_count': user_count,
                'clicks': clicks
            }
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {'user_count': 0, 'clicks': []}

async def get_all_users():
    """Get all users"""
    if pool is None:
        print("Database pool not initialized. Returning empty users list.")
        return []
    try:
        async with pool.acquire() as conn:
            users = await conn.fetch('SELECT user_id FROM users')
            return [user['user_id'] for user in users]
    except Exception as e:
        print(f"Error getting users: {e}")
        return []

async def save_broadcast(admin_id, text_content, photo_file_id, buttons_json, parse_mode):
    """Save a broadcast"""
    if pool is None:
        print("Database pool not initialized. Skipping save_broadcast.")
        return None
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchval('''
                INSERT INTO broadcasts (admin_id, text_content, photo_file_id, buttons_json, parse_mode)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING broadcast_id
            ''', admin_id, text_content, photo_file_id, buttons_json, parse_mode)
            return result
    except Exception as e:
        print(f"Error saving broadcast: {e}")
        return None

async def update_button_content(button_id, content, photo_file_id=None, buttons_json=None, parse_mode='HTML', parent_id=None, buttons_per_row=None, pages_json=None):
    """Update or insert button content"""
    if pool is None:
        print("Database pool not initialized. Skipping update_button_content.")
        return False
    try:
        # Используем print для гарантированного вывода в консоль на ПК
        print(f"\n[DB_DEBUG] === Saving Button Content ===")
        print(f"[DB_DEBUG] Button ID: '{button_id}'")
        print(f"[DB_DEBUG] Content: '{content[:50]}...'")
        print(f"[DB_DEBUG] Photo: {photo_file_id}")
        print(f"[DB_DEBUG] Parent: {parent_id}")
        print(f"[DB_DEBUG] Buttons JSON: {buttons_json}")
        print(f"[DB_DEBUG] Buttons per row: {buttons_per_row}")
        print(f"[DB_DEBUG] Pages: {len(json.loads(pages_json)) if pages_json else 0} pages")

        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO button_content (button_id, content, photo_file_id, buttons_json, parse_mode, parent_id, buttons_per_row, pages_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (button_id) DO UPDATE SET
                    content = EXCLUDED.content,
                    photo_file_id = EXCLUDED.photo_file_id,
                    buttons_json = EXCLUDED.buttons_json,
                    parse_mode = EXCLUDED.parse_mode,
                    parent_id = EXCLUDED.parent_id,
                    buttons_per_row = EXCLUDED.buttons_per_row,
                    pages_json = EXCLUDED.pages_json
            ''', button_id, content, photo_file_id, buttons_json, parse_mode, parent_id, buttons_per_row, pages_json)
            print(f"[DB_DEBUG] ✅ Saved successfully to button_content")
            return True
    except Exception as e:
        print(f"[DB_DEBUG] ❌ Error saving content for '{button_id}': {e}")
        return False

async def get_button_content(button_id):
    """Get content for a specific button"""
    if pool is None:
        print("Database pool not initialized. Returning None for button content.")
        return None
    try:
        print(f"\n[DB_DEBUG] === get_button_content ===")
        print(f"[DB_DEBUG] Looking for button_id: '{button_id}'")
        
        async with pool.acquire() as conn:
            res = await conn.fetchrow('SELECT * FROM button_content WHERE button_id = $1', button_id)
            if res:
                print(f"[DB_DEBUG] Found content in DB for button '{button_id}': {dict(res)}")
                return dict(res)
            else:
                print(f"[DB_DEBUG] No content found in DB for button '{button_id}'")
            return res
    except Exception as e:
        print(f"[DB_DEBUG] ❌ Error fetching content for '{button_id}': {e}")
        return None

async def get_all_keyboard_buttons():
    """Get all keyboard buttons ordered by row and column"""
    if pool is None:
        print("Database pool not initialized. Returning empty keyboard buttons list.")
        return []
    try:
        async with pool.acquire() as conn:
            # Возвращаем список словарей для консистентности
            rows = await conn.fetch('SELECT label, menu_key FROM keyboard_buttons ORDER BY row_index, col_index')
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB_DEBUG] ❌ Error getting keyboard buttons: {e}")
        return []

async def add_keyboard_button(label, row=0, col=0, menu_key=None):
    """Add a new keyboard button"""
    if pool is None:
        print("Database pool not initialized. Skipping add_keyboard_button.")
        return False
    try:
        async with pool.acquire() as conn:
            # Если menu_key не передан, используем label как ключ
            if menu_key is None:
                menu_key = label
            await conn.execute('''
                INSERT INTO keyboard_buttons (label, menu_key, row_index, col_index)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (label) DO UPDATE SET menu_key = $2, row_index = $3, col_index = $4
            ''', label, menu_key, row, col)
            return True
    except Exception as e:
        print(f"Error adding keyboard button: {e}")
        return False

async def delete_keyboard_button(label):
    """Delete a keyboard button and its content"""
    if pool is None:
        print("Database pool not initialized. Skipping delete_keyboard_button.")
        return False
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute('DELETE FROM button_content WHERE button_id = $1', label)
                await conn.execute('DELETE FROM keyboard_buttons WHERE label = $1', label)
            return True
    except Exception as e:
        print(f"Error deleting keyboard button: {e}")
        return False

async def rename_keyboard_button(old_label, new_label):
    """Rename a keyboard button and update all related content"""
    if pool is None:
        print("Database pool not initialized. Skipping rename_keyboard_button.")
        return False
    try:
        async with pool.acquire() as conn:
            # 1. Обновляем label в keyboard_buttons
            # menu_key остаётся прежним (ключ из MENU_STRUCTURE)
            await conn.execute('UPDATE keyboard_buttons SET label = $1 WHERE label = $2', new_label, old_label)

            # 2. Обновляем button_id в button_content
            # Это важно чтобы контент был доступен по новому имени
            await conn.execute('UPDATE button_content SET button_id = $1 WHERE button_id = $2', new_label, old_label)

            # 3. Обновляем parent_id у всех дочерних элементов
            # Если у этой кнопки были подменю, их parent_id тоже нужно обновить
            await conn.execute('UPDATE button_content SET parent_id = $1 WHERE parent_id = $2', new_label, old_label)

            # 4. Обновляем ID инлайн-кнопок в buttons_json
            # Если эта кнопка упоминается как инлайн-кнопка в других меню
            rows = await conn.fetch('SELECT button_id, buttons_json FROM button_content WHERE buttons_json IS NOT NULL')
            for row in rows:
                try:
                    buttons = json.loads(row['buttons_json'])
                    updated = False

                    for btn in buttons:
                        # Обновляем ID если он содержит старый label
                        if btn.get('id'):
                            # Проверяем точное совпадение или формат "parent:old_label"
                            if btn['id'] == old_label:
                                btn['id'] = new_label
                                updated = True
                            elif ':' in btn['id']:
                                parts = btn['id'].split(':')
                                # Обновляем каждую часть если она совпадает со старым label
                                new_parts = [new_label if part == old_label else part for part in parts]
                                new_id = ':'.join(new_parts)
                                if new_id != btn['id']:
                                    btn['id'] = new_id
                                    updated = True

                    if updated:
                        new_json = json.dumps(buttons)
                        await conn.execute(
                            'UPDATE button_content SET buttons_json = $1 WHERE button_id = $2',
                            new_json,
                            row['button_id']
                        )
                        print(f"[DB_DEBUG] Updated buttons_json in '{row['button_id']}'")

                except json.JSONDecodeError:
                    pass

            print(f"[DB_DEBUG] Renamed button: '{old_label}' -> '{new_label}'")
            print(f"[DB_DEBUG] Updated button_id in button_content")
            print(f"[DB_DEBUG] Updated parent_id for child items")
            print(f"[DB_DEBUG] Updated inline button IDs in buttons_json")

            return True
    except Exception as e:
        print(f"Error renaming keyboard button: {e}")
        return False
