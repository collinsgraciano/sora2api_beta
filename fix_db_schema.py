
import asyncio
import os
import sys

# Add src to path
sys.path.append(os.getcwd())

from src.core.database import Database

async def check_db_path():
    db = Database()
    print(f"Calculated DB Path: {db.db_path}")
    print(f"Exists: {os.path.exists(db.db_path)}")
    
    if os.path.exists(db.db_path):
        import aiosqlite
        async with aiosqlite.connect(db.db_path) as conn:
            print("Connected to DB.")
            cursor = await conn.execute("PRAGMA table_info(admin_config)")
            columns = await cursor.fetchall()
            col_names = [c[1] for c in columns]
            print(f"admin_config columns: {col_names}")
            
            if 'scheduling_mode' not in col_names:
                print("ADDING scheduling_mode column...")
                try:
                    await conn.execute("ALTER TABLE admin_config ADD COLUMN scheduling_mode TEXT DEFAULT 'random'")
                    await conn.commit()
                    print("Column added successfully.")
                except Exception as e:
                    print(f"Failed to add column: {e}")
            else:
                print("scheduling_mode column already exists.")

if __name__ == "__main__":
    asyncio.run(check_db_path())
