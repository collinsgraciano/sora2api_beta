
import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), "data", "hancat.db")

def check_db():
    print(f"Checking database at {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("Database file not found!")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check table info
        print("\n--- admin_config Table Info ---")
        cursor.execute("PRAGMA table_info(admin_config)")
        columns = cursor.fetchall()
        for col in columns:
            print(dict(col))  # sqlite3.Row can be converted to dict but here columns are tuples from PRAGMA
            # Actually PRAGMA returns tuples: (cid, name, type, notnull, dflt_value, pk)
            # Let's print them plainly
            
        print([c for c in columns])

        # Check table content
        print("\n--- admin_config Table Content ---")
        cursor.execute("SELECT * FROM admin_config")
        rows = cursor.fetchall()
        if not rows:
            print("No rows found in admin_config!")
        for row in rows:
            print(dict(row))
            if 'scheduling_mode' in dict(row):
                 print(f"scheduling_mode: {row['scheduling_mode']}")
            else:
                 print("scheduling_mode column MISSING in row dict keys")

        # Explicitly check for column
        cursor.execute("PRAGMA table_info(admin_config)")
        columns = cursor.fetchall()
        col_names = [c['name'] for c in columns]
        print(f"\nColumn names: {col_names}")
        if 'scheduling_mode' in col_names:
            print("scheduling_mode column EXISTS")
        else:
            print("scheduling_mode column MISSING")


        conn.close()
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db()
