import sqlite3
import os
import sys

def _get_persistent_data_base_path():
    """
    Determines the base path for persistent data.
    For bundled apps (PyInstaller), this aims to be next to the executable.
    For development, it's the current working directory.
    """
    if getattr(sys, 'frozen', False):
        if hasattr(sys, '_MEIPASS'):
            return os.path.dirname(sys.executable)
        else:
            return os.path.abspath(".")
    else:
        return os.path.abspath(".")

DB_PATH = os.path.join(_get_persistent_data_base_path(), 'instance', 'toystore.db')

def column_exists(cursor, table_name, column_name):
    """Checks if a column exists in a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns

def migrate_discounts_table():
    """
    Migrates the discounts table to the new schema:
    - Adds 'discount_type' and 'value' columns.
    - Populates them based on the old 'percent' column.
    - Drops the 'percent' column.
    """
    if not os.path.exists(DB_PATH):
        print(f"Database file not found at {DB_PATH}. Cannot migrate.")
        return

    conn = None
    try:
        print(f"Connecting to database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        print("Starting migration of 'discounts' table...")

        # --- Step 1: Check if migration is even needed ---
        if column_exists(cur, 'discounts', 'discount_type') and \
           column_exists(cur, 'discounts', 'value') and \
           not column_exists(cur, 'discounts', 'percent'):
            print("'discounts' table already appears to be migrated. Skipping.")
            return

        # --- Step 2: Add new columns if they don't exist ---
        if not column_exists(cur, 'discounts', 'discount_type'):
            print("Adding 'discount_type' column...")
            cur.execute("ALTER TABLE discounts ADD COLUMN discount_type TEXT")
        else:
            print("'discount_type' column already exists.")

        if not column_exists(cur, 'discounts', 'value'):
            print("Adding 'value' column...")
            cur.execute("ALTER TABLE discounts ADD COLUMN value REAL")
        else:
            print("'value' column already exists.")

        # --- Step 3: Populate new columns from old 'percent' column (if it exists) ---
        if column_exists(cur, 'discounts', 'percent'):
            print("Populating 'discount_type' and 'value' from old 'percent' column...")
            # Assuming all old discounts were percentage based
            # And setting a default value of 0 if percent was NULL (though it shouldn't be)
            cur.execute("""
                UPDATE discounts 
                SET discount_type = 'percentage', 
                    value = COALESCE(percent, 0.0)
                WHERE discount_type IS NULL OR value IS NULL 
            """) # Only update rows that haven't been touched yet
            print(f"{cur.rowcount} rows updated for discount_type and value.")
        else:
            print("'percent' column does not exist. Skipping data population from 'percent'.")
            # If 'percent' doesn't exist but 'discount_type' or 'value' are NULL,
            # we might need to set a default. For now, we assume if 'percent' is gone,
            # previous steps of migration handled it or it's a new table.
            # If 'discount_type' and 'value' were added but not populated, let's default them
            cur.execute("""
                UPDATE discounts
                SET discount_type = 'percentage',
                    value = 0.0
                WHERE discount_type IS NULL
            """)
            print(f"{cur.rowcount} rows updated with default discount_type/value for NULLs.")


        # --- Step 4: Add NOT NULL constraints and CHECK constraints (complex with ALTER TABLE in SQLite) ---
        # SQLite has limited ALTER TABLE support. We can't directly add NOT NULL or CHECK constraints
        # to existing columns easily without recreating the table.
        # For now, we'll rely on the application logic in models.py for these constraints
        # during new inserts/updates. The init_db() function in models.py defines these
        # for newly created databases.
        #
        # A more robust migration would involve:
        # 1. CREATE TABLE new_discounts_table AS (new schema with constraints)
        # 2. INSERT INTO new_discounts_table SELECT id, name, discount_type, value FROM discounts
        # 3. DROP TABLE discounts
        # 4. ALTER TABLE new_discounts_table RENAME TO discounts
        # This is more involved and requires careful handling of data types and constraints.
        # For this scenario, we'll skip fully enforcing constraints via ALTER for simplicity,
        # assuming the data populated in step 3 is valid.

        print("Updating existing NULL values in 'discount_type' to 'percentage' and 'value' to 0.0 if any...")
        cur.execute("UPDATE discounts SET discount_type = 'percentage' WHERE discount_type IS NULL")
        cur.execute("UPDATE discounts SET value = 0.0 WHERE value IS NULL")


        # --- Step 5: Drop the old 'percent' column if it still exists ---
        if column_exists(cur, 'discounts', 'percent'):
            print("Dropping old 'percent' column...")
            # SQLite < 3.35.0 doesn't support DROP COLUMN directly.
            # We'll assume a modern SQLite version. If not, the more complex table recreation is needed.
            try:
                cur.execute("ALTER TABLE discounts DROP COLUMN percent")
                print("'percent' column dropped successfully.")
            except sqlite3.OperationalError as e:
                if "near \"DROP\": syntax error" in str(e) or "Cannot drop column" in str(e):
                    print("Warning: Your SQLite version might not support DROP COLUMN directly.")
                    print("The 'percent' column was not dropped. You might need to do this manually or use a more complex migration.")
                    print("The application should still work, but the old 'percent' column will remain.")
                else:
                    raise # Re-raise other operational errors
        else:
            print("'percent' column already dropped or never existed.")

        conn.commit()
        print("'discounts' table migration completed successfully.")

    except sqlite3.Error as e:
        print(f"SQLite error during migration: {e}")
        if conn:
            conn.rollback()
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"An unexpected error occurred during migration: {e}")
        if conn:
            conn.rollback()
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    print("This script will attempt to migrate the 'discounts' table in your database.")
    print(f"Database path: {DB_PATH}")
    print("!!! PLEASE BACKUP YOUR DATABASE FILE BEFORE PROCEEDING !!!")
    
    # Basic check for discounts table existence
    conn_check = None
    can_proceed = False
    try:
        if os.path.exists(DB_PATH):
            conn_check = sqlite3.connect(DB_PATH)
            cur_check = conn_check.cursor()
            cur_check.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='discounts';")
            if cur_check.fetchone():
                can_proceed = True
            else:
                print("Error: 'discounts' table not found in the database.")
        else:
            print(f"Error: Database file not found at {DB_PATH}")
    except sqlite3.Error as e:
        print(f"Error connecting to database for pre-check: {e}")
    finally:
        if conn_check:
            conn_check.close()

    if can_proceed:
        confirm = input("Have you backed up your database? (yes/no): ").strip().lower()
        if confirm == 'yes':
            migrate_discounts_table()
        else:
            print("Migration cancelled. Please backup your database and run the script again.")
    else:
        print("Cannot proceed with migration due to pre-check failures.")