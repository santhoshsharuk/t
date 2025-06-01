# models.py

import sqlite3
import os
import sys # Added for PyInstaller path handling

# Helper to determine the base path for data storage (especially the database)
def _get_persistent_data_base_path():
    """
    Determines the base path for persistent data.
    For bundled apps (PyInstaller), this aims to be next to the executable.
    For development, it's the current working directory.
    """
    if getattr(sys, 'frozen', False):
        # Running in a bundle
        if hasattr(sys, '_MEIPASS'):
            return os.path.dirname(sys.executable)
        else:
            return os.path.abspath(".")
    else:
        # Not running in a bundle (development)
        return os.path.abspath(".")


def connect_db():
    """Connect to SQLite database with improved settings to prevent locking."""
    base_dir = _get_persistent_data_base_path()
    db_path = os.path.join(base_dir, 'instance', 'toystore.db')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(
        db_path,
        check_same_thread=False,
        timeout=30,
        isolation_level=None # Autocommit mode; transactions managed explicitly
    )


def init_db():
    """Initializes the database and creates tables if they don't exist."""
    conn = connect_db()
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()
    conn.execute('BEGIN') # Start a transaction for all DDL
    try:
        # Users table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL COLLATE NOCASE,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin', 'cashier'))
        )
        ''')

        # Categories table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL COLLATE NOCASE
        )
        ''')

        # Products table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL COLLATE NOCASE,
            cost_price REAL NOT NULL DEFAULT 0.0 CHECK(cost_price >= 0),
            selling_price REAL NOT NULL DEFAULT 0.0 CHECK(selling_price >= 0),
            quantity INTEGER NOT NULL DEFAULT 0 CHECK(quantity >= 0),
            category_id INTEGER,
            barcode TEXT UNIQUE COLLATE NOCASE,
            FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
        )
        ''')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_product_barcode ON products (barcode)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_product_name ON products (name)")

        # Sales table - MODIFIED
        cur.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_identifier TEXT NOT NULL,      -- Stores sequential bill number as text
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            sale_price REAL NOT NULL CHECK(sale_price >= 0), 
            sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,                    
            discount_applied_to_bill REAL DEFAULT 0.0,
            payment_method TEXT,                -- << NEW COLUMN FOR PAYMENT METHOD
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
        )
        ''')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_sale_date ON sales (sale_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_bill_identifier ON sales (bill_identifier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_payment_method ON sales (payment_method)") # Optional index

        # Returns table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS returns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK(quantity > 0),
            return_price REAL NOT NULL CHECK(return_price >= 0),
            return_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reason TEXT,
            original_sale_id INTEGER,          
            original_bill_identifier TEXT,     -- Link to the bill_identifier in sales
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT,
            FOREIGN KEY(original_bill_identifier) REFERENCES sales(bill_identifier) ON DELETE SET NULL 
        )
        ''')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_returns_return_date ON returns (return_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_returns_product_id ON returns (product_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_returns_original_bill_identifier ON returns (original_bill_identifier)")

        # Discounts table - MODIFIED for discount type and value
        cur.execute('''
        CREATE TABLE IF NOT EXISTS discounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL COLLATE NOCASE,
            discount_type TEXT NOT NULL CHECK(discount_type IN ('percentage', 'fixed')),
            value REAL NOT NULL CHECK(value >= 0) -- Basic check, specific app-level validation for percentage range
        )
        ''')
        cur.execute("CREATE INDEX IF NOT EXISTS idx_discount_name ON discounts (name)")

        # Bill Sequence table
        cur.execute('''
        CREATE TABLE IF NOT EXISTS bill_sequence (
            id INTEGER PRIMARY KEY CHECK (id = 1), 
            last_bill_no INTEGER NOT NULL DEFAULT 0
        )
        ''')
        cur.execute("INSERT OR IGNORE INTO bill_sequence (id, last_bill_no) VALUES (1, 0)")

        conn.commit()
        print("Database tables checked/created successfully (discounts table updated, sales.payment_method added).")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Error initializing database schema: {e}")
    finally:
        conn.close()

def get_next_bill_number():
    """
    Retrieves the next sequential bill number atomically.
    """
    conn = connect_db()
    cur = conn.cursor()
    next_bill_no = 1 
    try:
        conn.execute('BEGIN IMMEDIATE TRANSACTION')
        cur.execute("SELECT last_bill_no FROM bill_sequence WHERE id = 1")
        row = cur.fetchone()
        if row:
            last_no = row[0]
            next_bill_no = last_no + 1
            cur.execute("UPDATE bill_sequence SET last_bill_no = ? WHERE id = 1", (next_bill_no,))
        else:
            print("Warning: bill_sequence row not found, re-initializing.")
            cur.execute("INSERT INTO bill_sequence (id, last_bill_no) VALUES (1, ?)", (next_bill_no,))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error in get_next_bill_number: {e}. Falling back to default next_bill_no={next_bill_no}")
    finally:
        conn.close()
    return next_bill_no

def get_next_barcode():
    """
    Generate the next sequential barcode by incrementing the highest existing
    numeric-only barcode.
    """
    conn = connect_db()
    cur = conn.cursor()
    next_code = "100000000000" 
    try:
        query = """
            SELECT barcode
            FROM products
            WHERE barcode IS NOT NULL AND barcode != '' AND trim(barcode, '0123456789') = ''
            ORDER BY CAST(barcode AS INTEGER) DESC
            LIMIT 1
        """
        cur.execute(query)
        result = cur.fetchone()
        if result and result[0]:
            try:
                next_code_int = int(result[0]) + 1
                next_code = str(next_code_int).zfill(12)
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not convert barcode '{result[0]}' to int: {e}. Using default next code.")
    except sqlite3.Error as e:
        print(f"Database error in get_next_barcode: {e}")
    finally:
        conn.close()
    return next_code

# --- Discount CRUD Operations ---
def create_discount(name: str, discount_type: str, value: float):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute('BEGIN')
        cur.execute(
            'INSERT INTO discounts (name, discount_type, value) VALUES (?, ?, ?)',
            (name.strip().upper(), discount_type, value)
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        conn.rollback()
        raise
    except sqlite3.Error as e:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_all_discounts():
    conn = connect_db()
    conn.row_factory = sqlite3.Row 
    cur = conn.cursor()
    rows = []
    try:
        cur.execute('SELECT id, name, discount_type, value FROM discounts ORDER BY name')
        rows_raw = cur.fetchall()
        rows = [tuple(row) for row in rows_raw] # Convert Row objects to tuples (id, name, type, value)
    except sqlite3.Error as e:
        print(f"Database error in get_all_discounts: {e}")
    finally:
        conn.close()
    return rows

def get_discount(discount_id: int):
    conn = connect_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    row_obj = None
    try:
        cur.execute('SELECT id, name, discount_type, value FROM discounts WHERE id = ?', (discount_id,))
        row_obj = cur.fetchone()
        if row_obj:
            return tuple(row_obj) # Convert Row object to tuple (id, name, type, value)
    except sqlite3.Error as e:
        print(f"Database error in get_discount (id={discount_id}): {e}")
    finally:
        conn.close()
    return None