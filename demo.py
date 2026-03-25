"""
demo.py
Demonstration script for the NexusData Agentic Pipeline.
Creates a rich sample database with 6 tables suitable for testing
simple, analytical, complex, and extreme-complex queries.
"""
import os
import sqlite3


def create_demo_db(path: str = "sample.db") -> None:
    """Create (or recreate) the demo SQLite database."""
    if os.path.exists(path):
        try:
            os.remove(path)
        except PermissionError:
            print(f"Warning: Could not remove {path}. Trying to overwrite…")

    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # ── 1. users ──────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            email       TEXT    UNIQUE NOT NULL,
            country     TEXT    NOT NULL DEFAULT 'USA',
            city        TEXT,
            status      TEXT    NOT NULL DEFAULT 'active',
            age         INTEGER,
            joined_at   DATE    DEFAULT CURRENT_DATE
        )
    """)
    users_data = [
        ('Alice Johnson',   'alice@example.com',   'USA',   'New York',    'active',   32, '2022-03-15'),
        ('Bob Smith',       'bob@example.com',     'USA',   'Chicago',     'active',   27, '2022-06-10'),
        ('Charlie Brown',   'charlie@example.com', 'UK',    'London',      'active',   45, '2022-07-05'),
        ('David Wilson',    'david@example.com',   'USA',   'Los Angeles', 'inactive', 38, '2022-09-12'),
        ('Eve Davis',       'eve@example.com',     'India', 'Mumbai',      'active',   29, '2023-01-20'),
        ('Frank Miller',    'frank@example.com',   'UK',    'Manchester',  'active',   53, '2023-02-15'),
        ('Grace Hopper',    'grace@example.com',   'USA',   'San Jose',    'active',   41, '2023-03-01'),
        ('Heidi Klum',      'heidi@example.com',   'Germany','Berlin',     'active',   35, '2023-04-10'),
        ('Ivan Petrov',     'ivan@example.com',    'Russia','Moscow',      'inactive', 47, '2023-05-05'),
        ('Jack Sparrow',    'jack@example.com',    'USA',   'Miami',       'active',   30, '2023-06-12'),
        ('Karen Lee',       'karen@example.com',   'USA',   'Seattle',     'active',   25, '2023-07-01'),
        ('Liam Chen',       'liam@example.com',    'China', 'Beijing',     'active',   33, '2023-08-15'),
        ('Maya Patel',      'maya@example.com',    'India', 'Delhi',       'active',   28, '2023-09-20'),
        ('Nathan Reed',     'nathan@example.com',  'Canada','Toronto',     'active',   44, '2023-10-10'),
        ('Olivia Scott',    'olivia@example.com',  'USA',   'Boston',      'suspended',31, '2023-11-05'),
        ('Paul Young',      'paul@example.com',    'UK',    'Edinburgh',   'active',   55, '2024-01-12'),
        ('Quinn Davis',     'quinn@example.com',   'USA',   'Denver',      'active',   39, '2024-02-01'),
        ('Rachel Green',    'rachel@example.com',  'USA',   'New York',    'active',   26, '2024-03-10'),
        ('Sam Torres',      'sam@example.com',     'Mexico','Mexico City', 'active',   48, '2024-04-01'),
        ('Tina Brown',      'tina@example.com',    'USA',   'Phoenix',     'inactive', 37, '2024-05-15'),
    ]
    cur.executemany(
        "INSERT INTO users (name,email,country,city,status,age,joined_at) VALUES (?,?,?,?,?,?,?)",
        users_data
    )

    # ── 2. products ──────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            category    TEXT    NOT NULL,
            price       REAL    NOT NULL,
            cost_price  REAL    NOT NULL,
            stock       INTEGER NOT NULL,
            supplier    TEXT,
            rating      REAL    DEFAULT 4.0
        )
    """)
    products_data = [
        ('Laptop Pro',          'Electronics', 1200.00, 720.00,  15, 'TechCorp',  4.5),
        ('Smartphone X',        'Electronics',  800.00, 480.00,  30, 'TechCorp',  4.3),
        ('Wireless Mouse',      'Electronics',   25.00,  10.00, 100, 'PeriphCo',  4.1),
        ('Mechanical Keyboard', 'Electronics',   75.00,  30.00,  50, 'PeriphCo',  4.6),
        ('USB-C Hub',           'Electronics',   45.00,  18.00,  80, 'PeriphCo',  4.2),
        ('Denim Jacket',        'Clothing',       60.00,  24.00,  40, 'FashionX',  3.9),
        ('Cotton T-Shirt',      'Clothing',       20.00,   8.00, 200, 'FashionX',  4.0),
        ('Running Shoes',       'Clothing',       85.00,  34.00,  60, 'SportBrand',4.4),
        ('Winter Coat',         'Clothing',      150.00,  60.00,  25, 'FashionX',  4.2),
        ('Coffee Maker',        'Home',            45.00,  18.00,  25, 'HomeGoods', 4.3),
        ('Dining Table',        'Home',           350.00, 140.00,   5, 'FurnitureCo',4.1),
        ('Desk Lamp',           'Home',            30.00,  12.00,  80, 'HomeGoods', 3.8),
        ('Air Purifier',        'Home',           200.00,  80.00,  20, 'HomeGoods', 4.5),
        ('Protein Powder',      'Food',            55.00,  22.00,  90, 'NutrCo',   4.0),
        ('Green Tea Pack',      'Food',            12.00,   4.80, 150, 'NutrCo',   4.2),
    ]
    cur.executemany(
        "INSERT INTO products (name,category,price,cost_price,stock,supplier,rating) VALUES (?,?,?,?,?,?,?)",
        products_data
    )

    # ── 3. orders ────────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE orders (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER NOT NULL REFERENCES users(id),
            order_date     DATE    NOT NULL,
            total_amount   REAL    NOT NULL,
            status         TEXT    NOT NULL DEFAULT 'completed',
            discount       REAL    NOT NULL DEFAULT 0.0,
            payment_method TEXT    NOT NULL DEFAULT 'card'
        )
    """)
    orders_data = [
        (1,  '2023-01-10', 1225.00, 'completed', 0.00,  'card'),
        (2,  '2023-02-05',   80.00, 'completed', 5.00,  'cash'),
        (3,  '2023-02-18',  800.00, 'completed', 0.00,  'online'),
        (4,  '2023-03-01',   45.00, 'completed', 0.00,  'card'),
        (5,  '2023-03-15',  135.00, 'completed', 10.00, 'card'),
        (6,  '2023-04-02',   30.00, 'completed', 0.00,  'cash'),
        (7,  '2023-04-20', 1200.00, 'completed', 50.00, 'card'),
        (8,  '2023-05-10',   25.00, 'completed', 0.00,  'online'),
        (9,  '2023-05-25',  350.00, 'completed', 20.00, 'card'),
        (10, '2023-06-08',   75.00, 'completed', 0.00,  'cash'),
        (11, '2023-06-20',  200.00, 'pending',   0.00,  'card'),
        (12, '2023-07-05',  550.00, 'completed', 25.00, 'online'),
        (13, '2023-07-18',   60.00, 'cancelled', 0.00,  'card'),
        (14, '2023-08-01',  120.00, 'completed', 0.00,  'cash'),
        (15, '2023-08-15',  800.00, 'completed', 40.00, 'online'),
        (1,  '2023-09-01',   45.00, 'completed', 0.00,  'card'),
        (3,  '2023-09-20',  400.00, 'pending',   15.00, 'card'),
        (5,  '2023-10-05',  170.00, 'completed', 0.00,  'online'),
        (7,  '2023-10-18',  250.00, 'completed', 0.00,  'cash'),
        (2,  '2023-11-01',  100.00, 'completed', 5.00,  'card'),
        (9,  '2023-11-15',  700.00, 'completed', 35.00, 'card'),
        (11, '2023-12-01',   55.00, 'cancelled', 0.00,  'cash'),
        (13, '2023-12-15',  300.00, 'completed', 0.00,  'online'),
        (15, '2024-01-10',  450.00, 'completed', 20.00, 'card'),
        (4,  '2024-01-25',   90.00, 'completed', 0.00,  'cash'),
        (6,  '2024-02-08', 1100.00, 'completed', 100.00,'card'),
        (8,  '2024-02-20',  200.00, 'pending',   0.00,  'online'),
        (10, '2024-03-05',  600.00, 'completed', 30.00, 'card'),
        (12, '2024-03-18',   75.00, 'completed', 0.00,  'cash'),
        (14, '2024-04-01',  950.00, 'completed', 50.00, 'card'),
    ]
    cur.executemany(
        "INSERT INTO orders (user_id,order_date,total_amount,status,discount,payment_method) VALUES (?,?,?,?,?,?)",
        orders_data
    )

    # ── 4. order_items ───────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE order_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id    INTEGER NOT NULL REFERENCES orders(id),
            product_id  INTEGER NOT NULL REFERENCES products(id),
            quantity    INTEGER NOT NULL,
            unit_price  REAL    NOT NULL
        )
    """)
    order_items_data = [
        (1,  1,  1, 1200.00), (1,  3,  1,   25.00),
        (2,  6,  4,   20.00),
        (3,  2,  1,  800.00),
        (4,  10, 1,   45.00),
        (5,  8,  1,   85.00), (5,  4,  1,   50.00),
        (6,  12, 1,   30.00),
        (7,  1,  1, 1200.00),
        (8,  3,  1,   25.00),
        (9,  11, 1,  350.00),
        (10, 4,  1,   75.00),
        (11, 13, 1,  200.00),
        (12, 2,  1,  800.00), (12, 7,  5,   20.00), (12, 3,  2,   25.00),
        (13, 6,  1,   60.00),
        (14, 10, 2,   45.00), (14, 12, 1,   30.00),
        (15, 2,  1,  800.00),
        (16, 10, 1,   45.00),
        (17, 9,  1,  350.00), (17, 3,  2,   25.00),
        (18, 8,  2,   85.00),
        (19, 1,  1, 1200.00), (19, 5,  1,   45.00),
        (20, 7,  2,   20.00), (20, 15, 3,   12.00),
        (21, 2,  1,  800.00),
        (22, 13, 1,  200.00),  # cancelled
        (23, 9,  1,  350.00),
        (24, 5,  2,   85.00), (24, 9,  1,  150.00), (24, 14, 1,   55.00),
        (25, 3,  1,   25.00), (25, 7,  3,   20.00), (25, 15, 1,   12.00),
        (26, 1,  1, 1200.00),
        (27, 13, 1,  200.00),
        (28, 4,  2,   75.00), (28, 9,  1,  350.00),
        (29, 3,  1,   25.00), (29, 12, 2,   30.00),
        (30, 1,  1, 1200.00), (30, 5,  1,   45.00), (30, 14, 1,   55.00),
    ]
    cur.executemany(
        "INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (?,?,?,?)",
        order_items_data
    )

    # ── 5. reviews ───────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE reviews (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES users(id),
            product_id  INTEGER NOT NULL REFERENCES products(id),
            rating      INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
            comment     TEXT,
            reviewed_at DATE    NOT NULL
        )
    """)
    reviews_data = [
        (1,  1,  5, 'Great laptop!',        '2023-01-25'),
        (2,  2,  4, 'Good phone',           '2023-02-20'),
        (3,  3,  4, 'Solid mouse',          '2023-03-10'),
        (4,  10, 5, 'Best coffee maker',    '2023-03-15'),
        (5,  8,  4, 'Comfortable shoes',    '2023-03-28'),
        (6,  12, 3, 'Lamp is average',      '2023-04-15'),
        (7,  1,  5, 'Love this laptop',     '2023-05-05'),
        (8,  3,  4, 'Does the job',         '2023-05-22'),
        (9,  11, 4, 'Nice table',           '2023-06-10'),
        (10, 4,  5, 'Best keyboard ever',   '2023-06-25'),
        (11, 2,  3, 'Phone is ok',          '2023-07-10'),
        (12, 13, 5, 'Air purifier is great','2023-07-20'),
        (13, 6,  2, 'Jacket faded fast',    '2023-08-05'),
        (14, 10, 4, 'Decent coffee maker',  '2023-08-18'),
        (15, 2,  5, 'Excellent phone',      '2023-09-02'),
        (1,  14, 4, 'Good protein',         '2023-09-15'),
        (3,  9,  3, 'Coat runs small',      '2023-10-01'),
        (5,  8,  5, 'Best shoes ever',      '2023-10-12'),
        (7,  5,  4, 'Hub works well',       '2023-10-28'),
        (9,  11, 3, 'Table took time',      '2023-11-05'),
        (2,  15, 5, 'Love green tea',       '2023-11-20'),
        (4,  7,  4, 'Nice t-shirt',         '2023-12-01'),
        (6,  4,  5, 'Keyboard is amazing',  '2023-12-15'),
        (8,  13, 4, 'Purifier works well',  '2024-01-05'),
        (10, 1,  4, 'Laptop heats up a bit','2024-01-20'),
    ]
    cur.executemany(
        "INSERT INTO reviews (user_id,product_id,rating,comment,reviewed_at) VALUES (?,?,?,?,?)",
        reviews_data
    )

    # ── 6. employees ─────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE employees (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            department  TEXT    NOT NULL,
            salary      REAL    NOT NULL,
            hire_date   DATE    NOT NULL,
            manager_id  INTEGER REFERENCES employees(id)
        )
    """)
    employees_data = [
        # Engineering
        ('Alice Eng',   'Engineering', 120000, '2020-01-10', None),   # 1 — manager
        ('Bob Dev',     'Engineering',  95000, '2020-06-15', 1),
        ('Carol Dev',   'Engineering',  88000, '2021-02-01', 1),
        ('Dan Dev',     'Engineering',  75000, '2022-03-10', 1),
        # Marketing
        ('Eve Mkt',     'Marketing',   105000, '2019-05-20', None),    # 5 — manager
        ('Frank Mkt',   'Marketing',    82000, '2020-09-01', 5),
        ('Grace Mkt',   'Marketing',    78000, '2021-11-15', 5),
        # Sales
        ('Hank Sales',  'Sales',        98000, '2018-07-10', None),    # 8 — manager
        ('Iris Sales',  'Sales',        72000, '2020-02-20', 8),
        ('Jack Sales',  'Sales',        68000, '2021-06-01', 8),
        ('Karl Sales',  'Sales',        64000, '2022-09-15', 8),
        # HR
        ('Laura HR',    'HR',           90000, '2019-03-01', None),    # 12 — manager
        ('Mike HR',     'HR',           70000, '2021-01-10', 12),
    ]
    cur.executemany(
        "INSERT INTO employees (name,department,salary,hire_date,manager_id) VALUES (?,?,?,?,?)",
        employees_data
    )

    conn.commit()
    conn.close()
    print(f"  Created {path} with 6 tables: users, products, orders, order_items, reviews, employees")


def run_demo() -> None:
    print("\n── NexusData Demo ────────────────────────────────────────────────────────")
    print("Creating sample database…")
    create_demo_db("sample.db")

    from nexus_data.orchestrator import NexusData
    nd = NexusData()
    db_uri = "sqlite:///sample.db"

    print("\nConnecting and initialising knowledge base…")
    nd.connect_and_initialize(db_uri, interactive=False)
    nd.set_user_context("The user is a data analyst interested in e-commerce analytics.")

    queries = [
        ("Revenue by category",
         "Show me the total revenue generated per product category, sorted by revenue descending."),
        ("High-value customers",
         "Which users have spent more than 500 dollars in total? List their names and total spent."),
        ("Profit margins",
         "What is the profit margin percentage for each product category?"),
        ("Top employees per dept",
         "Show the top 2 highest-paid employees in each department."),
        ("Monthly order trend",
         "Show the monthly order count and total revenue trend for 2023."),
    ]

    for label, query in queries:
        print(f"\n── {label} ─────────────────────────────────────")
        print(f"Q: {query}")
        result = nd.ask(query)
        if result.error:
            print(f"Error: {result.error}")
        else:
            print(f"SQL:\n{result.sql}")
            if result.rows:
                print("Results (first 5):")
                for row in result.rows[:5]:
                    print(f"  {row}")
                print(f"  … {len(result.rows)} total row(s)")
            else:
                print("  No rows returned.")
            if result.natural_language_summary:
                print(f"Summary: {result.natural_language_summary}")

    print("\n── Demo complete ─────────────────────────────────────────────────────────\n")


if __name__ == "__main__":
    run_demo()
