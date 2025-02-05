# db_setup.py
# ** database setup and tables creation

import sqlite3
from datetime import datetime, timedelta
import random

# Database file name
DB_FILE = 'sports_ecomm.db'

def create_database():
    """Create new SQLite database file"""
    conn = sqlite3.connect(DB_FILE)
    conn.close()
    print(f"Database created: {DB_FILE}")

def create_tables():
    """Create required tables"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create dimension_products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dimension_products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT,
            category_id INTEGER,
            base_price DECIMAL(10,2),
            brand TEXT,
            gender TEXT,
            size TEXT,
            color TEXT
        )
    """)

    # Create dimension_dates table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dimension_dates (
            date_id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            quarter INTEGER,
            season TEXT
        )
    """)

    # Create fact_daily_sales table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_daily_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            date_id INTEGER,
            quantity_sold INTEGER,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(10,2),
            discount_applied DECIMAL(5,2),
            channel TEXT,
            region TEXT,
            FOREIGN KEY (product_id) REFERENCES dimension_products(product_id),
            FOREIGN KEY (date_id) REFERENCES dimension_dates(date_id)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully")

def generate_product_data():
    """Generate initial product data"""
    products = [
        ("Nike Running Shoes", 1, 129.99, "Nike", "Unisex"),
        ("Adidas Performance Shorts", 2, 39.99, "Adidas", "Unisex"),
        ("Under Armour T-shirt", 3, 34.99, "Under Armour", "Unisex"),
        ("Lululemon Leggings", 4, 64.99, "Lululemon", "Female"),
        ("Nike Sports Bra", 5, 44.99, "Nike", "Female"),
        ("North Face Training Jacket", 6, 89.99, "North Face", "Unisex"),
        ("Nike Elite Socks", 7, 14.99, "Nike", "Unisex"),
        ("Adidas Gym Bag", 8, 59.99, "Adidas", "Unisex"),
        ("Under Armour Compression Sleeves", 9, 24.99, "Under Armour", "Unisex"),
        ("Fitbit Charge", 10, 149.99, "Fitbit", "Unisex")
    ]

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM dimension_products")

    # Insert new products
    cursor.executemany("""
        INSERT INTO dimension_products (product_name, category_id, base_price, brand, gender)
        VALUES (?, ?, ?, ?, ?)
    """, products)

    conn.commit()
    cursor.close()
    conn.close()
    print("Product data generated successfully")

def initialize_database():
    """Initialize the database with all required tables and initial data"""
    create_database()
    create_tables()
    generate_product_data()
    print("Database initialization completed")

if __name__ == "__main__":
    initialize_database()