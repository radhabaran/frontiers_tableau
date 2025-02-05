# db_setup.py
# ** database setup and tables creation

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine
import random

# Database connection parameters
DB_PARAMS = {
    'database': 'sports_ecomm',
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}


# Create database connection
def create_database():
    # Connect to default postgres database first
    conn = psycopg2.connect(
        database="postgres",
        user=DB_PARAMS['user'],
        password=DB_PARAMS['password'],
        host=DB_PARAMS['host']
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Create database if it doesn't exist
    cursor.execute("DROP DATABASE IF EXISTS sports_ecomm")
    cursor.execute("CREATE DATABASE sports_ecomm")

    cursor.close()
    conn.close()


# Create tables
def create_tables():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Create dimension_products table
    cursor.execute("""
        CREATE TABLE dimension_products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(100),
            category_id INTEGER,
            base_price DECIMAL(10,2),
            brand VARCHAR(50),
            gender VARCHAR(20),
            size VARCHAR(10),
            color VARCHAR(20)
        )
    """)

    # Create dimension_dates table
    cursor.execute("""
        CREATE TABLE dimension_dates (
            date_id SERIAL PRIMARY KEY,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            quarter INTEGER,
            season VARCHAR(20)
        )
    """)

    # Create fact_daily_sales table
    cursor.execute("""
        CREATE TABLE fact_daily_sales (
            sale_id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES dimension_products(product_id),
            date_id INTEGER REFERENCES dimension_dates(date_id),
            quantity_sold INTEGER,
            unit_price DECIMAL(10,2),
            total_price DECIMAL(10,2),
            discount_applied DECIMAL(5,2),
            channel VARCHAR(20),
            region VARCHAR(50)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()


# Generate product data
def generate_product_data():
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

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    for product in products:
        cursor.execute("""
            INSERT INTO dimension_products (product_name, category_id, base_price, brand, gender)
            VALUES (%s, %s, %s, %s, %s)
        """, product)

    conn.commit()
    cursor.close()
    conn.close()


# Initialize database and tables
if __name__ == "__main__":
    create_database()
    create_tables()
    generate_product_data()