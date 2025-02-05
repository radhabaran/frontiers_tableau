# generate_data.py
# *****************************************************************************************
# Key features of this data generation:
#
# 1.Date Dimension:
#       Covers 2015-2024
#       Includes seasons, quarters, day of week
#       Full calendar hierarchy
# 2.Sales Data Generation Logic:
#       Base quantity randomization
#   Seasonal adjustments:
#       Holiday season boost (December)
#       New Year resolution boost (January)
#       Summer season boost
# Weekend vs weekday patterns
# Year-over-year growth (10% annually)
# Random discounts (0%, 10%, or 20%)
# Channel distribution (online/store)
# Regional distribution
# 3.Realistic Patterns:
#       Multiple transactions per day
#       Varied quantities
#       Price adjustments with discounts
#       Regional and channel diversity
# ****************************************************************************************

import sqlite3
from datetime import datetime, timedelta
import random
from pathlib import Path

# Database file name
DB_FILE = 'sports_ecomm.db'

def get_db_connection():
    """Create a database connection"""
    return sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

def generate_date_dimension():
    """Generate date dimension data for 10 years"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Clear existing dates
        cursor.execute("DELETE FROM dimension_dates")

        start_date = datetime(2015, 1, 1)
        end_date = datetime(2024, 12, 31)

        current_date = start_date
        while current_date <= end_date:
            # Determine season
            month = current_date.month
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'

            cursor.execute("""
                INSERT INTO dimension_dates 
                (full_date, year, month, day, day_of_week, quarter, season)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                current_date.date(),
                current_date.year,
                current_date.month,
                current_date.day,
                current_date.weekday(),
                (current_date.month - 1) // 3 + 1,
                season
            ))

            current_date += timedelta(days=1)

        conn.commit()
        print("Date dimension generated successfully")

    except Exception as e:
        print(f"Error generating date dimension: {str(e)}")

    finally:
        conn.close()

def generate_sales_data():
    """Generate sales data with realistic patterns"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Clear existing sales data
        cursor.execute("DELETE FROM fact_daily_sales")

        # Get all dates
        cursor.execute("SELECT date_id, full_date, month, day_of_week FROM dimension_dates")
        dates = cursor.fetchall()

        # Get all products
        cursor.execute("SELECT product_id, base_price FROM dimension_products")
        products = cursor.fetchall()

        regions = ['North', 'South', 'East', 'West']
        channels = ['online', 'store']

        total_records = 0
        batch_size = 1000
        batch_data = []

        # Generate sales data
        for date in dates:
            date_id, full_date, month, day_of_week = date

            for product in products:
                product_id, base_price = product

                # Base quantity (random)
                base_quantity = random.randint(10, 50)

                # Seasonal adjustments
                seasonal_multiplier = 1.0
                if month in [12]:  # December holiday season
                    seasonal_multiplier *= 1.5
                elif month in [1]:  # January (New Year resolutions)
                    seasonal_multiplier *= 1.3
                elif month in [6, 7, 8]:  # Summer months
                    seasonal_multiplier *= 1.2

                # Weekend adjustment
                if day_of_week in [5, 6]:  # Weekend
                    seasonal_multiplier *= 1.2

                # Year over year growth (10% annual growth)
                year = full_date.year
                yoy_growth = 1.0 + (0.1 * (year - 2015))

                final_quantity = int(base_quantity * seasonal_multiplier * yoy_growth)

                # Generate multiple transactions for each product-date combination
                for _ in range(random.randint(1, 3)):
                    quantity = random.randint(1, final_quantity)
                    discount = random.choice([0, 0, 0, 0.1, 0.2])  # 60% chance of no discount
                    unit_price = base_price * (1 - discount)
                    total_price = unit_price * quantity

                    batch_data.append((
                        product_id,
                        date_id,
                        quantity,
                        unit_price,
                        total_price,
                        discount,
                        random.choice(channels),
                        random.choice(regions)
                    ))
                    total_records += 1

                    # Batch insert
                    if len(batch_data) >= batch_size:
                        cursor.executemany("""
                            INSERT INTO fact_daily_sales 
                            (product_id, date_id, quantity_sold, unit_price, total_price, 
                             discount_applied, channel, region)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, batch_data)
                        conn.commit()
                        print(f"Generated {total_records} sales records...")
                        batch_data = []

        # Insert remaining records
        if batch_data:
            cursor.executemany("""
                INSERT INTO fact_daily_sales 
                (product_id, date_id, quantity_sold, unit_price, total_price, 
                 discount_applied, channel, region)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()

        print(f"Sales data generation completed. Total records: {total_records}")

    except Exception as e:
        print(f"Error generating sales data: {str(e)}")

    finally:
        conn.close()

def verify_data():
    """Verify the generated data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("\nData Verification:")
        print("-" * 50)

        # Check products
        cursor.execute("SELECT COUNT(*) FROM dimension_products")
        print(f"Number of products: {cursor.fetchone()[0]}")

        # Check dates
        cursor.execute("SELECT COUNT(*) FROM dimension_dates")
        print(f"Number of dates: {cursor.fetchone()[0]}")

        # Check sales
        cursor.execute("SELECT COUNT(*) FROM fact_daily_sales")
        sales_count = cursor.fetchone()[0]
        print(f"Number of sales records: {sales_count:,}")

        # Check date range
        cursor.execute("""
            SELECT MIN(full_date), MAX(full_date) 
            FROM dimension_dates
        """)
        date_range = cursor.fetchone()
        print(f"Date range: {date_range[0]} to {date_range[1]}")

        # Sample of total revenue by year
        cursor.execute("""
            SELECT d.year, ROUND(SUM(s.total_price), 2) as total_revenue
            FROM fact_daily_sales s
            JOIN dimension_dates d ON s.date_id = d.date_id
            GROUP BY d.year
            ORDER BY d.year
        """)
        print("\nYearly Revenue:")
        for row in cursor.fetchall():
            print(f"Year {row[0]}: ${row[1]:,.2f}")

    except Exception as e:
        print(f"Error verifying data: {str(e)}")

    finally:
        conn.close()

if __name__ == "__main__":
    if not Path(DB_FILE).exists():
        print("Database file not found. Please run db_setup.py first.")
        exit(1)

    print("Starting data generation process...")
    print("\nStep 1: Generating date dimension...")
    generate_date_dimension()

    print("\nStep 2: Generating sales data...")
    generate_sales_data()

    print("\nStep 3: Verifying generated data...")
    verify_data()

    print("\nData generation process completed!")