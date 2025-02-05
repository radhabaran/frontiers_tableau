# test_db.py
# test program with sample queries to analyze the generated data
# *****************************************************************************************
# This analysis program:
#
# 1. Connects to the database
# 2. Runs 8 different analytical queries covering:
#   Top performing products
#   Yearly trends
#   Seasonal patterns
#   Channel performance
#   Regional distribution
#   Day of week patterns
#   Discount impact
#   Monthly trends
# *****************************************************************************************
import sqlite3
import pandas as pd
from tabulate import tabulate
from pathlib import Path

# Database file name
DB_FILE = 'sports_ecomm.db'

def run_analysis():
    if not Path(DB_FILE).exists():
        print(f"Database file {DB_FILE} not found!")
        return

    conn = sqlite3.connect(DB_FILE)

    queries = {
        "1. Top 5 Products by Revenue": """
            SELECT 
                p.product_name,
                ROUND(SUM(s.total_price), 2) as total_revenue,
                SUM(s.quantity_sold) as units_sold
            FROM fact_daily_sales s
            JOIN dimension_products p ON s.product_id = p.product_id
            GROUP BY p.product_name
            ORDER BY total_revenue DESC
            LIMIT 5
        """,

        "2. Yearly Revenue Trend": """
            SELECT 
                d.year,
                ROUND(SUM(s.total_price), 2) as total_revenue,
                COUNT(DISTINCT s.sale_id) as number_of_transactions
            FROM fact_daily_sales s
            JOIN dimension_dates d ON s.date_id = d.date_id
            GROUP BY d.year
            ORDER BY d.year
        """,

        "3. Seasonal Sales Pattern": """
            SELECT 
                d.season,
                ROUND(AVG(s.quantity_sold), 2) as avg_daily_quantity,
                ROUND(AVG(s.total_price), 2) as avg_daily_revenue
            FROM fact_daily_sales s
            JOIN dimension_dates d ON s.date_id = d.date_id
            GROUP BY d.season
            ORDER BY avg_daily_revenue DESC
        """,

        "4. Channel Performance": """
            SELECT 
                channel,
                ROUND(SUM(total_price), 2) as total_revenue,
                COUNT(DISTINCT sale_id) as number_of_transactions,
                ROUND(AVG(quantity_sold), 2) as avg_quantity_per_transaction
            FROM fact_daily_sales
            GROUP BY channel
        """,

        "5. Regional Sales Distribution": """
            SELECT 
                region,
                ROUND(SUM(total_price), 2) as total_revenue,
                COUNT(DISTINCT sale_id) as number_of_transactions
            FROM fact_daily_sales
            GROUP BY region
            ORDER BY total_revenue DESC
        """,

        "6. Day of Week Analysis": """
            SELECT 
                CASE day_of_week
                    WHEN 0 THEN 'Monday'
                    WHEN 1 THEN 'Tuesday'
                    WHEN 2 THEN 'Wednesday'
                    WHEN 3 THEN 'Thursday'
                    WHEN 4 THEN 'Friday'
                    WHEN 5 THEN 'Saturday'
                    WHEN 6 THEN 'Sunday'
                END as day_name,
                ROUND(AVG(daily_revenue), 2) as avg_daily_revenue
            FROM (
                SELECT 
                    d.day_of_week,
                    d.full_date,
                    SUM(s.total_price) as daily_revenue
                FROM fact_daily_sales s
                JOIN dimension_dates d ON s.date_id = d.date_id
                GROUP BY d.day_of_week, d.full_date
            ) daily_sales
            GROUP BY day_of_week
            ORDER BY day_of_week
        """,

        "7. Discount Impact Analysis": """
            SELECT 
                CASE 
                    WHEN discount_applied = 0 THEN 'No Discount'
                    WHEN discount_applied = 0.1 THEN '10% Discount'
                    WHEN discount_applied = 0.2 THEN '20% Discount'
                END as discount_tier,
                COUNT(*) as number_of_sales,
                ROUND(AVG(quantity_sold), 2) as avg_quantity_sold,
                ROUND(SUM(total_price), 2) as total_revenue
            FROM fact_daily_sales
            GROUP BY discount_applied
            ORDER BY discount_applied
        """,

        "8. Monthly Sales Trend (2024)": """
            SELECT 
                d.month,
                ROUND(SUM(s.total_price), 2) as total_revenue,
                SUM(s.quantity_sold) as total_quantity
            FROM fact_daily_sales s
            JOIN dimension_dates d ON s.date_id = d.date_id
            WHERE d.year = 2024
            GROUP BY d.month
            ORDER BY d.month
        """
    }

    try:
        for title, query in queries.items():
            print(f"\n{title}")
            print("-" * 80)

            df = pd.read_sql_query(query, conn)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            print("\n")

    except Exception as e:
        print(f"Error executing query: {str(e)}")

    finally:
        conn.close()

if __name__ == "__main__":
    run_analysis()