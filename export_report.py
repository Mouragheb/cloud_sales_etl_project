import psycopg2
import pandas as pd
import os
from datetime import datetime

def export_query_to_csv(query, filename, dbname="sales_db", user="mousragheb"):
    conn = psycopg2.connect(dbname=dbname, user=user)
    df = pd.read_sql_query(query, conn)
    conn.close()

    output_folder = "reports"
    os.makedirs(output_folder, exist_ok=True)

    full_path = f"{output_folder}/{filename}"
    df.to_csv(full_path, index=False)
    print(f"Exported: {full_path}")

if __name__ == "__main__":
    # 1. Total sales by product
    export_query_to_csv(
        """
        SELECT product, SUM(total_price) AS total_sales
        FROM sales
        GROUP BY product
        ORDER BY total_sales DESC;
        """,
        filename="sales_by_product.csv"
    )

    # 2. Quantity by region
    export_query_to_csv(
        """
        SELECT region, SUM(quantity) AS total_quantity
        FROM sales
        GROUP BY region
        ORDER BY total_quantity DESC;
        """,
        filename="quantity_by_region.csv"
    )

    # 3. Daily revenue
    export_query_to_csv(
        """
        SELECT DATE(timestamp) AS sales_day, SUM(total_price) AS total_revenue
        FROM sales
        GROUP BY sales_day
        ORDER BY sales_day;
        """,
        filename="daily_revenue.csv"
    )