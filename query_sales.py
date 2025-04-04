import psycopg2

def query_total_sales_by_product(cur):
    cur.execute("""
        SELECT product, SUM(total_price) AS total_sales
        FROM sales
        GROUP BY product
        ORDER BY total_sales DESC;
    """)
    rows = cur.fetchall()
    print("\n=== Total Sales by Product ===")
    for product, total in rows:
        print(f"{product:<10} ${round(total, 2)}")


def query_quantity_by_region(cur):
    cur.execute("""
        SELECT region, SUM(quantity) AS total_quantity
        FROM sales
        GROUP BY region
        ORDER BY total_quantity DESC;
    """)
    rows = cur.fetchall()
    print("\n=== Total Quantity Sold by Region ===")
    for region, qty in rows:
        print(f"{region:<10} {qty} units")


def query_daily_revenue(cur):
    cur.execute("""
        SELECT DATE(timestamp) AS sales_day, SUM(total_price) AS total_revenue
        FROM sales
        GROUP BY sales_day
        ORDER BY sales_day;
    """)
    rows = cur.fetchall()
    print("\n=== Daily Revenue Summary ===")
    for date, revenue in rows:
        print(f"{date} | ${round(revenue, 2)}")


def run_queries(dbname="sales_db", user="mousragheb"):
    conn = psycopg2.connect(dbname=dbname, user=user)
    cur = conn.cursor()

    query_total_sales_by_product(cur)
    query_quantity_by_region(cur)
    query_daily_revenue(cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_queries()