import pandas as pd
from faker import Faker
import random
from datetime import datetime
import os

# Initialize Faker
fake = Faker()

# Constants
PRODUCTS = ["Gyro", "Shawarma", "Falafel", "Salad", "Baklava"]
REGIONS = ["Houston", "Dallas", "Austin", "San Antonio"]
NUM_ROWS = 500  # You can increase this later

# Generate fake sales records
def generate_sales_data():
    sales = []

    for _ in range(NUM_ROWS):
        sale = {
            "order_id": fake.uuid4(),
            "customer_name": fake.name(),
            "region": random.choice(REGIONS),
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(5.0, 15.0), 2),
            "timestamp": fake.date_time_this_year().strftime("%Y-%m-%d %H:%M:%S")
        }
        sale["total_price"] = round(sale["quantity"] * sale["unit_price"], 2)
        sales.append(sale)

    return pd.DataFrame(sales)

# Save to data_lake/raw/ with timestamped filename
def save_data(df):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"sales_{date_str}.csv"
    filepath = os.path.join("data_lake", "raw", filename)
    df.to_csv(filepath, index=False)
    print(f"Saved {len(df)} rows to {filepath}")

if __name__ == "__main__":
    df = generate_sales_data()
    save_data(df)