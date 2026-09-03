"""
Seed script to populate the warehouse with sample data.
Run this after starting Docker services to have data available.
"""
import sys
import os
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://data_engineer:data_password@localhost:5432/data_platform")

# Sample data
FIRST_NAMES = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry']
LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Food']
REGIONS = ['us-east', 'us-west', 'eu-west', 'asia-pacific']
STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']


def seed_customers(engine, count=200):
    with engine.connect() as conn:
        for i in range(1, count + 1):
            conn.execute(text("""
                INSERT INTO raw.customers (customer_id, first_name, last_name, email, phone, region, country, tier, created_at)
                VALUES (:id, :first, :last, :email, :phone, :region, :country, :tier, :created)
                ON CONFLICT (customer_id) DO NOTHING
            """), {
                "id": f"CUST-{i:05d}",
                "first": random.choice(FIRST_NAMES),
                "last": random.choice(LAST_NAMES),
                "email": f"customer{i}@example.com",
                "phone": f"+1-555-{random.randint(1000, 9999)}",
                "region": random.choice(REGIONS),
                "country": random.choice(["US", "UK", "CA", "DE"]),
                "tier": random.choice(["standard", "premium", "vip"]),
                "created": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 500)),
            })
        conn.commit()
    print(f"  ✓ Seeded {count} customers")


def seed_products(engine, count=50):
    with engine.connect() as conn:
        for i in range(1, count + 1):
            cat = random.choice(CATEGORIES)
            price = round(random.uniform(10, 500), 2)
            conn.execute(text("""
                INSERT INTO raw.products (product_id, name, category, subcategory, price, cost, sku, status, created_at)
                VALUES (:id, :name, :cat, :subcat, :price, :cost, :sku, :status, :created)
                ON CONFLICT (product_id) DO NOTHING
            """), {
                "id": f"PROD-{i:04d}",
                "name": f"{cat} Product {i}",
                "cat": cat,
                "subcat": f"{cat} Sub-{random.randint(1, 5)}",
                "price": price,
                "cost": round(price * random.uniform(0.3, 0.7), 2),
                "sku": f"SKU-{cat[:3].upper()}-{i:04d}",
                "status": random.choice(["active", "inactive"]),
                "created": datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365)),
            })
        conn.commit()
    print(f"  ✓ Seeded {count} products")


def seed_orders(engine, count=2000):
    with engine.connect() as conn:
        for i in range(1, count + 1):
            status = random.choice(STATUSES)
            amount = round(random.uniform(10, 1000), 2)
            created = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 270), hours=random.randint(0, 23))
            
            conn.execute(text("""
                INSERT INTO raw.orders (order_id, customer_id, product_id, status, amount, quantity, currency, created_at, updated_at)
                VALUES (:id, :cust, :prod, :status, :amount, :qty, 'USD', :created, :updated)
                ON CONFLICT (order_id) DO NOTHING
            """), {
                "id": f"ORD-{i:06d}",
                "cust": f"CUST-{random.randint(1, 200):05d}",
                "prod": f"PROD-{random.randint(1, 50):04d}",
                "status": status,
                "amount": amount,
                "qty": random.randint(1, 5),
                "created": created,
                "updated": created + timedelta(hours=random.randint(1, 48)),
            })
        conn.commit()
    print(f"  ✓ Seeded {count} orders")


if __name__ == "__main__":
    print("🌱 Seeding warehouse data...")
    engine = create_engine(DATABASE_URL)
    
    seed_customers(engine)
    seed_products(engine)
    seed_orders(engine)
    
    print("\n✅ Warehouse seeded successfully!")
