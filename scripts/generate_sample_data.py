"""
Sample Data Generator for Retail Analytics Data Engineering Pipeline.
Generates realistic retail sales transactions in CSV and JSON formats,
including a controlled subset of dirty/anomalous rows to test schema validation,
business rule validation, duplicate detection, and quarantine logic.
"""

import os
import json
import random
from datetime import datetime, timedelta
import pandas as pd

def generate_retail_dataset(num_records: int = 1200, output_dir: str = "sample_data", seed: int = 42):
    random.seed(seed)
    os.makedirs(output_dir, exist_ok=True)

    products = [
        {"id": "PROD-101", "name": "Apple MacBook Pro 14", "category": "Electronics", "base_price": 1999.00},
        {"id": "PROD-102", "name": "Dell XPS 15 Laptop", "category": "Electronics", "base_price": 1599.00},
        {"id": "PROD-103", "name": "Sony WH-1000XM5 Headphones", "category": "Electronics", "base_price": 399.00},
        {"id": "PROD-104", "name": "Samsung 4K Smart Monitor", "category": "Electronics", "base_price": 499.00},
        {"id": "PROD-201", "name": "Men Classic Denim Jacket", "category": "Apparel", "base_price": 89.00},
        {"id": "PROD-202", "name": "Women Merino Wool Sweater", "category": "Apparel", "base_price": 110.00},
        {"id": "PROD-203", "name": "Waterproof Hiking Boots", "category": "Apparel", "base_price": 145.00},
        {"id": "PROD-301", "name": "Nespresso Espresso Maker", "category": "Home & Kitchen", "base_price": 189.00},
        {"id": "PROD-302", "name": "Cast Iron Dutch Oven 6Qt", "category": "Home & Kitchen", "base_price": 79.99},
        {"id": "PROD-303", "name": "Stainless Steel Cookware Set", "category": "Home & Kitchen", "base_price": 249.00},
        {"id": "PROD-401", "name": "Designing Data-Intensive Applications", "category": "Books", "base_price": 45.00},
        {"id": "PROD-402", "name": "Cloud Native Architecture Guide", "category": "Books", "base_price": 49.99},
        {"id": "PROD-501", "name": "Hydrating Face Serum", "category": "Health & Beauty", "base_price": 38.00},
        {"id": "PROD-502", "name": "Organic Sunscreen SPF 50", "category": "Health & Beauty", "base_price": 24.50},
        {"id": "PROD-601", "name": "Adjustable Dumbbell Set", "category": "Sports & Outdoors", "base_price": 299.00},
        {"id": "PROD-602", "name": "Carbon Fiber Road Bike", "category": "Sports & Outdoors", "base_price": 1200.00},
    ]

    regions = ["North", "South", "East", "West", "Central", "Online"]
    stores = {
        "North": ["STORE-N10", "STORE-N11", "STORE-N12"],
        "South": ["STORE-S20", "STORE-S21"],
        "East": ["STORE-E30", "STORE-E31", "STORE-E32"],
        "West": ["STORE-W40", "STORE-W41"],
        "Central": ["STORE-C50", "STORE-C51"],
        "Online": ["STORE-WEB1", "STORE-APP1"]
    }
    payment_methods = ["Credit Card", "Debit Card", "UPI", "PayPal", "Cash"]
    
    customers = [
        (f"CUST-{1000 + i}", f"user_{1000 + i}@retailcorp.com")
        for i in range(150)
    ]

    base_time = datetime(2026, 8, 1, 9, 0, 0)
    records = []

    # 1. Generate normal, high quality transactions
    valid_count = int(num_records * 0.93)
    for i in range(1, valid_count + 1):
        txn_id = f"TXN-{100000 + i}"
        cust_id, cust_email = random.choice(customers)
        prod = random.choice(products)
        region = random.choice(regions)
        store_id = random.choice(stores[region])
        
        # Add random timedelta up to 60 days
        timestamp = base_time + timedelta(
            days=random.randint(0, 59),
            hours=random.randint(0, 12),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        qty = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03])[0]
        price = prod["base_price"]
        gross = round(price * qty, 2)
        
        # 30% chance of discount (5% - 20% of gross)
        if random.random() < 0.30:
            disc = round(gross * random.uniform(0.05, 0.20), 2)
        else:
            disc = 0.0

        records.append({
            "transaction_id": txn_id,
            "customer_id": cust_id,
            "customer_email": cust_email,
            "transaction_timestamp": timestamp.isoformat(),
            "store_id": store_id,
            "store_region": region,
            "product_id": prod["id"],
            "product_name": prod["name"],
            "product_category": prod["category"],
            "unit_price": price,
            "quantity": qty,
            "discount_amount": disc,
            "payment_method": random.choice(payment_methods)
        })

    # 2. Add controlled anomalies/dirty records for testing validation and quarantine
    dirty_types = [
        "MISSING_CUSTOMER", "NEGATIVE_QUANTITY", "INVALID_PRICE",
        "DISCOUNT_EXCEEDS_GROSS", "MALFORMED_TIMESTAMP", "UNKNOWN_REGION",
        "DUPLICATE_TXN_ID"
    ]

    anomaly_count = num_records - valid_count
    for i in range(anomaly_count):
        anomaly = dirty_types[i % len(dirty_types)]
        idx = valid_count + i + 1
        txn_id = f"TXN-{100000 + idx}"
        cust_id, cust_email = random.choice(customers)
        prod = random.choice(products)
        region = random.choice(regions)
        store_id = random.choice(stores[region])
        timestamp = (base_time + timedelta(days=i % 30, hours=10)).isoformat()
        qty = 2
        price = prod["base_price"]
        disc = 10.0

        if anomaly == "MISSING_CUSTOMER":
            cust_id = None
            cust_email = ""
        elif anomaly == "NEGATIVE_QUANTITY":
            qty = -3
        elif anomaly == "INVALID_PRICE":
            price = -50.00
        elif anomaly == "DISCOUNT_EXCEEDS_GROSS":
            qty = 1
            price = 50.00
            disc = 120.00  # Discount > Gross
        elif anomaly == "MALFORMED_TIMESTAMP":
            timestamp = "2026-99-99 25:99:99_INVALID"
        elif anomaly == "UNKNOWN_REGION":
            region = "Antarctica_Zone"
        elif anomaly == "DUPLICATE_TXN_ID":
            # Duplicate an existing valid transaction ID
            txn_id = "TXN-100005"

        records.append({
            "transaction_id": txn_id,
            "customer_id": cust_id,
            "customer_email": cust_email,
            "transaction_timestamp": timestamp,
            "store_id": store_id,
            "store_region": region,
            "product_id": prod["id"],
            "product_name": prod["name"],
            "product_category": prod["category"],
            "unit_price": price,
            "quantity": qty,
            "discount_amount": disc,
            "payment_method": random.choice(payment_methods)
        })

    df = pd.DataFrame(records)
    csv_file = os.path.join(output_dir, "retail_transactions.csv")
    json_file = os.path.join(output_dir, "retail_transactions.json")

    df.to_csv(csv_file, index=False)
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"Generated {len(records)} transactions.")
    print(f" - CSV: {csv_file}")
    print(f" - JSON: {json_file}")
    print(f" - Valid expected: ~{valid_count}, Dirty/anomalies: {anomaly_count}")

if __name__ == "__main__":
    generate_retail_dataset()

