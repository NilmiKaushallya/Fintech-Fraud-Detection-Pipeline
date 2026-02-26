import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["Sri Lanka", "India", "USA", "UK", "Germany"]
categories = ["Food", "Electronics", "Travel", "Luxury"]

def generate_transaction():
    user_id = random.randint(1, 20)
    amount = random.choice([50, 100, 200, 7000])  # Fraud injected
    location = random.choice(countries)

    return {
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "merchant_category": random.choice(categories),
        "amount": amount,
        "location": location
    }

while True:
    txn = generate_transaction()
    producer.send("transactions", txn)
    print("Sent:", txn)
    time.sleep(2)
