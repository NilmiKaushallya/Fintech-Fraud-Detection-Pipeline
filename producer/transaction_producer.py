import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["Sri Lanka", "India", "USA", "UK", "Germany"]
categories = ["Food", "Electronics", "Travel", "Luxury"]

def generate_transaction():
    user_id = random.randint(1, 5)

    # 10% fraud probability
    if random.random() < 0.1:
        amount = random.randint(6000, 10000)
        location = random.choice(["USA", "Germany"])
    else:
        amount = random.randint(50, 1000)
        location = random.choice(["Sri Lanka", "India"])

    return {
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant_category": random.choice(categories),
        "amount": amount,
        "location": location
    }

while True:
    txn = generate_transaction()
    producer.send("transactions", txn)
    print("Sent:", txn)
    time.sleep(2)