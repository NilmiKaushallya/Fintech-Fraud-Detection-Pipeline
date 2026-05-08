import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    user_id = random.randint(1, 10) # Increased range for better data
    # 15% fraud probability
    is_fraud = random.random() < 0.15
    
    if is_fraud:
        amount = random.randint(5001, 10000)
        location = random.choice(["USA", "Germany", "UK"])
    else:
        amount = random.randint(10, 1000)
        location = "Sri Lanka"

    return {
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant_category": random.choice(["Food", "Electronics", "Travel", "Luxury"]),
        "amount": amount,
        "location": location
    }

print("Starting Producer... Press Ctrl+C to stop.")
while True:
    txn = generate_transaction()
    producer.send("transactions", txn)
    print(f"Sent: {txn['location']} - ${txn['amount']}")
    time.sleep(1) # Faster generation for testing