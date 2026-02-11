import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_transaction():
    return {
        "transaction_id": random.randint(100000, 999999),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": random.randint(1, 1000),
        "merchant_id": random.randint(1, 200),
        "amount": round(random.uniform(1, 1000), 2),
        "country": random.choice(["TR", "US", "DE", "FR"]),
        "is_foreign": random.choice([0, 1])
    }

while True:
    transaction = generate_transaction()
    producer.send("fraud-transactions", transaction)
    print("Sent:", transaction)
    time.sleep(1)




