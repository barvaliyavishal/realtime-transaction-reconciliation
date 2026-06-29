import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "transactions"
MERCHANTS = ["Amazon", "Walmart", "Starbucks", "Uber", "Shell", "Netflix"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def make_txn():
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": f"card_{random.randint(1, 50):03d}",
        "amount": round(random.uniform(1.0, 500.0), 2),
        "currency": "CAD",
        "merchant": random.choice(MERCHANTS),
        "status": random.choice(["approved", "approved", "approved", "declined"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print(f"Sending transactions to topic '{TOPIC}'. Press Ctrl+C to stop.")
    count = 0
    try:
        while True:
            txn = make_txn()
            producer.send(TOPIC, txn)
            count += 1
            print(
                f"[{count}] {txn['transaction_id'][:8]}  "
                f"{txn['merchant']:9} {txn['amount']:>7} {txn['status']}"
            )
            time.sleep(1)
    except KeyboardInterrupt:
        producer.flush()
        print(f"\nStopped. {count} transactions sent.")
