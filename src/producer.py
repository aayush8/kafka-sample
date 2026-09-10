import json
import uuid
from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed : {err}")
    else:
        print(f"Delivered {msg.value().decode('utf-8')}")
        print(f"\nto topic {msg.topic()} partition [{msg.partition()}] at offset {msg.offset()}")

order_data = {
    "order_id": str(uuid.uuid4()),
    "customer_id": "12345",
    "order_date": "2026-09-01",
    "items": [
        {"item_id": "C007", "quantity": 2, "price": 10.0},
        {"item_id": "B002", "quantity": 1, "price": 20.0}
    ],
    "total_amount": "50.0"
}

value = json.dumps(order_data).encode("utf-8")

producer.produce(
    topic='orders', 
    value=value,
    callback=delivery_report
)
producer.flush()
