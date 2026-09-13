import json
from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

consumer1 = Consumer(consumer_config)

consumer1.subscribe(['orders'])
print(f"Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer1.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        value = msg.value().decode("utf-8")
        value = json.loads(value)
        print(f"Received: {value}")

except KeyboardInterrupt:
    print("Programm shut down using Ctl C")
