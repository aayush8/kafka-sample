import json
import time
from kafka import KafkaProducer

def serialize_message(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=serialize_message,
)

