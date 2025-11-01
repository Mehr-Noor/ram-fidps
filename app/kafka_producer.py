# app/kafka_producer.py
from kafka import KafkaProducer
import json, os

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
topic_name = os.getenv("TOPIC_NAME", "csv-ingest")

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    api_version=(2, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_message(data):
    producer.send(topic_name, data)
    producer.flush()
