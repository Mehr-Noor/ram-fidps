from kafka import KafkaProducer
import json, os

def send_message(data):
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic = os.getenv("TOPIC_NAME", "csv-ingest")
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(topic, data)
    producer.flush()
