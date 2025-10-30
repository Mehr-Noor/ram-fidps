from kafka import KafkaProducer
import json, os

def send_message(data):
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("TOPIC_NAME", "csv-ingest")
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        api_version=(2, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    producer.send(topic, data)
    producer.flush()
