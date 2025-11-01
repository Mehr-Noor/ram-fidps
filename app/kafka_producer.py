# kafka_producer.py
from kafka import KafkaProducer
import json, os

class KafkaProducerSingleton:
    _producer = None

    @classmethod
    def get_producer(cls):
        if cls._producer is None:
            bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
            cls._producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                api_version=(2, 8, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        return cls._producer

def send_message(data: dict, producer=None):
    if producer is None:
        producer = KafkaProducerSingleton.get_producer()
    topic_name = os.getenv("TOPIC_NAME", "csv-ingest")
    producer.send(topic_name, data)
    # flush را حذف کنید، تا فقط در پایان ingest_csv فراخوانی شود
