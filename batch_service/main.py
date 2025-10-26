import csv
import logging
from kafka import KafkaProducer
import os

logging.basicConfig(level=logging.INFO)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "my-cluster-kafka-bootstrap.kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "test-topic")
CSV_FILE = os.getenv("CSV_FILE", "/data/data.csv")

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

logging.info(f"Reading CSV from {CSV_FILE}")

with open(CSV_FILE, newline='') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        message = ",".join(row)
        producer.send(TOPIC, message.encode("utf-8"))
        logging.info(f"Produced message: {message}")

producer.flush()
logging.info("Finished sending messages")
