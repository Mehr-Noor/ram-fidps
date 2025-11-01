# csv_ingestor.py
import pandas as pd
from app.kafka_producer import send_message, KafkaProducerSingleton

def ingest_csv(file_path: str):
    df = pd.read_csv(file_path)
    producer = KafkaProducerSingleton.get_producer()
    for _, row in df.iterrows():
        send_message(row.to_dict(), producer=producer)
    producer.flush()  # فقط یک بار flush در پایان
