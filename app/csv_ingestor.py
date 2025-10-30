import pandas as pd
from app.kafka_producer import send_message

def ingest_csv(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        message = row.to_dict()
        send_message(message)
