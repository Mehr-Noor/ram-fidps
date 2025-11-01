from unittest.mock import patch
import pandas as pd
import app.kafka_producer as kafka_producer

def send_message(data):
    producer.send(topic_name, data)
    producer.flush()

def ingest_csv(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        message = row.to_dict()
        send_message(message)


@patch.object(kafka_producer, 'producer')
def test_ingest_csv(mock_producer, tmp_path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,Ali\n2,Sara")

    mock_producer.send.return_value = None  # Simulate successful send
    ingest_csv(str(test_file))

    assert mock_producer.send.call_count == 2
