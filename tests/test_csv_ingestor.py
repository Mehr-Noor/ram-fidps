from unittest.mock import patch
from app.csv_ingestor import ingest_csv

@patch('app.kafka_producer.KafkaProducer')
def test_ingest_csv(mock_kafka_producer_class, tmp_path):
    mock_producer_instance = mock_kafka_producer_class.return_value
    mock_producer_instance.send.return_value = None

    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,Ali\n2,Sara")

    ingest_csv(str(test_file))

    assert mock_producer_instance.send.call_count == 2
