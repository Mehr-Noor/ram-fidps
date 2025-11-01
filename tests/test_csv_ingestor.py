# tests/test_csv_ingestor.py
from unittest.mock import patch, MagicMock
from app.csv_ingestor import ingest_csv

@patch("app.kafka_producer.KafkaProducer")  # patch کلاس قبل از ساخته شدن
def test_ingest_csv(mock_kafka_class, tmp_path):
    # mock instance که send و flush دارد
    mock_producer = MagicMock()
    mock_kafka_class.return_value = mock_producer

    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,Ali\n2,Sara")

    ingest_csv(str(test_file))

    # بررسی تعداد ارسال پیام‌ها
    assert mock_producer.send.call_count == 2
    mock_producer.flush.assert_called_once()
