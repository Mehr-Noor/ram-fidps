import pytest
import builtins
from unittest.mock import patch, MagicMock
from csv_producer import main as producer_script


@patch("csv_producer.main.KafkaProducer")
def test_csv_producer_sends_messages(mock_producer_class, tmp_path):
    # داده تستی CSV
    test_csv = tmp_path / "data.csv"
    test_csv.write_text("Alice,Smith,28\nBob,Johnson,34\n")

    mock_producer = MagicMock()
    mock_producer_class.return_value = mock_producer

    # تغییر مسیر فایل CSV در اسکریپت
    producer_script.CSV_FILE = str(test_csv)
    producer_script.TOPIC = "test-topic"
    producer_script.KAFKA_BROKER = "localhost:9092"

    # اجرای تابع اصلی
    producer_script.producer = mock_producer
    with patch.object(builtins, "open", open):
        with open(test_csv) as f:
            for row in f:
                message = row.strip().encode("utf-8")
                mock_producer.send.assert_not_called()  # قبل از ارسال
                mock_producer.send("test-topic", message)
                mock_producer.send.assert_called_with("test-topic", message)

    mock_producer.flush.assert_called_once()
