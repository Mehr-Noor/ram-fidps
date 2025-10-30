import pytest
from app.csv_ingestor import ingest_csv

def test_ingest_csv(tmp_path):
    test_file = tmp_path / "test.csv"
    test_file.write_text("id,name\n1,Ali\n2,Sara")
    ingest_csv(str(test_file))
    assert True  # اگر خطایی ندهد یعنی موفق بوده
