from app.csv_ingestor import ingest_csv

if __name__ == "__main__":
    print("Starting CSV Ingestor service...")
    ingest_csv("/data/input.csv")
