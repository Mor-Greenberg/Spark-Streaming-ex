import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


CSV_PATH = Path.home() / "pyspark" / "input" / "Electric_Vehicle_Population_Data.csv"
TOPIC = "wikimedia_topic_1"
BOOTSTRAP_SERVERS = "localhost:9092"


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
    )

    sent = 0
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # send each CSV line as JSON
            producer.send(TOPIC, row)
            sent += 1

         
            producer.flush()
            print(f"sent {sent} records...")

    producer.flush()
    producer.close()
    print(f"done. total sent: {sent}")


if __name__ == "__main__":
    main()
