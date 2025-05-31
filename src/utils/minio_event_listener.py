# kafka/minio_event_listener.py

import os
import json, time
import requests
from kafka import KafkaProducer
from urllib.parse import urljoin
from time import sleep

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "retail-lakehouse")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "retail-events")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

def main():
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka broker")
        except Exception as e:
            print("Kafka broker not available, retrying in 5 seconds...")
            time.sleep(5)

    print("Listening to MinIO bucket for file uploads...")

    # MinIO event API (watch object creation in raw/ directory)
    url = f"{MINIO_ENDPOINT}/minio/events/listen"

    headers = {"Content-Type": "application/json"}
    session = requests.Session()
    session.auth = (MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    # Use MinIO's event notification via POST (long polling)
    while True:
        try:
            resp = session.post(
                url,
                headers=headers,
                stream=True,
                timeout=600,
            )

            for line in resp.iter_lines():
                if line:
                    event_data = json.loads(line.decode("utf-8"))
                    for record in event_data.get("Records", []):
                        key = record["s3"]["object"]["key"]
                        if key.startswith("raw/"):
                            event = {
                                "event": "FILE_ARRIVED",
                                "bucket": MINIO_BUCKET,
                                "key": key,
                            }
                            print("Sending Kafka event:", event)
                            producer.send(KAFKA_TOPIC, value=event)
                            producer.flush()
        except Exception as e:
            print("Error watching MinIO events:", str(e))
            sleep(5)  # retry in a few seconds

if __name__ == "__main__":
    main()
