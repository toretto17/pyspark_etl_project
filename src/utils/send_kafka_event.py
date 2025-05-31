import json
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_event(dag_id, bucket, key):
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        event = {
            "event": "FILE_ARRIVED",
            "dag_id": dag_id,
            "bucket": bucket,
            "key": key
        }

        future = producer.send("retail-events", value=event)
        result = future.get(timeout=10)  # block until sent or timeout
        producer.flush()
        logger.info(f"Kafka event sent successfully: {event}")
    except KafkaError as e:
        logger.error(f"Failed to send Kafka event: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending Kafka event: {e}")

def main():
    # You can customize these parameters or read from CLI args/env
    send_event(
        dag_id="retail_lakehouse_etl",
        bucket="retail-lakehouse",
        key="raw/online_retail.csv"
    )

if __name__ == "__main__":
    main()
