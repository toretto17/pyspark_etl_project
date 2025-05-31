import json
import os
import logging
import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = "retail-events"
GROUP_ID = "airflow-trigger-group"

def trigger_airflow_dag(dag_id, conf=None):
    if conf is None:
        conf = {}

    url = f"http://airflow-webserver:8080/api/v1/dags/{dag_id}/dagRuns"
    try:
        response = requests.post(
            url,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
            headers={"Content-Type": "application/json"},
            json={"conf": conf}
        )
        response.raise_for_status()  # Raise HTTPError for bad status codes
        logger.info(f"Successfully triggered DAG '{dag_id}' - Status: {response.status_code}")
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error while triggering DAG '{dag_id}': {http_err} - Response: {response.text}")
    except Exception as err:
        logger.error(f"Error triggering DAG '{dag_id}': {err}")

def main():
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset="earliest",
            group_id=GROUP_ID,
            enable_auto_commit=True
        )
        logger.info("Kafka consumer started. Waiting for events...")

        for message in consumer:
            try:
                event = message.value
                logger.info(f"Received Kafka event: {event}")

                if not isinstance(event, dict):
                    logger.warning(f"Invalid event format, expected dict but got {type(event)}")
                    continue

                if event.get("event") == "FILE_ARRIVED":
                    dag_id = event.get("dag_id")
                    if not dag_id:
                        logger.warning("No 'dag_id' found in event; skipping DAG trigger")
                        continue

                    conf = event.get("conf", {})  # Optionally pass additional config
                    trigger_airflow_dag(dag_id, conf)

                else:
                    logger.info(f"Ignored event type: {event.get('event')}")

            except Exception as inner_err:
                logger.error(f"Error processing Kafka message: {inner_err}")

    except KafkaError as kafka_err:
        logger.error(f"Kafka consumer error: {kafka_err}")
    except Exception as e:
        logger.error(f"Unexpected error in Kafka consumer: {e}")

if __name__ == "__main__":
    main()
