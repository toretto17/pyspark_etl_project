from airflow.plugins_manager import AirflowPlugin
from airflow.models import Connection
from airflow import settings
from airflow.utils.db import provide_session
import logging
import json


@provide_session
def create_minio_conn(session=None):
    conn_id = 'minio_conn'
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        logging.info(f"Connection {conn_id} already exists.")
        return

    minio_conn = Connection(
        conn_id=conn_id,
        conn_type='s3',
        login='admin',  # default MINIO_ACCESS_KEY
        password='password123',  # default MINIO_SECRET_KEY
        host='http://minio:9000',
        extra=json.dumps({"host": "http://minio:9000", "verify": False})
    )
    session.add(minio_conn)
    session.commit()
    logging.info(f"Created connection {conn_id}")


create_minio_conn()


class MinIOConnPlugin(AirflowPlugin):
    name = "minio_conn_plugin"
