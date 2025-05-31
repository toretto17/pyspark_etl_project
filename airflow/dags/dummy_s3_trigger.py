from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_connection():
    hook = S3Hook(aws_conn_id='dummy_minio')  # Just initialize, no method call
    # This alone loads the Amazon provider
    print("S3Hook initialized!")
    client = hook.get_conn()
    response = client.list_buckets()
    for bucket in response['Buckets']:
        print(bucket['Name'])

with DAG("trigger_amazon_s3_ui",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    load_amazon_s3 = PythonOperator(
        task_id="load_amazon_s3_hook",
        python_callable=check_connection
    )
