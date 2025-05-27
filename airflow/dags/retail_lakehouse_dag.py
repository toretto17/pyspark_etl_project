from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 27),
    'retries': 1,
}

# Define DAG
with DAG(
    dag_id='retail_lakehouse_etl',
    default_args=default_args,
    description='Retail Lakehouse ETL DAG using PySpark and MinIO',
    schedule_interval=None,
    catchup=False,
    tags=['retail', 'lakehouse', 'pyspark']
) as dag:

    raw_to_bronze = DockerOperator(
        task_id='raw_to_bronze',
        image='pyspark-etl-app',
        container_name='raw_to_bronze',
        api_version='auto',
        auto_remove=True,
        command='spark-submit /opt/spark_etl/src/jobs/raw_to_bronze.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False,
        working_dir='/opt/spark_etl',
        volumes=['/mnt/c/Users/rahul.rs/Desktop/pyspark-etl-project:/opt/spark_etl'],
        tty=True,
        force_pull=False
    )

    bronze_to_silver = DockerOperator(
        task_id='bronze_to_silver',
        image='pyspark-etl-app',
        container_name='bronze_to_silver',
        api_version='auto',
        auto_remove=True,
        command='spark-submit /opt/spark_etl/src/jobs/bronze_to_silver.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False,
        working_dir='/opt/spark_etl',
        volumes=['/mnt/c/Users/rahul.rs/Desktop/pyspark-etl-project:/opt/spark_etl'],
        tty=True,
        force_pull=False
    )

    silver_to_gold = DockerOperator(
        task_id='silver_to_gold',
        image='pyspark-etl-app',
        container_name='silver_to_gold',
        api_version='auto',
        auto_remove=True,
        command='spark-submit /opt/spark_etl/src/jobs/silver_to_gold.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False,
        working_dir='/opt/spark_etl',
        volumes=['/mnt/c/Users/rahul.rs/Desktop/pyspark-etl-project:/opt/spark_etl'],
        tty=True,
        force_pull=False
    )

    # Task dependencies
    raw_to_bronze >> bronze_to_silver >> silver_to_gold
