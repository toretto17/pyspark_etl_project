import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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

    # Production-level: Fetch MinIO credentials from Airflow Variables
    minio_access_key = Variable.get("MINIO_ACCESS_KEY", default_var="admin")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY", default_var="password123")

    # Common DockerOperator arguments (CORE CONFIG, EXCLUDING 'volumes' or 'environment')
    common_docker_core_config = {
        'image': 'pyspark-etl-app',
        'container_name': '{{ task.task_id }}_{{ ts_nodash }}',
        'api_version': 'auto',
        'auto_remove': True,
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'pyspark-etl-project_retail_lakehouse_network',
        'working_dir': '/app',
        'mount_tmp_dir': False,
        'tty': True,
        'force_pull': False,
    }

    # Environment variables for the Spark app container
    environment_config = {
        'AWS_ACCESS_KEY_ID': minio_access_key,
        'AWS_SECRET_ACCESS_KEY': minio_secret_key,
    }

    # --- MODIFIED SECTION: Re-added --jars and full --conf for Delta/S3A ---
    # This explicit definition on the spark-submit command line
    # is often the most reliable way to ensure Spark picks them up.
    spark_submit_common_args = [
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        # Use --packages to let Spark manage the classpath for these known libraries.
        # Spark often handles these better than explicit --jars, even if jars are present.
        '--packages', 'io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.593',
        '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
        '--conf', f'spark.hadoop.fs.s3a.access.key={minio_access_key}',
        '--conf', f'spark.hadoop.fs.s3a.secret.key={minio_secret_key}',
        '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
        '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog'
    ]
    # --- END MODIFIED SECTION ---

    raw_to_bronze = DockerOperator(
        task_id='raw_to_bronze',
        **common_docker_core_config,
        environment=environment_config,
        command=[
            *spark_submit_common_args,
            '/app/src/jobs/raw_to_bronze.py'
        ]
    )

    bronze_to_silver = DockerOperator(
        task_id='bronze_to_silver',
        **common_docker_core_config,
        environment=environment_config,
        command=[
            *spark_submit_common_args,
            '/app/src/jobs/bronze_to_silver.py'
        ]
    )

    silver_to_gold = DockerOperator(
        task_id='silver_to_gold',
        **common_docker_core_config,
        environment=environment_config,
        command=[
            *spark_submit_common_args,
            '/app/src/jobs/silver_to_gold.py'
        ]
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold