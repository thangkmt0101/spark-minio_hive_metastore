from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="spark_iceberg_docker_operator",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark_etl = DockerOperator(
        task_id="run_spark_iceberg_etl",
        image="spark-iceberg-etl:latest",
        api_version="auto",
        auto_remove="success",
        command="/app/entrypoint.sh /app/jobs/main.py 1",
        docker_url="unix://var/run/docker.sock",
        network_mode="app-network",
        mount_tmp_dir=False,

        environment={
        # ===== Spark =====
        "SPARK_MASTER": "spark://spark-master:7077",

        # ===== MinIO =====
        "MINIO_ENDPOINT": "http://10.8.75.82:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",


    },
    )
