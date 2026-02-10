from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

POSTGRES_CONN_ID = "conn_postgresql_migrate"
dag_name = 'dag_run_spark_minio'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 1),
}

dag = DAG(
    dag_id=dag_name,  # khai báo id
    default_args=default_args,
    description="DAG chạy NiFi qua API, kiểm soát theo run_count trong bảng etl_table_migrate",
    catchup=False,  # không chạy lại các task đã chạy trong quá khứ
    schedule='*/25 * * * *',  # ⏱ chạy mỗi 5 phút
    tags=['nifi_etl'],
)

task_run_spark_minio = BashOperator(
    task_id="task_1",
    bash_command="""
    docker exec spark-iceberg-etl /app/entrypoint.sh /app/jobs/main.py 1 {{ ds }}
    """,
    dag=dag,
)