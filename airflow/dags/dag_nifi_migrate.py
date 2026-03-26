from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.nifi_tasks import NifiTasks
from common.sensors import WaitJobNiFiSensor

process_group_id = "d206c9fa-019c-1000-0000-000074ce0335"
dag_name = "nifi_migrate_etl"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 1),
}

dag = DAG(
    dag_id=dag_name,  # khai báo id
    default_args=default_args,
    description="NiFi qua API (mTLS): stop trước nếu cần → start PG → chờ idle (status) → stop PG",
    catchup=False,  # không chạy lại các task đã chạy trong quá khứ
    schedule='44 08 * * *',  # lịch chạy (03:45 mỗi ngày)
    tags=['nifi_etl'],
)

nifi_tasks = NifiTasks()

def start_nifi_job_task(**context):
    return nifi_tasks.start_nifi_job(process_group_id, **context)

def stop_nifi_job_task(**context):
    return nifi_tasks.stop_nifi_job(process_group_id, **context)


task_check_stop_nifi_job_task = PythonOperator(
    task_id="check_stop_nifi_job_task",
    python_callable=stop_nifi_job_task,
    dag=dag,
)

# 1. Start NiFi job
task_start_nifi = PythonOperator(
    task_id="start_nifi_job",
    python_callable=start_nifi_job_task,
    dag=dag,
)

# 2. Wait job NiFi — chờ snapshot rảnh (activeThreadCount=0, queuedCount=0)
task_wait_job_nifi = WaitJobNiFiSensor(
    task_id="wait_job_nifi",
    process_group_id=process_group_id,
    poke_interval=15,
    timeout=60 * 60,
    mode="reschedule",
    dag=dag,
)

# 3. Stop NiFi job
task_stop_nifi = PythonOperator(
    task_id="stop_nifi_job",
    python_callable=stop_nifi_job_task,
    dag=dag,
)

task_check_stop_nifi_job_task >> task_start_nifi >> task_wait_job_nifi >> task_stop_nifi


