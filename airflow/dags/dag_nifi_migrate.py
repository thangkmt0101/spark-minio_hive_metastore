import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.nifi_tasks import NifiTasks
from common.sensors import WaitJobNiFiSensor

POSTGRES_CONN_ID = "conn_postgresql_migrate"
process_group_id = "45aba4e9-019c-1000-fe7a-ad0b7784d4df"
dag_name = 'nifi_migrate_etl'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 1),
}

dag = DAG(
    dag_id=dag_name,  # khai báo id
    default_args=default_args,
    description="DAG chạy NiFi qua API, kiểm soát theo run_count trong bảng etl_table_migrate",
    catchup=False,  # không chạy lại các task đã chạy trong quá khứ
    schedule='44 08 * * *',  # lịch chạy (03:45 mỗi ngày)
    tags=['nifi_etl'],
)

# Khởi tạo instance của NifiTasks
nifi_tasks = NifiTasks(postgres_conn_id=POSTGRES_CONN_ID)

def check_run_count_start(**context):
    """
    Kiểm tra run_count yêu cầu = 1 trước khi start NiFi.
    Lưu run_count vào XCom với key 'run_count_start'.
    """
    run_count = nifi_tasks.get_run_count_start(process_group_id, **context)
    context["ti"].xcom_push(key="run_count_start", value=run_count)
    print('check_run_count_start: ', run_count)
    nifi_tasks.update_status_run_count_start(process_group_id, **context)
    return run_count

def start_nifi_job_task(**context):
    return nifi_tasks.start_nifi_job(process_group_id, **context)

def stop_nifi_job_task(**context):
    return nifi_tasks.stop_nifi_job(process_group_id, **context)
    
def check_stop_nifi_job_task(**context):
    return nifi_tasks.stop_nifi_job(process_group_id, **context)


# 0. Check run_count = 1 và lưu XCom
task_check_stop_nifi_job_task = PythonOperator(
    task_id="check_stop_nifi_job_task",
    python_callable=check_stop_nifi_job_task,
    dag=dag,
)
    
# 0. Check run_count = 1 và lưu XCom
task_check_run_count_start = PythonOperator(
    task_id="check_run_count_start",
    python_callable=check_run_count_start,
    dag=dag,
)

# 1. Start NiFi job
task_start_nifi = PythonOperator(
    task_id="start_nifi_job",
    python_callable=start_nifi_job_task,
    dag=dag,
)

# 2. Wait job NiFi (chờ run_count tăng lên)
task_wait_job_nifi = WaitJobNiFiSensor(
    task_id="wait_job_nifi",
    process_group_id=process_group_id,
    target_task_id="check_run_count_start",
    xcom_key="run_count_start",
    postgres_conn_id=POSTGRES_CONN_ID,
    poke_interval=15,  # Check mỗi 30 giây
    timeout=60 * 60,  # Timeout 1 giờ
    mode="reschedule",
    dag=dag,
)

# 3. Stop NiFi job
task_stop_nifi = PythonOperator(
    task_id="stop_nifi_job",
    python_callable=stop_nifi_job_task,
    dag=dag,
)

# Thiết lập dependencies: check -> start -> wait -> stop
task_check_stop_nifi_job_task >> task_check_run_count_start >> task_start_nifi >> task_wait_job_nifi >> task_stop_nifi


