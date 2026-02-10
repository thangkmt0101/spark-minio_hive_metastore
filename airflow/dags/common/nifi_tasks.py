import os
import sys
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.nifi_token_config import NifiTokenConfig

class NifiTasks:
    """
    Class chứa các task functions cho NiFi DAG.
    """
    def __init__(self, postgres_conn_id: str = "conn_postgresql_migrate"):
        self.postgres_conn_id = postgres_conn_id
        self.nifi_config = NifiTokenConfig()
        
    def get_run_count_start(self, process_group_id, **context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = "SELECT count(*) run_count FROM etl_table_migrate_brand WHERE process_group_id = %s and status IN (1,2)"
        records = hook.get_records(sql, (process_group_id,))
        if not records:
            raise ValueError("Không tìm thấy dòng nào trong bảng etl_table_migrate_brand")
        return records[0][0]
    
    def update_status_run_count_start(self, process_group_id, **context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        sql = "UPDATE etl_table_migrate_brand SET status = 0 WHERE process_group_id = %s"
        cursor.execute(sql, (process_group_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return True


    def get_nifi_token(self, process_group_id, **context):
        """
        Task: Lấy access token từ NiFi API.
        """
        return self.nifi_config.get_nifi_token()

    def start_nifi_job(self, process_group_id, **context):
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        resp = self.nifi_config.get_nifi_session_with_token(
            method="PUT",
            process_group_id=process_group_id,
            state="RUNNING",
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(f"Start NiFi thất bại: {resp.status_code} {resp.text}")

    def stop_nifi_job(self, process_group_id, **context):
        """
        Khi run_count = 2 (sensor ok) thì stop NiFi job.
        """
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")

        resp = self.nifi_config.get_nifi_session_with_token(
            method="PUT",
            process_group_id=process_group_id,
            state="STOPPED",
        )
        if not resp.ok:
            raise RuntimeError(f"Stop NiFi thất bại: {resp.status_code} {resp.text}")


# Instance mặc định để tương thích code cũ
_tasks = NifiTasks()

# Export hàm wrapper để giữ nguyên interface cũ
def get_run_count_start(process_group_id, **context):
    return _tasks.get_run_count_start(process_group_id, **context)


def get_nifi_token(process_group_id, **context):
    return _tasks.get_nifi_token(process_group_id, **context)


def start_nifi_job(process_group_id, **context):
    return _tasks.start_nifi_job(process_group_id, **context)


def stop_nifi_job(process_group_id, **context):
    return _tasks.stop_nifi_job(process_group_id, **context)

