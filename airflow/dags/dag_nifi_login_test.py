import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
import os

from common.nifi_token_config import NifiTokenConfig
from common.nifi_tasks import NifiTasks


@dag(
    dag_id="nifi_login_test",
    start_date=pendulum.now().subtract(days=1),
    schedule=None,  # chạy thủ công để test
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description="Test mTLS NiFi + liệt kê flow; PUT RUNNING/STOPPED (start/stop_nifi_job).",
    tags=["nifi_test"],
)
def nifi_login_test():
    @task
    def check_mtls():
        cfg = NifiTokenConfig()

        try:
            process_group_id = os.getenv(
                "NIFI_TEST_PROCESS_GROUP_ID",
                "d206c9fa-019c-1000-0000-000074ce0335",
            )
            snap = cfg.get_process_group_flow_snapshot_by_cert_only(process_group_id)
        except Exception as e:
            raise AirflowFailException(f"NiFi cert-only call failed: {type(e).__name__}: {e}")

        version = snap["revision_version"]
        children = snap["children"]
        print(f"NiFi cert-only OK. process_group_id={process_group_id} revision_version={version}")
        print("kind\tid\tname")
        for row in sorted(children, key=lambda r: (r["kind"], r["name"] or "")):
            print(f"{row['kind']}\t{row['id']}\t{row['name']}")
        return {"revision_version": version, "children": children}

    @task
    def start_nifi_job_test():
        """Gọi cùng logic start_nifi_job như dag_nifi_migrate (PUT state=RUNNING, mTLS)."""
        # Mặc định bật start; đặt NIFI_TEST_ENABLE_START=false để chỉ chạy check_mtls + liệt kê flow.
        if os.getenv("NIFI_TEST_ENABLE_START", "true").strip().lower() in (
            "0", "false", "no", "n",
        ):
            print("Bỏ qua start_nifi_job (NIFI_TEST_ENABLE_START=false).")
            return None

        process_group_id = os.getenv(
            "NIFI_TEST_PROCESS_GROUP_ID",
            "d206c9fa-019c-1000-0000-000074ce0335",
        )
        nifi = NifiTasks()
        try:
            nifi.start_nifi_job(process_group_id)
        except Exception as e:
            raise AirflowFailException(f"start_nifi_job failed: {type(e).__name__}: {e}")
        print(f"start_nifi_job OK. process_group_id={process_group_id}")
        return process_group_id

    @task
    def stop_nifi_job_test():
        """Gọi cùng logic stop_nifi_job như dag_nifi_migrate (PUT state=STOPPED, mTLS)."""
        if os.getenv("NIFI_TEST_ENABLE_STOP", "true").strip().lower() in (
            "0", "false", "no", "n",
        ):
            print("Bỏ qua stop_nifi_job (NIFI_TEST_ENABLE_STOP=false).")
            return None

        process_group_id = os.getenv(
            "NIFI_TEST_PROCESS_GROUP_ID",
            "d206c9fa-019c-1000-0000-000074ce0335",
        )
        nifi = NifiTasks()
        try:
            nifi.stop_nifi_job(process_group_id)
        except Exception as e:
            raise AirflowFailException(f"stop_nifi_job failed: {type(e).__name__}: {e}")
        print(f"stop_nifi_job OK. process_group_id={process_group_id}")
        return process_group_id

    check_mtls() >> start_nifi_job_test() >> stop_nifi_job_test()


nifi_login_test()

