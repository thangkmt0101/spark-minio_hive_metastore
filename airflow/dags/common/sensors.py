import os
import sys
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.exceptions import AirflowFailException
from common.nifi_tasks import NifiTasks


class WaitJobNiFiSensor(BaseSensorOperator):
    """
    Sensor chờ run_count trong DB tăng từ before -> before + 1
    """

    def __init__(
        self,
        process_group_id,
        target_task_id="check_run_count_start",
        xcom_key="run_count_start",
        postgres_conn_id=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.process_group_id = process_group_id
        self.target_task_id = target_task_id
        self.xcom_key = xcom_key
        self.postgres_conn_id = postgres_conn_id or "conn_postgresql_migrate"
        # Khởi tạo instance NifiTasks
        self.nifi_tasks = NifiTasks(postgres_conn_id=self.postgres_conn_id)

    def poke(self, context):
        if not self.process_group_id:
            raise AirflowFailException("Thiếu process_group_id")

        ti = context["ti"]

        # 1️⃣ Lấy run_count ban đầu từ XCom
        before = ti.xcom_pull(
            task_ids=self.target_task_id,
            key=self.xcom_key
        )

        if before is None:
            raise AirflowFailException(
                f"Không tìm thấy {self.xcom_key} từ task {self.target_task_id}"
            )

        # 2️⃣ Lấy run_count hiện tại từ DB
        try:
            current = self.nifi_tasks.get_run_count_start(self.process_group_id, **context)
            print('wait_job_nifi: ', current)
        except Exception as e:
            self.log.warning(f"Lỗi đọc DB, sẽ retry: {e}")
            return False   # ⬅️ Sensor sẽ poke lại sau

        self.log.info(
            f"Kiểm tra run_count | current={current} = before={before}"
        )

        # 3️⃣ Điều kiện kết thúc
        return current == before   #giá trị hiện tại = giá trị ban đầu

