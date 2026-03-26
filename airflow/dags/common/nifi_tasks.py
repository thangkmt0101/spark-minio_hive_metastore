from common.nifi_token_config import NifiTokenConfig


class NifiTasks:
    """Điều khiển NiFi (start/stop process group) qua NifiTokenConfig (mTLS)."""

    def __init__(self):
        self.nifi_config = NifiTokenConfig()

    def start_nifi_job(self, process_group_id, **context):
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        resp = self.nifi_config.get_nifi_session(
            method="PUT",
            process_group_id=process_group_id,
            state="RUNNING",
            timeout=30,
        )
        if not resp.ok:
            raise RuntimeError(f"Start NiFi thất bại: {resp.status_code} {resp.text}")

    def stop_nifi_job(self, process_group_id, **context):
        """PUT state=STOPPED. 404 (PG không tồn tại trên cụm) → bỏ qua, coi như đã tắt."""
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")

        try:
            resp = self.nifi_config.get_nifi_session(
                method="PUT",
                process_group_id=process_group_id,
                state="STOPPED",
            )
        except RuntimeError as e:
            err = str(e)
            if "404" in err or "Unable to locate group" in err:
                print(
                    f"stop_nifi_job: bỏ qua — process group không có trên NiFi: {process_group_id}. {err}"
                )
                return None
            raise
        if not resp.ok:
            raise RuntimeError(f"Stop NiFi thất bại: {resp.status_code} {resp.text}")


_tasks = NifiTasks()


def start_nifi_job(process_group_id, **context):
    return _tasks.start_nifi_job(process_group_id, **context)


def stop_nifi_job(process_group_id, **context):
    return _tasks.stop_nifi_job(process_group_id, **context)
