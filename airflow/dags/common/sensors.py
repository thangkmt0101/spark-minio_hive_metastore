from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.exceptions import AirflowFailException
from common.nifi_token_config import NifiTokenConfig


def _aggregate_snapshot_from_status(status_json):
    """
    Chỉ dùng processGroupStatus.aggregateSnapshot.
    Các mảng connectionStatusSnapshots / processorStatusSnapshots nằm trong object
    đó nhưng sensor không duyệt hay cộng dồn từ chúng — chỉ đọc các field tổng hợp gốc.
    """
    if not isinstance(status_json, dict):
        return {}
    pgs = status_json.get("processGroupStatus")
    if not isinstance(pgs, dict):
        return {}
    snap = pgs.get("aggregateSnapshot")
    return snap if isinstance(snap, dict) else {}


def _parse_nifi_metric_int(val):
    """
    NiFi trả activeThreadCount kiểu int; queuedCount thường là str \"0\"
    hoặc có dấu phẩy / kèm text (\"0 (0 bytes)\"). Trả None nếu không parse được.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, int):
        return val
    s = str(val).strip().replace(",", "")
    if not s:
        return None
    n = 0
    while n < len(s) and s[n].isdigit():
        n += 1
    if n == 0:
        return None
    return int(s[:n])


def _idle_metrics_from_snapshot(snap):
    """
    Chỉ các field tổng hợp trực tiếp trên aggregateSnapshot (cùng cấp với
    connectionStatusSnapshots / processorStatusSnapshots), không đọc phần tử trong hai mảng đó.
    - active: activeThreadCount, nếu thiếu thì statelessActiveThreadCount
    - queued: chỉ queuedCount
    """
    active = _parse_nifi_metric_int(snap.get("activeThreadCount"))
    if active is None:
        active = _parse_nifi_metric_int(snap.get("statelessActiveThreadCount")) or 0

    queued = _parse_nifi_metric_int(snap.get("queuedCount"))
    if queued is None:
        queued = 0

    return active, queued


class WaitJobNiFiSensor(BaseSensorOperator):
    """
    Chờ PG rảnh theo processGroupStatus.aggregateSnapshot:
    activeThreadCount == 0 và queuedCount == 0 (chỉ field gốc trên snapshot,
    không lấy từ connectionStatusSnapshots hay processorStatusSnapshots).
    """

    def __init__(
        self,
        process_group_id,
        request_timeout=30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.process_group_id = process_group_id
        self.request_timeout = request_timeout
        self._nifi = NifiTokenConfig()

    def poke(self, context):
        if not self.process_group_id:
            raise AirflowFailException("Thiếu process_group_id")

        try:
            status = self._nifi.get_process_group_status(
                self.process_group_id,
                timeout=self.request_timeout,
            )
        except Exception as e:
            self.log.warning("Lỗi gọi get_process_group_status, sẽ retry: %s", e)
            return False

        snap = _aggregate_snapshot_from_status(status)
        if not snap:
            self.log.warning(
                "Không tìm thấy processGroupStatus.aggregateSnapshot trong response, retry"
            )
            return False

        raw_active = snap.get("activeThreadCount")
        raw_queued = snap.get("queuedCount")
        active, queued = _idle_metrics_from_snapshot(snap)

        idle = active == 0 and queued == 0
        msg = (
            f"[WaitJobNiFiSensor] PG={self.process_group_id} | "
            f"activeThreadCount={active} (raw={raw_active!r}) "
            f"queuedCount={queued} (raw={raw_queued!r}) | idle={idle}"
        )
        print(msg)
        self.log.info(msg)

        return idle
