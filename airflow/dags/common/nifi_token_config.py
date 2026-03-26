import os
import requests
import urllib3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))  # chỉ định rõ đường dẫn

class NifiTokenConfig:
    """Class gọi NiFi REST API qua mTLS (client cert)."""
    
    def __init__(self):
        # Nếu bạn cung cấp file CA bundle (.pem) thì requests sẽ verify SSL bằng CA đó.
        # Ví dụ: NIFI_CA_CERT_PATH=./certs/nifi-ca.pem hoặc /etc/ssl/certs/nifi-ca.pem
        self.NIFI_CA_CERT_PATH = os.getenv("NIFI_CA_CERT_PATH", "").strip()

        # Client cert mTLS (PEM). Có thể dùng tên đầy đủ hoặc alias ngắn:
        #   NIFI_CLIENT_CERT_PEM_PATH / NIFI_CLIENT_KEY_PEM_PATH
        #   NIFI_CLIENT_CERT / NIFI_CLIENT_KEY
        self.NIFI_CLIENT_CERT_PEM_PATH = (
            os.getenv("NIFI_CLIENT_CERT_PEM_PATH", "").strip()
            or os.getenv("NIFI_CLIENT_CERT", "").strip()
        )
        self.NIFI_CLIENT_KEY_PEM_PATH = (
            os.getenv("NIFI_CLIENT_KEY_PEM_PATH", "").strip()
            or os.getenv("NIFI_CLIENT_KEY", "").strip()
        )

        # Giữ hành vi cũ: mặc định vẫn verify=False để không làm DAG fail khi NiFi dùng self-signed.
        # Khi bạn set NIFI_CA_CERT_PATH thì verify sẽ tự chuyển sang verify=<path CA>.
        self.NIFI_INSECURE_SKIP_VERIFY = os.getenv("NIFI_INSECURE_SKIP_VERIFY", "true").strip().lower() in (
            "1", "true", "yes", "y"
        )

        # URL base của NiFi API
        self.NIFI_URL = os.getenv("NIFI_URL", "").rstrip("/")

    def _get_verify(self):
        """
        Trả về tham số 'verify' cho requests:
        - Nếu có CA file: verify=<đường dẫn .pem>
        - Nếu không có CA: theo NIFI_INSECURE_SKIP_VERIFY (default: False như code cũ)
        """
        if self.NIFI_CA_CERT_PATH:
            ca_path = self.NIFI_CA_CERT_PATH
            # Hỗ trợ truyền đường dẫn tương đối so với thư mục common/
            if not os.path.isabs(ca_path):
                ca_path = os.path.join(BASE_DIR, ca_path)
            return ca_path

        return False if self.NIFI_INSECURE_SKIP_VERIFY else True

    def _verify_cert_for_requests(self):
        """verify + client cert cho requests; tắt warning khi verify=False."""
        verify = self._get_verify()
        if verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return verify, self._get_client_cert()

    def _get_client_cert(self):
        """
        requests sẽ dùng mTLS client cert qua tham số cert=...
        - Nếu bạn cung cấp cert.pem + key.pem: return (cert_path, key_path)
        - Nếu chỉ cung cấp cert.pem: return cert_path (key phải nằm trong cùng PEM)
        """
        cert_path = self.NIFI_CLIENT_CERT_PEM_PATH
        key_path = self.NIFI_CLIENT_KEY_PEM_PATH

        if not cert_path:
            return None

        # requests/urllib3 không đọc được trực tiếp file .p12/.pfx làm client cert.
        # Bạn cần convert sang PEM cert + PEM key (hoặc PEM cert kèm key nằm chung).
        lowered = cert_path.lower()
        if lowered.endswith(".p12") or lowered.endswith(".pfx"):
            raise ValueError(
                "NIFI_CLIENT_CERT_PEM_PATH đang trỏ tới file .p12/.pfx. "
                "requests cần PEM (cert.pem và key.pem). Hãy convert p12 sang PEM trước."
            )

        if not os.path.isabs(cert_path):
            cert_path = os.path.join(BASE_DIR, cert_path)

        if key_path:
            if not os.path.isabs(key_path):
                key_path = os.path.join(BASE_DIR, key_path)
            return (cert_path, key_path)

        # Một file PEM duy nhất có thể chứa cả cert + private key (rời riêng thì set KEY_PEM_PATH)
        return cert_path

    def _fetch_flow_process_group_by_cert_only(self, process_group_id, timeout=30):
        """
        GET /flow/process-groups/{id} — body gồm revision + processGroupFlow.flow (processors, processGroups, ...).
        """
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")

        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")

        verify, cert = self._verify_cert_for_requests()
        url = f"{self.NIFI_URL}/flow/process-groups/{process_group_id}"

        resp = requests.get(url, verify=verify, cert=cert, timeout=timeout)
        if not resp.ok:
            hint = ""
            if resp.status_code == 404:
                hint = (
                    f" Kiểm tra process_group_id đúng với NiFi đang gọi ({self.NIFI_URL}) "
                    "(UUID trên canvas: chuột phải process group → Copy id)."
                )
            raise RuntimeError(
                f"Không lấy được thông tin process group (cert-only): {resp.status_code} {resp.text}.{hint}"
            )

        return resp.json()

    def _get_process_group_status_json(self, process_group_id, timeout=30):
        """GET /flow/process-groups/{id}/status — snapshot runtime (mTLS)."""
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")

        verify, cert = self._verify_cert_for_requests()
        url = f"{self.NIFI_URL}/flow/process-groups/{process_group_id}/status"

        headers = {"Content-Type": "application/json"}

        resp = requests.get(
            url,
            headers=headers,
            verify=verify,
            cert=cert,
            timeout=timeout,
        )
        if not resp.ok:
            raise RuntimeError(
                f"Không lấy được status process group: {resp.status_code} {resp.text}"
            )
        return resp.json()

    def get_process_group_status(self, process_group_id, timeout=30):
        """GET /flow/process-groups/{id}/status — trả về JSON từ NiFi (mTLS)."""
        return self._get_process_group_status_json(process_group_id, timeout=timeout)

    @staticmethod
    def _flow_entity_id_name(entity):
        """Lấy (id, name) từ ProcessorEntity / ProcessGroupEntity / PortDTO... của NiFi."""
        if not isinstance(entity, dict):
            return None, None
        comp = entity.get("component")
        comp = comp if isinstance(comp, dict) else {}
        eid = comp.get("id") or entity.get("id")
        name = comp.get("name") or comp.get("label") or eid
        return eid, name

    def _parse_flow_children(self, data):
        """Từ JSON GET /flow/process-groups/{id} → list kind/id/name."""
        pgf = data.get("processGroupFlow") or {}
        flow = pgf.get("flow") or {}
        if not isinstance(flow, dict):
            return []

        sections = (
            ("process_group", "processGroups"),
            ("processor", "processors"),
            ("input_port", "inputPorts"),
            ("output_port", "outputPorts"),
            ("funnel", "funnels"),
            ("remote_process_group", "remoteProcessGroups"),
        )
        out = []
        for kind, key in sections:
            for entity in flow.get(key) or []:
                eid, name = self._flow_entity_id_name(entity)
                if eid:
                    out.append({"kind": kind, "id": eid, "name": name or eid})
        return out

    def list_process_group_children_by_cert_only(self, process_group_id, timeout=30):
        """
        Liệt kê các thành phần trong process group: process group con, processor, cổng vào/ra, ...
        Trả về list[dict]: kind, id, name
        """
        data = self._fetch_flow_process_group_by_cert_only(process_group_id, timeout=timeout)
        return self._parse_flow_children(data)

    def get_process_group_flow_snapshot_by_cert_only(self, process_group_id, timeout=30):
        """Một lần GET: revision version + danh sách children (processors, PG con, ...)."""
        data = self._fetch_flow_process_group_by_cert_only(process_group_id, timeout=timeout)
        return {
            "revision_version": data["revision"]["version"],
            "children": self._parse_flow_children(data),
        }

    def get_process_group_version(self, process_group_id, timeout=30):
        """GET /flow/process-groups/{id} → revision.version (mTLS)."""
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")
        data = self._fetch_flow_process_group_by_cert_only(process_group_id, timeout=timeout)
        return data["revision"]["version"]

    def get_nifi_session(self, method=None, process_group_id=None, state=None, timeout=30):
        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")
        
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        
        url = f"{self.NIFI_URL}/flow/process-groups/{process_group_id}"

        session = requests.Session()
        verify, cert = self._verify_cert_for_requests()
        session.verify = verify
        session.cert = cert
        session.headers.update({"Content-Type": "application/json"})
        body = None
        if process_group_id and state is not None:
            version = self.get_process_group_version(process_group_id, timeout=timeout)
            # Tạo body với format đúng từ các tham số
            body = {
                "id": process_group_id,
                "revision": {
                    "version": version
                },
                "state": state
            }
        
        # Nếu có method và url thì gửi request luôn
        if method and url:
            if body is not None:
                return session.request(method, url, json=body, timeout=timeout)
            else:
                return session.request(method, url, timeout=timeout)
        
        # Nếu không có method+url thì chỉ trả về session
        return session


# Tạo instance mặc định để tương thích với code cũ
_nifi_config = NifiTokenConfig()

def get_process_group_version(process_group_id, timeout=30):
    return _nifi_config.get_process_group_version(process_group_id, timeout)


def get_process_group_status(process_group_id, timeout=30):
    return _nifi_config.get_process_group_status(process_group_id, timeout)


def get_nifi_session(method=None, process_group_id=None, state=None, timeout=30):
    return _nifi_config.get_nifi_session(method, process_group_id, state, timeout)