import os
import sys
import requests
import urllib3
from dotenv import load_dotenv

# Tắt cảnh báo InsecureRequestWarning khi dùng verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))  # chỉ định rõ đường dẫn

class NifiTokenConfig:
    """
    Class quản lý token và session cho NiFi API.
    Giữ nguyên logic từ code cũ.
    """
    
    def __init__(self):
        # URL lấy token - có thể override bằng biến môi trường NIFI_TOKEN_URL
        self.NIFI_TOKEN_URL = os.getenv("NIFI_TOKEN_URL", "")
        
        # Lấy username/password từ file .env
        self.NIFI_USERNAME = os.getenv("NIFI_USERNAME", "")
        self.NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "")
        
        # URL base của NiFi API
        self.NIFI_URL = os.getenv("NIFI_URL", "").rstrip("/")
    
    def get_nifi_token(self, username: str = None, password: str = None, timeout: int = 30) -> str:
        # Lấy username/password từ tham số hoặc từ biến môi trường
        username = username or self.NIFI_USERNAME
        password = password or self.NIFI_PASSWORD
        
        if not username or not password:
            raise ValueError("Thiếu username hoặc password. Cấu hình trong file .env hoặc truyền qua tham số")
        
        data = {
            "username": username,
            "password": password,
        }

        # verify=False vì thường dùng self-signed cert; nếu có cert chuẩn thì đổi thành True
        resp = requests.post(self.NIFI_TOKEN_URL, data=data, verify=False, timeout=timeout)
        resp.raise_for_status()

        token = resp.text.strip()
        if not token:
            raise ValueError("Không nhận được token từ NiFi (response rỗng)")
        return token

    def get_process_group_version(self, process_group_id, token=None, timeout=30):
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        
        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")
        
        if token is None:
            token = self.get_nifi_token()

        # Tạo HEADERS với token
        HEADERS = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        # URL với endpoint /status
        url = f"{self.NIFI_URL}/flow/process-groups/{process_group_id}"
        
        resp = requests.get(url, headers=HEADERS, verify=False, timeout=timeout)
        
        if not resp.ok:
            raise RuntimeError(f"Không lấy được thông tin process group: {resp.status_code} {resp.text}")
        
        data = resp.json()
        version = data["revision"]["version"]  # ✅ ĐÚNG
        return version

    def get_nifi_session_with_token(self, token=None, method=None, process_group_id=None, state=None, timeout=30):
        if not self.NIFI_URL:
            raise ValueError("Thiếu NIFI_URL. Cấu hình trong file .env")
        
        if not process_group_id:
            raise ValueError("Thiếu process_group_id")
        
        url = f"{self.NIFI_URL}/flow/process-groups/{process_group_id}"
        if token is None:
            token = self.get_nifi_token()
        
        session = requests.Session()
        session.verify = False  # NiFi thường dùng self-signed cert
        # Set header Authorization với format: Bearer {token}
        session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        # Tạo body từ các tham số process_group_id, version, state nếu được truyền vào
        body = None
        if process_group_id and state is not None:
            # Nếu chưa có version, tự động lấy version từ API
            version = self.get_process_group_version(process_group_id, token=token, timeout=timeout)
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

# Export các hàm để tương thích với code cũ (giữ nguyên interface)
def get_nifi_token(username: str = None, password: str = None, timeout: int = 30) -> str:
    return _nifi_config.get_nifi_token(username, password, timeout)

def get_process_group_version(process_group_id, token=None, timeout=30):
    return _nifi_config.get_process_group_version(process_group_id, token, timeout)

def get_nifi_session_with_token(token=None, method=None, process_group_id=None, state=None, timeout=30):
    return _nifi_config.get_nifi_session_with_token(token, method, process_group_id, state, timeout)
 