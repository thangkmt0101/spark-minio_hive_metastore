import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env từ thư mục config (ưu tiên), nếu không có thì load từ root
env_path_config = Path(__file__).parent / ".env"
env_path_root = Path(__file__).parent.parent / ".env"

if env_path_config.exists():
    load_dotenv(env_path_config)
elif env_path_root.exists():
    load_dotenv(env_path_root)
else:
    # Fallback: load từ thư mục hiện tại hoặc thư mục gốc
    load_dotenv()


def _normalize_encrypt(value: str, default: str = "false") -> str:
    """
    Chuẩn hóa giá trị encrypt/trustServerCertificate về true/false/strict
    để tránh lỗi driver JDBC (không chấp nhận 'yes'/'no').
    """
    if not value:
        return default
    v = value.strip().lower()
    if v in ("true", "1", "y", "yes", "on"):
        return "true"
    if v in ("false", "0", "n", "no", "off"):
        return "false"
    if v == "strict":
        return "strict"
    return default

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_SSL = os.getenv("MINIO_SSL", "false")

INPUT_PATH = os.getenv("INPUT_PATH")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")
# MinIO ETL Data Export Path (source path cho sync từ MinIO)
ETL_DATA_EXPORT_PATH = os.getenv("OUTPUT_PATH")

PG_HIVE_METASTORE_URI = os.getenv("PG_HIVE_METASTORE_URI")
# PostgreSQL Iceberg Catalog
PG_CATALOG_URI = os.getenv("PG_CATALOG_URI")
PG_CATALOG_USER = os.getenv("PG_CATALOG_USER")
PG_CATALOG_PASSWORD = os.getenv("PG_CATALOG_PASSWORD")
PG_HOST = os.getenv("PG_HOST") 
PG_PORT = os.getenv("PG_PORT") 
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Spark JDBC Write Configuration
# Số dòng insert mỗi batch (tối ưu cho bulk insert)
SPARK_JDBC_BATCH_SIZE = int(os.getenv("SPARK_JDBC_BATCH_SIZE", "100000"))
# Số partition để parallel insert (tối ưu performance)
# Có thể tăng lên để tận dụng nhiều connections song song (khuyến nghị: 4-8)
# Với cleanup đúng cách, có thể sử dụng nhiều connections mà không bị leak
SPARK_JDBC_NUM_PARTITIONS = int(os.getenv("SPARK_JDBC_NUM_PARTITIONS", "4"))  # Tăng lên 4 để tận dụng parallel insert
# Tăng connection pool để handle nhiều jobs liên tục
SPARK_JDBC_MAX_POOL_SIZE = int(os.getenv("SPARK_JDBC_MAX_POOL_SIZE", "32"))  # Tăng lên 20