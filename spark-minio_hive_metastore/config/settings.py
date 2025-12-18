import os
from dotenv import load_dotenv

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
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output")

# SQL Server Configuration
# SQL Server 2025: Khuyến nghị sử dụng ODBC Driver 18 hoặc 19 cho SQL Server
# Các driver hỗ trợ: "ODBC Driver 18 for SQL Server", "ODBC Driver 19 for SQL Server", "ODBC Driver 17 for SQL Server"
# Lưu ý: ODBC Driver 18+ yêu cầu TrustServerCertificate=yes hoặc cấu hình SSL certificate
SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT", "1433")
SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE")
SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
SQL_SERVER_DRIVER = os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 18 for SQL Server")
# JDBC driver yêu cầu true/false/strict, không chấp nhận "yes"/"no"
SQL_SERVER_TRUST_SERVER_CERTIFICATE = _normalize_encrypt(
    os.getenv("SQL_SERVER_TRUST_SERVER_CERTIFICATE", "true"),
    default="true"
)
SQL_SERVER_ENCRYPT = _normalize_encrypt(
    os.getenv("SQL_SERVER_ENCRYPT", "false"),
    default="false"
)

PG_HIVE_METASTORE_URI = os.getenv("PG_HIVE_METASTORE_URI")
# PostgreSQL Iceberg Catalog
PG_CATALOG_URI = os.getenv("PG_CATALOG_URI")
PG_CATALOG_USER = os.getenv("PG_CATALOG_USER")
PG_CATALOG_PASSWORD = os.getenv("PG_CATALOG_PASSWORD")

# Spark JDBC Write Configuration
# Số dòng insert mỗi batch (tối ưu cho bulk insert)
SPARK_JDBC_BATCH_SIZE = int(os.getenv("SPARK_JDBC_BATCH_SIZE", "100000"))
# Số partition để parallel insert (tối ưu performance)
SPARK_JDBC_NUM_PARTITIONS = int(os.getenv("SPARK_JDBC_NUM_PARTITIONS", "4"))