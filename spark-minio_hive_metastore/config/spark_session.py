from pyspark.sql import SparkSession
from config.settings import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    MINIO_BUCKET, MINIO_SSL,
    PG_CATALOG_URI, PG_CATALOG_USER, PG_CATALOG_PASSWORD, PG_HIVE_METASTORE_URI
)

class SparkSessionBuilder:
    """
    Class tạo SparkSession theo chuẩn Spark + Iceberg + MinIO + PostgreSQL Catalog.
    """

    @staticmethod
    def get_spark(app_name: str = "Spark Iceberg Hive Metastore"):
        spark = (
            SparkSession.builder
                .appName(app_name)
                # Iceberg Catalog (gold)
                .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.gold.type", "hive")
                .config("spark.sql.catalog.gold.type", "hive")
                .config("spark.sql.catalog.gold.uri", PG_HIVE_METASTORE_URI)
                # Iceberg FileIO
                .config("spark.sql.catalog.gold.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                # MinIO warehouse
                .config("spark.sql.catalog.gold.warehouse", f"s3a://{MINIO_BUCKET}/iceberg-warehouse")
                # MinIO S3A configs
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", MINIO_SSL.lower() != "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .getOrCreate()
        )
        return spark

