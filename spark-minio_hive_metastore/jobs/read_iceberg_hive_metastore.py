from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("Iceberg Hive Metastore")
        .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.silver.type", "hive")
        .config("spark.sql.catalog.silver.uri", "thrift://10.8.75.82:9083")
        .config("spark.sql.catalog.silver.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # MinIO warehouse
        .config("spark.sql.catalog.silver.warehouse", "s3a://csdlgt/iceberg-warehouse")
        # MinIO S3 config
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://10.8.75.82:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
)

# df = spark.read.table("silver.silver.transport_transaction_stage")
# df.show()
spark.sql("SHOW CATALOGS;").show()
spark.sql("SHOW NAMESPACES IN silver;").show()
spark.sql("SHOW TABLES IN silver.silver").show()
