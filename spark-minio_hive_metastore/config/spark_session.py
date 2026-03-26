import os
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
        driver_host = os.getenv("SPARK_DRIVER_HOST")
        driver_bind_address = os.getenv("SPARK_DRIVER_BIND_ADDRESS")

        spark = (
            SparkSession.builder
                .appName(app_name)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.registrationRequired", "false")
                .config("spark.kryo.unsafe", "false")  # Tắt unsafe mode để tương thích tốt hơn
                .config("spark.kryoserializer.buffer.max", "256m")  # Tăng buffer size cho large objects
                .config("spark.sql.iceberg.vectorization.enabled", "false")  # Tắt vectorization để tránh lỗi serialization với Iceberg
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config(
                    "spark.sql.sources.commitProtocolClass",
                    "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
                ) # Để Spark tự động commit file CSV sau khi write
                # ===== Tối ưu hiệu suất cho dữ liệu lớn =====
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                # Adaptive Query Execution (AQE) - Tự động tối ưu query execution
                .config("spark.sql.adaptive.enabled", "true")  # Bật AQE để tự động tối ưu execution plan
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # Tự động gộp partitions nhỏ
                .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")  # Kích thước partition tối thiểu sau khi gộp
                .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "16")  # Số partitions ban đầu (tối ưu cho 5-10GB)
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")  # Kích thước partition đề xuất (tối ưu cho 5-10GB)
                .config("spark.sql.adaptive.skewJoin.enabled", "false")  # Tắt skew join (không cần config liên quan)
                
                # Shuffle optimization
                .config("spark.sql.shuffle.partitions", "16")  # Số partitions mặc định cho shuffle (tối ưu cho 5-10GB, 2-4 cores)
                .config("spark.sql.adaptive.maxNumPostShufflePartitions", "32")  # Số partitions tối đa sau shuffle (tối ưu cho 5-10GB)
                
                # Network và timeout settings để tránh treo job
                # Tăng timeout để xử lý shuffle và operations lớn
                .config("spark.driver.host", driver_host)
                .config("spark.driver.bindAddress", driver_bind_address)
                .config("spark.driver.port", os.getenv("SPARK_DRIVER_PORT", "42315"))
                .config("spark.blockManager.port", os.getenv("SPARK_BLOCKMANAGER_PORT", "42316"))
                .config("spark.network.timeout", "600s")  # Timeout cho network operations (10 phút)
                .config("spark.executor.heartbeatInterval", "30s")  # Heartbeat interval cho executor
                .config("spark.executor.heartbeatTimeout", "600s")  # Timeout cho executor heartbeat (10 phút) - phải >= network.timeout
                .config("spark.sql.execution.timeout", "600")  # Timeout cho SQL execution (10 phút)

                # Shuffle timeout settings để tránh treo ở shuffle operations
                .config("spark.shuffle.io.connectionTimeout", "300s")  # Timeout cho shuffle connections (5 phút)
                .config("spark.shuffle.io.retryWait", "10s")  # Thời gian chờ giữa các retry khi shuffle fail
                .config("spark.shuffle.io.maxRetries", "1")  # Số lần retry tối đa cho shuffle
                .config("spark.shuffle.io.backLog", "4096")  # Tăng backlog để xử lý nhiều connections hơn
                .config("spark.shuffle.service.enabled", "false")  # Tắt shuffle service (khi bật sẽ gây lỗi executor lost)
                
                # Parquet optimization
                .config("spark.sql.parquet.compression.codec", "snappy")  # Compression codec cho Parquet (snappy cân bằng tốc độ/kích thước)
                .config("spark.sql.parquet.columnarReaderBatchSize", "4096")  # Batch size cho columnar reader
                
                # Columnar storage và caching
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "100000")  # Batch size cho columnar storage
                
                # S3/MinIO optimization
                .config("spark.hadoop.fs.s3a.fast.upload", "true")  # Fast upload cho S3/MinIO
                .config("spark.hadoop.fs.s3a.multipart.size", "67108864")  # 64MB cho multipart upload
                .config("spark.hadoop.fs.s3a.multipart.threshold", "67108864")  # Threshold để dùng multipart
                .config("spark.hadoop.fs.s3a.block.size", "134217728")  # 128MB block size
                .config("spark.hadoop.fs.s3a.connection.maximum", "16")  # Số connections tối đa (tối ưu cho 5-10GB)
                .config("spark.hadoop.fs.s3a.threads.max", "8")  # Số threads tối đa cho S3A (tối ưu cho 5-10GB)
                .config("spark.hadoop.fs.s3a.readahead.range", "65536")  # Readahead range
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")  # Timeout kết nối (ms)
                
                # Memory management
                .config("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB max partition size (tối ưu cho 5-10GB)
                .config("spark.sql.files.openCostInBytes", "4194304")  # 4MB cost để mở file
                
                # Join optimization
                .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")  # Dynamic partition pruning
                .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB - Tăng threshold để broadcast join nhiều hơn (giảm shuffle)
                .config("spark.sql.broadcastTimeout", "600")  # 10 phút - Timeout cho broadcast join
                
                # Memory optimization cho 5-10GB data
                .config("spark.memory.fraction", "0.8")  # 80% memory cho execution và storage
                .config("spark.memory.storageFraction", "0.3")  # 30% storage memory (có thể reclaim)
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Tắt Arrow cho tương thích tốt hơn
                
                # Garbage Collection optimization - G1GC cho memory lớn (5-10GB)
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")  # G1GC tối ưu cho memory lớn
                
                # Speculation settings - Tắt để tránh duplicate tasks (tốn CPU)
                .config("spark.speculation", "false")  # Tắt speculation để tránh duplicate tasks
                
                # Dynamic allocation - Tắt để ổn định hơn (executors cố định)
                .config("spark.dynamicAllocation.enabled", "false")  # Tắt dynamic allocation để giữ executors cố định
                
                # Iceberg Catalog (gold)
                .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ice.type", "hive")
                .config("spark.sql.catalog.ice.uri", PG_HIVE_METASTORE_URI)
                # Iceberg FileIO
                .config("spark.sql.catalog.ice.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
                # MinIO warehouse
                .config("spark.sql.catalog.ice.warehouse", f"s3a://{MINIO_BUCKET}")
                # MinIO S3A configs
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled","true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .getOrCreate()
        )
        
        print(f"[SPARK SESSION] SparkSession created successfully: {app_name}")

        # try:
        #     # Thử liệt kê các bảng trong catalog Iceberg
        #     print("--- Đang kiểm tra kết nối MinIO qua Iceberg Catalog ---")
        #     spark.sql("SHOW NAMESPACES IN ice").show()
        #     print("=> Kết nối Iceberg Catalog (Hive Metastore) THÀNH CÔNG.")

        #     # Thử tạo một metadata file nhỏ hoặc kiểm tra file hệ thống S3A
        #     sc = spark.sparkContext
        #     Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        #     FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        #     conf = sc._jsc.hadoopConfiguration()
        #     fs = FileSystem.get(Path(f"s3a://{MINIO_BUCKET}/").toUri(), conf)

        #     if fs.exists(Path(f"s3a://{MINIO_BUCKET}/")):
        #         print(f"=> Kết nối S3A tới bucket '{MINIO_BUCKET}' THÀNH CÔNG.")
        # except Exception as e:
        #     print(f"!!! LỖI KẾT NỐI: {str(e)}")

        return spark

