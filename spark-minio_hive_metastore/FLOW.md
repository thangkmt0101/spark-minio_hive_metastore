# FLOW XỬ LÝ ETL JOB

## Tổng quan

Dự án ETL sử dụng Apache Spark để xử lý dữ liệu từ Iceberg tables (lưu trên MinIO) và export kết quả ra CSV, sau đó sync về local filesystem.

## Entry Point

**File:** `jobs/main.py`

- Nhận 2 tham số từ command line:
  - `job_type` (bắt buộc): Loại job để filter (ví dụ: '1' = Daily, '2' = Week, '3' = Month, '4' = Quarter, '5' = Year)
  - `input_date` (tùy chọn): Ngày xử lý dạng 'yyyy-mm-dd'. Nếu không có thì mặc định = hôm qua

- Khởi tạo `Etl_Runner` và gọi method `run()`
- Cleanup Spark resources sau khi job kết thúc

## Flow Xử Lý Chính

**File:** `jobs/etl_runner.py` - Class `Etl_Runner`

### 1. Khởi tạo

- Tạo Spark Session với cấu hình Iceberg + MinIO + PostgreSQL Catalog
- Kết nối PostgreSQL để đọc cấu hình jobs từ bảng `view_etl_job`

### 2. Xử lý Thời Gian

**File:** `config/time_config.py` - Class `TimeService`

- Lấy thông tin ngày từ `input_date` hoặc mặc định = hôm qua
- Xác định mức thời gian dựa trên `job_type`:
  - `job_type = 1`: Daily - lấy UTC range cho 1 ngày
  - `job_type = 2`: Week - lấy UTC range cho cả tuần (thứ 2 đến chủ nhật)
  - `job_type = 3`: Month - lấy UTC range cho cả tháng
  - `job_type = 4`: Quarter - lấy UTC range cho cả quý
  - `job_type = 5`: Year - lấy UTC range cho cả năm

- Chuyển đổi từ giờ VN (UTC+7) sang UTC để query dữ liệu đúng

### 3. Đọc Danh Sách Jobs

- Query từ bảng `view_etl_job` với điều kiện:
  - `is_active = true`
  - `job_type = <job_type truyền vào>`
- Lấy các thông tin: `table_name`, `sql_path`
- Sắp xếp theo `id` để đảm bảo thứ tự xử lý

### 4. Xử Lý Từng Job

Với mỗi job trong danh sách:

#### 4.1. Đọc File SQL

- Đọc file SQL từ `sql_path`
- Kiểm tra file tồn tại, nếu không có thì log lỗi và bỏ qua job này

#### 4.2. Render Template SQL

**Template Engine:** Jinja2

- Render SQL template với các biến:
  - `year_utc_7`, `quarter_utc_7`, `month_utc_7`, `week_utc_7`, `day_utc_7`: Thông tin ngày theo UTC+7
  - `year`, `quarter`, `month`, `week`: Thông tin UTC
  - `day_ranges`: List các dict chứa `{year, month, day, hour_start, hour_end}` để loop trong SQL

#### 4.3. Chạy SQL Query

- Sử dụng `spark.sql()` để chạy SQL query đã render
- Nếu lỗi thì log vào database và bỏ qua job này

#### 4.4. Kiểm Tra Dữ Liệu

- Chỉ scan 1 record (`df.limit(1).count()`) để kiểm tra có dữ liệu không
- Nếu không có dữ liệu thì skip job này
- Nếu executor crash thì dừng toàn bộ job

#### 4.5. Export DataFrame Sang CSV

**File:** `config/utils.py` - Method `EtlUtils.export_dataframe_to_csv()`

- Export DataFrame sang CSV trên MinIO (S3A)
- Đường dẫn: `s3a://{MINIO_BUCKET}/{OUTPUT_PATH}/{table_name}/{date_str}/`
- Format: CSV với delimiter `|`, encoding UTF-8
- Spark sẽ tạo nhiều part files (part-*.csv) tùy theo số partitions

#### 4.6. Gộp và Đổi Tên File

**File:** `config/utils.py` - Method `EtlUtils.merge_and_rename_files_in_minio()`

- Sử dụng MinIO Client (`mc`) để:
  - List tất cả part files (part-*.csv) trên MinIO
  - Download và gộp các part files thành 1 file duy nhất
  - Upload file đã gộp với tên `{table_name}.csv`
  - Xóa các part files cũ

#### 4.7. Sync Từ MinIO Về Local

**File:** `config/utils.py` - Method `EtlUtils.sync_from_minio()`

- Sử dụng `mc mirror` để sync dữ liệu từ MinIO về local filesystem
- Đường dẫn destination: `/data/etl_data_export/{table_name}/{date_str}/`
- Xóa thư mục destination cũ trước khi sync để đảm bảo dữ liệu sạch

#### 4.8. Xóa Dữ Liệu Cũ

**File:** `config/utils.py` - Method `EtlUtils.delete_old_data_from_minio()`

- Xóa dữ liệu cũ từ MinIO (mặc định: 3 ngày trước và tất cả các ngày trước đó)
- Sử dụng `mc rm --recursive` để xóa các thư mục ngày cũ

#### 4.9. Ghi Log

**File:** `config/job_etl_logger.py` - Class `JobLogger`

- Ghi log thành công/thất bại vào bảng `etl_job_log` trong PostgreSQL
- Thông tin log: job_name, job_type, table_name, sql_path, operation_type, error_level, message, error_traceback, execution_time_ms, year, month, day

#### 4.10. Cleanup Memory

- Unpersist DataFrame để giải phóng memory
- Clear Spark catalog cache
- Tính toán thời gian execution cho job con này

### 5. Xử Lý Lỗi

- **Lỗi nhẹ** (SQL, Iceberg, S3): Log lỗi và tiếp tục với job tiếp theo
- **Lỗi nghiêm trọng** (Executor crash): Dừng toàn bộ job ngay lập tức
- Tất cả lỗi đều được log vào database với stack trace đầy đủ

### 6. Cleanup Cuối Cùng

- Đóng PostgreSQL connection
- Clear Spark catalog cache
- Stop Spark session
- Đợi 2 giây để đảm bảo cleanup hoàn tất

## Cấu Hình Spark

**File:** `config/spark_session.py` - Class `SparkSessionBuilder`

### Cấu hình chính:

- **Iceberg Catalog**: Sử dụng Hive Metastore (PostgreSQL) để quản lý metadata
- **MinIO S3A**: Cấu hình kết nối MinIO như S3-compatible storage
- **Warehouse**: `s3a://{MINIO_BUCKET}/iceberg-warehouse`
- **Tối ưu hiệu suất**:
  - Adaptive Query Execution (AQE): Tự động tối ưu execution plan
  - Shuffle partitions: 16 partitions mặc định
  - Memory management: 80% memory cho execution và storage
  - G1GC cho memory lớn
  - S3A optimization: Fast upload, multipart upload

## Cấu Hình Môi Trường

**File:** `config/settings.py`

- Load biến môi trường từ `.env` (ưu tiên `config/.env`, sau đó root `.env`)
- Các biến quan trọng:
  - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`
  - `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`
  - `PG_HIVE_METASTORE_URI`: URI của Hive Metastore
  - `OUTPUT_PATH`: Đường dẫn output trên MinIO
  - `SPARK_JDBC_BATCH_SIZE`, `SPARK_JDBC_NUM_PARTITIONS`

## Cách Chạy

### Với Docker Compose:

```bash
docker-compose exec spark-iceberg-etl /app/entrypoint.sh /app/jobs/main.py <job_type> [input_date]
```

Ví dụ:
```bash
# Chạy job Daily với ngày mặc định (hôm qua)
docker-compose exec spark-iceberg-etl /app/entrypoint.sh /app/jobs/main.py 1

# Chạy job Daily với ngày cụ thể
docker-compose exec spark-iceberg-etl /app/entrypoint.sh /app/jobs/main.py 1 2025-01-15
```

### Entrypoint Script

**File:** `entrypoint.sh`

- Script wrapper cho `spark-submit`
- Tự động thêm các JAR files cần thiết vào `--jars`
- Cấu hình Spark Master, deploy mode, executor resources

## Lưu Ý

1. **Job Type là bắt buộc**: Phải truyền `job_type` khi chạy job
2. **Input Date**: Nếu không truyền thì mặc định = hôm qua
3. **Error Handling**: Lỗi executor crash sẽ dừng toàn bộ job, các lỗi khác chỉ log và tiếp tục
4. **Memory Management**: Tự động cleanup DataFrame và cache sau mỗi job để tránh memory leak
5. **MinIO Operations**: Sử dụng MinIO Client (`mc`) để thao tác với MinIO (merge, sync, delete)
6. **Logging**: Tất cả operations đều được log vào PostgreSQL với đầy đủ thông tin

