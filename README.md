# Luồng Xử Lý ETL - Spark MinIO Hive Metastore

## Tổng Quan

Project này thực hiện ETL (Extract, Transform, Load) dữ liệu từ **Iceberg tables** (lưu trên MinIO) vào **SQL Server** sử dụng **Apache Spark** với **Hive Metastore**.

## Kiến Trúc Hệ Thống

```
┌─────────────────┐
│   Iceberg       │
│   (MinIO/S3)    │
│   gold.gold.*   │
└────────┬────────┘
         │
         │ Spark SQL Query
         │
┌────────▼────────┐
│  Apache Spark   │
│  + Iceberg      │
│  + Hive         │
│  Metastore      │
└────────┬────────┘
         │
         │ Transform & Load
         │
┌────────▼────────┐
│   SQL Server    │
│   (Target DB)   │
└─────────────────┘
```

## Luồng Xử Lý Chi Tiết

### 1. Entry Point: `main.py`

**Chức năng:**
- Điểm khởi đầu của ứng dụng
- Nhận tham số `job_type` từ command line hoặc function call
- Khởi tạo và chạy `Etl_Runner`

**Tham số:**
- `job_type` (bắt buộc): Loại job cần chạy (ví dụ: `daily_revenue_summary`)
- `input_date` (tùy chọn): Ngày xử lý dạng `yyyy-mm-dd`, mặc định là hôm qua

**Ví dụ sử dụng:**

- spark-submit .\main.py 1
- spark-submit .\main.py 1 '2025-12-17'


### 2. Khởi Tạo Spark Session: `config/spark_session.py`

**Class: `SparkSessionBuilder`**

**Chức năng:**
- Tạo SparkSession với cấu hình Iceberg + Hive Metastore + MinIO
- Cấu hình catalog `gold` để truy cập Iceberg tables

**Cấu hình chính:**
- **Iceberg Catalog**: `gold` (type: `hive`)
- **Hive Metastore URI**: Kết nối đến Hive Metastore qua Thrift
- **FileIO**: `HadoopFileIO` (tương thích với MinIO)
- **MinIO Warehouse**: `s3a://{bucket}/iceberg-warehouse`
- **MinIO S3A Config**: Access key, secret key, endpoint, path-style access

**Dependencies tự động tải:**
- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1`
- `org.apache.hadoop:hadoop-aws:3.3.4`
- `com.amazonaws:aws-java-sdk-bundle:1.12.262`

### 3. Xử Lý Thời Gian: `config/time_config.py`

**Class: `TimeService`**

**Chức năng:**
- Xử lý và format ngày tháng cho partition Iceberg
- Trả về dictionary với các thông tin: `year`, `month`, `day`, `quarter`, `date_str`

**Logic:**
- Nếu `input_date = None` → Lấy ngày hôm qua
- Nếu `input_date = 'yyyy-mm-dd'` → Sử dụng ngày truyền vào
- Tính toán quarter dựa trên tháng

**Output:**
```python
{
    "year": "2025",
    "month": "01",
    "day": "15",
    "quarter": "Q1 2025",
    "date_str": "2025-01-15"
}
```

### 4. ETL Runner: `jobs/etl_runner.py`

**Class: `Etl_Runner`**

Đây là class chính thực hiện luồng ETL, gồm các bước:

#### 4.1. Khởi Tạo (`__init__`)

**Chức năng:**
- Tạo connection string cho SQL Server (ODBC và JDBC)
- Chuẩn bị JDBC properties cho bulk insert
- Lazy initialization Spark session (tạo khi cần)

**Connection Strings:**
- **ODBC**: Dùng cho `pyodbc` (đọc metadata từ SQL Server)
- **JDBC**: Dùng cho Spark DataFrame write (ghi dữ liệu vào SQL Server)

#### 4.2. Chạy Job (`run`)

**Luồng xử lý:**

##### Bước 1: Lấy Thông Tin Ngày Tháng
```python
info = TimeService.get_target_date(input_date)
y_year = info["year"]
y_quarter = info["quarter"]
y_month = info["month"]
y_day = info["day"]
```

##### Bước 2: Kết Nối SQL Server và Đọc Metadata
- Kết nối SQL Server qua `pyodbc`
- Đọc bảng `view_etl_job` để lấy danh sách job cần chạy
- Filter theo `job_type` và `is_active = 1`
- Lấy thông tin: `table_name`, `sql_path`, `delete_column`, `delete_condition`

**Query:**
```sql
SELECT table_name, sql_path, delete_column, delete_condition
FROM view_etl_job 
WHERE is_active = 1 AND job_type = ?
ORDER BY id
```

##### Bước 3: Xử Lý Từng Job (Loop)

Với mỗi job trong danh sách:

**3.1. Đọc File SQL Template**
- Đọc file SQL từ `sql_path`
- File SQL sử dụng Jinja2 template với các biến: `year`, `quarter`, `month`, `day`
- Render template với giá trị thực tế

**Ví dụ SQL Template:**
```sql
SELECT TRANSPORT_TRANS_ID, ETAG_ID, VEHICLE_ID, 
       CHECKIN_TOLL_ID, CHECKIN_LANE_ID, {{year}} as DATE_ID
FROM gold.gold.fact_transport_transaction_stage
WHERE year = {{year}} AND month = {{month}} AND day = {{day}}
```

**3.2. Thực Thi SQL Query**
- Chạy SQL query bằng `spark.sql()`
- Query đọc dữ liệu từ Iceberg tables (catalog `gold`)
- Tạo temporary view để tái sử dụng
- Đếm số dòng kết quả

**3.3. Xóa Dữ Liệu Cũ (Nếu Có)**
- Nếu `delete_column` được cấu hình → Xóa dữ liệu cũ trước khi insert
- Sử dụng `EtlUtils.delete_by_config()` để xóa theo điều kiện:
  - `day`: Xóa theo ngày (`yyyy-mm-dd`)
  - `month`: Xóa theo tháng (`yyyymm`)
  - `year`: Xóa theo năm (`yyyy`)
  - `time`: Xóa theo khoảng thời gian (`yyyymmdd00` - `yyyymmdd23`)
  - `quarter`: Xóa theo quý (`YYYYQn`)

**3.4. Bulk Insert Dữ Liệu**
- Sử dụng Spark JDBC writer để bulk insert
- Cấu hình tối ưu:
  - `numPartitions`: Số partition để parallel insert (mặc định: 4)
  - `batchsize`: Số dòng insert mỗi batch (mặc định: 100,000)
  - `isolationLevel`: `READ_UNCOMMITTED` (tối ưu performance)

**3.5. Ghi Log**
- Ghi log thành công vào bảng `job_etl_log` với thông tin:
  - Số dòng đã insert
  - Tên bảng, SQL path
  - Thông tin ngày tháng
  - Loại operation (INSERT)

##### Bước 4: Xử Lý Lỗi

- Nếu có lỗi ở bất kỳ bước nào:
  - Ghi log lỗi vào bảng `job_etl_log`
  - Tiếp tục xử lý job tiếp theo (không dừng toàn bộ)
  - Log bao gồm: error message, traceback, table_name, sql_path, operation_type

##### Bước 5: Cleanup

- Đóng connection SQL Server
- Dừng Spark session
- Giải phóng tài nguyên

### 5. Utilities: `config/utils.py`

**Class: `EtlUtils`**

**Chức năng:**
- Format helpers cho các loại điều kiện delete
- Hàm `delete_by_config()`: Xóa dữ liệu theo cấu hình

**Format Functions:**
- `format_day(year, month, day)`: `yyyy-mm-dd`
- `format_month_id(year, month)`: `yyyymm`
- `format_year_id(year)`: `yyyy`
- `format_time_start(year, month, day)`: `yyyymmdd00`
- `format_time_end(year, month, day)`: `yyyymmdd23`

### 6. Logging: `config/job_etl_logger.py`

**Class: `JobLogger`**

**Chức năng:**
- Ghi log vào bảng `job_etl_log` trong SQL Server
- Hỗ trợ 3 mức log: `ERROR`, `WARNING`, `INFO`

**Methods:**
- `log_error()`: Ghi log lỗi
- `log_warning()`: Ghi log cảnh báo
- `log_info()`: Ghi log thông tin

**Thông tin log:**
- `job_name`, `job_type`
- `table_name`, `sql_path`
- `operation_type` (DELETE, INSERT, QUERY, etc.)
- `error_level`, `message`, `error_traceback`
- `year`, `month`, `day`
- `delete_column`, `delete_condition`
- `rows_inserted`, `execution_time_ms`
- `created_at`, `created_by`

## Cấu Hình: `config/settings.py`

**Chức năng:**
- Load các biến môi trường từ file `.env`
- Cung cấp constants cho toàn bộ ứng dụng

**Các cấu hình chính:**
- **MinIO**: Endpoint, access key, secret key, bucket, SSL
- **SQL Server**: Host, port, database, user, password, driver, trust certificate, encrypt
- **Hive Metastore**: URI
- **Spark JDBC**: Batch size, số partitions

## Cấu Trúc Thư Mục

```
spark-minio_hive_metastore/
├── main.py                    # Entry point
├── config/
│   ├── spark_session.py       # Spark session builder
│   ├── settings.py            # Cấu hình từ .env
│   ├── time_config.py         # Xử lý ngày tháng
│   ├── utils.py               # Utilities (delete, format)
│   └── job_etl_logger.py      # Logging
├── jobs/
│   ├── etl_runner.py          # ETL runner chính
│   └── read_iceberg_hive_metastore.py  # Test script
└── sql/
    ├── get_transport_transaction_stage.sql
    ├── baocao_tonghop_doanhthu/
    └── phanhebc_thongke/
```

## Luồng Dữ Liệu

```
1. main.py
   ↓
2. Etl_Runner.__init__()
   - Tạo connection strings
   ↓
3. Etl_Runner.run(job_type, input_date)
   ↓
4. TimeService.get_target_date()
   - Lấy thông tin ngày tháng
   ↓
5. Kết nối SQL Server
   - Đọc view_etl_job
   - Lấy danh sách job
   ↓
6. Với mỗi job:
   ├─ Đọc SQL template
   ├─ Render template (Jinja2)
   ├─ Chạy Spark SQL query
   │  └─ Đọc từ Iceberg (gold.gold.*)
   ├─ Xóa dữ liệu cũ (nếu có)
   ├─ Bulk insert vào SQL Server
   └─ Ghi log
   ↓
7. Cleanup
   - Đóng connections
   - Dừng Spark session
```

## Điểm Quan Trọng

### 1. Lazy Initialization
- Spark session chỉ được tạo khi cần (trong `run()` method)
- Tối ưu tài nguyên

### 2. Connection Reuse
- Tái sử dụng SQL Server connection cho nhiều operations
- Giảm overhead kết nối

### 3. Error Handling
- Mỗi job được xử lý độc lập
- Lỗi ở một job không ảnh hưởng đến job khác
- Tất cả lỗi đều được log vào database

### 4. Bulk Insert Optimization
- Sử dụng Spark JDBC writer với batch size lớn
- Parallel insert với nhiều partitions
- Isolation level `READ_UNCOMMITTED` để tăng tốc

### 5. Template Rendering
- SQL queries sử dụng Jinja2 template
- Dễ dàng parameterize theo ngày tháng
- Hỗ trợ partition pruning trong Iceberg

## Ví Dụ Sử Dụng

### Chạy Job Với Ngày Mặc Định (Hôm Qua)
```bash
python main.py daily_revenue_summary
```

### Chạy Job Với Ngày Cụ Thể
```python
from main import main
main(job_type="daily_revenue_summary", input_date="2025-01-15")
```

### Cấu Hình Job Trong SQL Server

Bảng `view_etl_job` cần có các cột:
- `job_type`: Loại job (ví dụ: `daily_revenue_summary`)
- `table_name`: Tên bảng đích trong SQL Server
- `sql_path`: Đường dẫn file SQL template
- `delete_column`: Cột dùng để xóa dữ liệu cũ
- `delete_condition`: Điều kiện xóa (`day`, `month`, `year`, `time`, `quarter`, `none`)
- `is_active`: Trạng thái job (1 = active, 0 = inactive)

## Lưu Ý

1. **Dependencies**: Spark sẽ tự động tải các JAR packages từ Maven khi khởi động
2. **Hive Metastore**: Phải đảm bảo Hive Metastore đang chạy và accessible
3. **MinIO**: Phải cấu hình đúng endpoint, credentials, và bucket
4. **SQL Server**: Phải có quyền đọc `view_etl_job` và ghi vào các bảng đích
5. **SQL Templates**: Phải sử dụng đúng format Jinja2 và catalog `gold.gold.*`

