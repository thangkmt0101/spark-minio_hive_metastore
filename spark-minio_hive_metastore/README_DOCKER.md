# Hướng dẫn Build và Chạy Docker Image cho ETL Project

## Tổng quan

Dự án ETL sử dụng Apache Spark 3.5.0 để xử lý dữ liệu từ Iceberg tables (lưu trên MinIO) và export kết quả ra CSV.

## Các bước Build Docker Image

### 1. Base Image

- Sử dụng base image: `apache/spark:3.5.0`
- Chuyển sang user root để cài đặt packages

### 2. Cài đặt System Dependencies

Cài đặt các packages hệ thống cần thiết:

```bash
apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    gcc \
    g++ \
    curl \
    gnupg \
    wget
```

**Mục đích:**
- `python3-pip`, `python3-dev`: Python và pip để cài đặt Python packages
- `gcc`, `g++`: Compiler để build các Python packages cần compile
- `curl`, `wget`: Download files từ internet
- `gnupg`: GPG keys cho package verification

### 3. Cài đặt MinIO Client (mc)

```bash
wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc
chmod +x /usr/local/bin/mc
```

**Mục đích:** Cài đặt MinIO Client để thao tác với MinIO (merge files, sync, delete)

### 4. Setup Working Directory

```bash
WORKDIR /app
```

Tạo và chuyển vào thư mục làm việc `/app`

### 5. Cài đặt Python Dependencies

```bash
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
```

- Copy file `requirements.txt` vào image
- Cài đặt tất cả Python packages từ requirements.txt
- `--no-cache-dir`: Không lưu cache để giảm kích thước image

### 6. Copy Project Code

```bash
COPY . .
```

Copy toàn bộ project code vào image

### 7. Cài đặt Project như Package

```bash
RUN pip3 install -e .
```

Cài đặt project như một package editable (development mode) để có thể import modules

### 8. Tải JAR Files cho Spark

Tải các JAR files cần thiết vào `/opt/spark/jars/`:

```bash
JARS_DIR="/opt/spark/jars"
mkdir -p "$JARS_DIR"
cd "$JARS_DIR"
```

**Các JAR files được tải:**

1. **hadoop-aws-3.3.4.jar**
   - URL: `https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar`
   - Mục đích: Hỗ trợ S3/MinIO filesystem

2. **aws-java-sdk-bundle-1.12.644.jar**
   - URL: `https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.644/aws-java-sdk-bundle-1.12.644.jar`
   - Mục đích: AWS SDK để kết nối với S3/MinIO

3. **postgresql-42.7.3.jar**
   - URL: `https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar`
   - Mục đích: JDBC driver cho PostgreSQL

4. **iceberg-spark-runtime-3.4_2.12-1.5.0.jar**
   - URL: `https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.0/iceberg-spark-runtime-3.4_2.12-1.5.0.jar`
   - Mục đích: Iceberg runtime cho Spark 3.4

**Lưu ý:** Các JAR files này sẽ được Spark tự động load khi chạy, không cần thêm vào `--jars` trong spark-submit (nhưng vẫn có thể thêm để đảm bảo).

### 9. Copy và Setup Scripts

```bash
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

COPY spark-iceberg-etl.sh /app/spark-iceberg-etl.sh
RUN chmod +x /app/spark-iceberg-etl.sh
```

- Copy `entrypoint.sh`: Script wrapper cho spark-submit
- Copy `spark-iceberg-etl.sh`: Script giữ container chạy
- Set quyền thực thi cho cả 2 scripts

### 10. Setup MinIO Alias (Optional)

```bash
mc alias set myminio http://10.8.75.82:9000 minioadmin minioadmin
mc ls myminio/csdlgt/staging_etl_data_export/
```

**Lưu ý:** 
- Command này có thể fail khi build nếu MinIO chưa accessible
- Không ảnh hưởng đến việc build image thành công
- Có thể setup lại khi container chạy

### 11. Default Command

```bash
CMD ["/app/spark-iceberg-etl.sh"]
```

Container sẽ chạy script `spark-iceberg-etl.sh` để giữ container chạy và chờ được gọi.

## Build Docker Image

### Cách 1: Build từ thư mục project

```bash
docker build -t spark-etl:latest .
```

### Cách 2: Build với tag cụ thể

```bash
docker build -t spark-etl:v1.0.0 .
```

### Kiểm tra Image đã build

```bash
docker images | grep spark-etl
```

## Cấu hình Environment Variables

Tạo file `.env` từ `env_template.txt` và điền các giá trị phù hợp:

```bash
cp env_template.txt config/.env
# Chỉnh sửa config/.env với các giá trị thực tế
```

**Lưu ý:** File `.env` nằm trong thư mục `config/`. Code sẽ tự động load từ `config/.env`.

### Environment Variables quan trọng

- `SPARK_MASTER`: URL của Spark Master (mặc định: `spark://spark-master:7077`)
- `DEPLOY_MODE`: Deploy mode cho Spark (mặc định: `client`)
- `MINIO_ENDPOINT`: Endpoint của MinIO
- `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`: Credentials cho MinIO
- `MINIO_BUCKET`: Tên bucket trên MinIO
- `PG_HIVE_METASTORE_URI`: URI của Hive Metastore
- `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`: Thông tin PostgreSQL
- `OUTPUT_PATH`: Đường dẫn output trên MinIO
- Các biến khác xem trong `env_template.txt`

## Chạy Container

### 1. Tạo Docker Network (nếu chưa có)

Nếu Spark Master của bạn chưa có network, tạo network chung:

```bash
docker network create spark-network
```

Sau đó kết nối Spark Master container vào network này:

```bash
docker network connect spark-network <spark-master-container-name>
```

### 2. Chạy với Docker Compose

#### Chạy job ETL với job_type:

```bash
docker-compose run --rm etl-worker /app/entrypoint.sh /app/jobs/main.py <job_type>
```

Ví dụ:
```bash
# Chạy job Daily với ngày mặc định (hôm qua)
docker-compose run --rm etl-worker /app/entrypoint.sh /app/jobs/main.py 1

# Chạy job Daily với ngày cụ thể
docker-compose run --rm etl-worker /app/entrypoint.sh /app/jobs/main.py 1 2025-01-15
```

#### Chạy với input_date (nếu job hỗ trợ):

```bash
docker-compose run --rm etl-worker /app/entrypoint.sh /app/jobs/main.py <job_type> <input_date>
```

### 3. Chạy trực tiếp với Docker (không dùng docker-compose)

```bash
docker run --rm \
  --network spark-network \
  -e SPARK_MASTER=spark://spark-master:7077 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -e PG_HIVE_METASTORE_URI=thrift://hive-metastore:9083 \
  -v $(pwd)/output:/app/output \
  spark-etl:latest /app/entrypoint.sh /app/jobs/main.py <job_type>
```

### 4. Chạy container ở chế độ interactive

```bash
docker run -it --rm \
  --network spark-network \
  -v $(pwd)/output:/app/output \
  spark-etl:latest /bin/bash
```

Sau đó chạy job từ trong container:

```bash
/app/entrypoint.sh /app/jobs/main.py 1
```

## Volumes

- `./output:/app/output`: Mount thư mục output để lưu kết quả

## Troubleshooting

### Lỗi kết nối với Spark Master

1. Kiểm tra Spark Master đang chạy:
   ```bash
   docker ps | grep spark-master
   ```

2. Kiểm tra network:
   ```bash
   docker network inspect spark-network
   ```

3. Test kết nối từ ETL container:
   ```bash
   docker run --rm --network spark-network spark-etl:latest ping spark-master
   ```

### Lỗi thiếu JAR files

Các JAR files sẽ được tự động tải khi build image. Nếu gặp lỗi:

1. Kiểm tra JAR files trong image:
   ```bash
   docker run --rm spark-etl:latest ls -lh /opt/spark/jars/ | grep -E "(hadoop-aws|aws-java-sdk|postgresql|iceberg)"
   ```

2. Nếu thiếu, có thể thêm vào `--jars` trong entrypoint.sh hoặc tải lại khi build

### Lỗi kết nối MinIO/PostgreSQL

Đảm bảo các service này có thể truy cập được từ ETL container trong cùng network:

```bash
# Test kết nối MinIO
docker run --rm --network spark-network spark-etl:latest mc alias set myminio http://minio:9000 minioadmin minioadmin && mc ls myminio/

# Test kết nối PostgreSQL
docker run --rm --network spark-network spark-etl:latest psql -h postgres -U postgres -d postgres -c "SELECT 1"
```

### Lỗi khi build image

1. **Lỗi download JAR files**: Kiểm tra kết nối internet hoặc thử build lại
2. **Lỗi cài đặt Python packages**: Kiểm tra `requirements.txt` có đúng format không
3. **Lỗi MinIO setup**: Có thể bỏ qua, sẽ setup lại khi container chạy

## Lưu ý

- Image này được thiết kế để chạy như một Spark client, kết nối với Spark Master riêng
- Deploy mode mặc định là `client` - driver sẽ chạy trong container này
- Các Spark packages được tải vào `/opt/spark/jars/` để tăng tốc độ khởi động
- Container sẽ chạy script `spark-iceberg-etl.sh` để giữ container chạy và chờ được gọi
- Khi cần chạy job, sử dụng: `docker exec <container_name> /app/entrypoint.sh /app/jobs/main.py <job_type>`

