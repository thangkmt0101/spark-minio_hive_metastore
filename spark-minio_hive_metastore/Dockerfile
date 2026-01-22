# Sử dụng base image có Spark và Python
FROM apache/spark:3.5.0

# Chuyển sang user root để cài đặt packages
USER root

# Cài đặt các dependencies cần thiết
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    gcc \
    g++ \
    curl \
    gnupg \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt MinIO Client (mc)
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Tạo thư mục cho project
WORKDIR /app

# Copy requirements và cài đặt Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy toàn bộ project code
COPY . .

# Cài đặt project như một package
RUN pip3 install -e .

# Tải các JAR files cần thiết vào thư mục jars của Spark
# Apache Spark thường ở /opt/spark/jars/
RUN JARS_DIR="/opt/spark/jars" && \
    mkdir -p "$JARS_DIR" && \
    cd "$JARS_DIR" && \
    echo "Đang tải các JAR files..." && \
    curl -L -o hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    curl -L -o aws-java-sdk-bundle-1.12.644.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.644/aws-java-sdk-bundle-1.12.644.jar" && \
    curl -L -o postgresql-42.7.3.jar "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && \
    curl -L -o iceberg-spark-runtime-3.4_2.12-1.5.0.jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.0/iceberg-spark-runtime-3.4_2.12-1.5.0.jar" && \
    echo "Đã tải xong các JAR files:" && \
    ls -lh *.jar | grep -E "(hadoop-aws|aws-java-sdk|postgresql|iceberg)" && \
    echo "Tổng số JAR files trong thư mục: $(ls -1 *.jar | wc -l)"

# Copy script giữ container chạy (tên theo image)
COPY spark-iceberg-etl.sh /app/spark-iceberg-etl.sh
RUN chmod +x /app/spark-iceberg-etl.sh


# Setup MinIO alias và kiểm tra đường dẫn (chạy khi build, có thể fail nếu MinIO chưa sẵn sàng)
# Lưu ý: Command này có thể fail khi build nếu MinIO chưa accessible, nhưng không ảnh hưởng đến image
RUN bash -c "mc alias set myminio http://10.8.75.82:9000 minioadmin minioadmin && mc ls myminio/csdlgt/staging_etl_data_export/" || echo "Warning: MinIO setup skipped (MinIO may not be available during build)"

# Giữ container chạy và chờ được gọi
# Khi cần chạy job, sử dụng: docker exec <container_name> /app/entrypoint.sh /app/jobs/main.py <job_type>
CMD ["/app/spark-iceberg-etl.sh"]

