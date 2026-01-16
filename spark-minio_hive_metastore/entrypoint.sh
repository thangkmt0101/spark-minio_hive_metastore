#!/bin/bash
set -e

# Nếu có biến môi trường SPARK_MASTER, sử dụng nó
# Nếu không, mặc định là spark://spark-master:7077

#SPARK_MASTER=${SPARK_MASTER:-"spark://10.8.75.82:7077"}
SPARK_MASTER=${SPARK_MASTER:-"spark://spark-master:7077"}

# Nếu có biến môi trường DEPLOY_MODE, sử dụng nó
# Mặc định là client mode
DEPLOY_MODE=${DEPLOY_MODE:-"client"}


# Tìm đường dẫn spark-submit
if [ -f "/opt/spark/bin/spark-submit" ]; then
    SPARK_SUBMIT="/opt/spark/bin/spark-submit"
else
    echo "Error: spark-submit not found!"
    exit 1
fi

# Kiểm tra và tạo danh sách JAR files để thêm vào --jars
JARS_DIR="/opt/spark/jars"
JAR_LIST=""

if [ -d "$JARS_DIR" ]; then
    JAR_COUNT=$(ls -1 "$JARS_DIR"/*.jar 2>/dev/null | wc -l)
    echo "Số lượng JAR files có sẵn trong $JARS_DIR: $JAR_COUNT"
    
    if [ "$JAR_COUNT" -gt 0 ]; then
        echo "Các JAR files chính sẽ được thêm vào --jars:"
        ls -1 "$JARS_DIR"/*.jar 2>/dev/null | grep -E "(hadoop-aws|aws-java-sdk|postgresql|mssql-jdbc|iceberg|hadoop-common|aws-java-sdk-bundle)" | head -7
        
        # Tạo danh sách JAR files cách nhau bởi dấu phẩy
        # Chỉ lấy các JAR cần thiết để tránh quá nhiều
        JAR_LIST=$(ls -1 "$JARS_DIR"/*.jar 2>/dev/null | \
            grep -E "(hadoop-aws|aws-java-sdk|postgresql|mssql-jdbc|iceberg|hadoop-common|aws-java-sdk-bundle)" | \
            tr '\n' ',' | sed 's/,$//')
    fi
fi

# Xây dựng command spark-submit
# LƯU Ý: Các config Spark đã được cấu hình trong spark_session.py
# Config từ SparkSession.builder sẽ override config từ spark-submit
# Chỉ giữ lại các config chỉ có thể set ở spark-submit level
SPARK_ARGS=(
  --master "$SPARK_MASTER"
  --deploy-mode "$DEPLOY_MODE"
  --name "${APP_NAME:-Spark ETL Job}"
  --num-executors "${NUM_EXECUTORS:-2}"
  --executor-cores "${EXECUTOR_CORES:-2}"  # Tăng từ 1 lên 2 để tận dụng parallelism (tối ưu cho 5-10GB)
  --executor-memory "${EXECUTOR_MEMORY:-1g}"  # Tăng từ 2g lên 4g để tránh spill to disk (tối ưu cho 5-10GB)
)

# Thêm --jars nếu có JAR files (QUAN TRỌNG: Spark sẽ copy JAR từ driver đến executor)
if [ -n "$JAR_LIST" ]; then
    echo "Thêm --jars để Spark copy JAR files từ driver đến executor:"
    echo "  $JAR_LIST"
    SPARK_ARGS+=(--jars "$JAR_LIST")
fi

# Thêm --packages nếu được chỉ định (fallback)
if [ -n "$SPARK_PACKAGES" ]; then
    echo "Sử dụng --packages: $SPARK_PACKAGES"
    SPARK_ARGS+=(--packages "$SPARK_PACKAGES")
fi

# Thêm các tham số từ command line
SPARK_ARGS+=("$@")

# Chạy spark-submit
echo "Chạy spark-submit với các tham số:"
echo "  ${SPARK_ARGS[@]}"
exec "$SPARK_SUBMIT" "${SPARK_ARGS[@]}"