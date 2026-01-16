#!/bin/bash
# Script để submit hello_spark.py vào Spark master

# Spark master URL
SPARK_MASTER=${SPARK_MASTER:-"spark://10.8.75.82:7077"}

# Tìm đường dẫn spark-submit
if [ -f "/opt/spark/bin/spark-submit" ]; then
    SPARK_SUBMIT="/opt/spark/bin/spark-submit"
elif command -v spark-submit &> /dev/null; then
    SPARK_SUBMIT="spark-submit"
else
    echo "Error: spark-submit not found!"
    exit 1
fi

# Đường dẫn đến file Python
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_FILE="$SCRIPT_DIR/hello_spark.py"

# Kiểm tra file có tồn tại không
if [ ! -f "$PYTHON_FILE" ]; then
    echo "Error: File $PYTHON_FILE not found!"
    exit 1
fi

# Submit job
echo "Submitting hello_spark.py to Spark master: $SPARK_MASTER"
echo "=========================================="

$SPARK_SUBMIT \
    --master "$SPARK_MASTER" \
    --deploy-mode client \
    --name "Hello Spark Job" \
    --num-executors 1 \
    --executor-cores 1 \
    --executor-memory 512m \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --conf spark.executor.maxFailures=1 \
    --conf spark.task.maxFailures=1 \
    --conf spark.deploy.maxExecutorFailures=1 \
    --conf spark.executor.instances=1 \
    "$PYTHON_FILE"

echo "=========================================="
echo "Job completed!"

