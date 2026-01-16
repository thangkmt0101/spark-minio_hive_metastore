# Test Spark Jobs

Folder này chứa các file test để submit vào Spark master.

## Spark Master

```
spark://10.8.75.82:7077
```

## Files

- `hello_spark.py`: File Spark job đơn giản để print "hello" và lấy log từ Spark master
- `submit_hello.sh`: Script để submit hello_spark.py vào Spark master với cấu hình giới hạn executor
- `get_spark_logs.py`: Script độc lập để lấy log từ Spark master (không cần chạy Spark job)

## ⚠️ QUAN TRỌNG: Giới hạn Executor để tránh log liên tục

Để tránh vấn đề Spark liên tục tạo executor → executor EXITED → tạo executor mới (gây log liên tục và đầy disk), **BẮT BUỘC** phải:

1. **Giới hạn executor**: `--num-executors 1 --executor-cores 1 --executor-memory 512m`
2. **Tắt dynamic allocation**: `--conf spark.dynamicAllocation.enabled=false`
3. **Giới hạn retry**: `--conf spark.executor.maxFailures=1 --conf spark.task.maxFailures=1 --conf spark.deploy.maxExecutorFailures=1`

## Cách sử dụng

### Cách 1: Sử dụng script submit (KHUYẾN NGHỊ - đã có đầy đủ config)

```bash
cd test
./submit_hello.sh
```

### Cách 2: Sử dụng spark-submit trực tiếp (với đầy đủ config)

```bash
spark-submit \
    --master spark://10.8.75.82:7077 \
    --deploy-mode client \
    --name "Hello Spark Job" \
    --num-executors 1 \
    --executor-cores 1 \
    --executor-memory 512m \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.maxFailures=1 \
    --conf spark.task.maxFailures=1 \
    --conf spark.deploy.maxExecutorFailures=1 \
    test/hello_spark.py
```

### Cách 3: Sử dụng entrypoint.sh (trong Docker container)

```bash
# Lưu ý: entrypoint.sh đã có một số config, nhưng nên thêm các config trên
./entrypoint.sh test/hello_spark.py
```

### Cách 4: Sử dụng biến môi trường

```bash
export SPARK_MASTER=spark://10.8.75.82:7077
./entrypoint.sh test/hello_spark.py
```

## Lấy log từ Spark Master

### Tự động trong hello_spark.py

File `hello_spark.py` sẽ tự động lấy log từ Spark master khi chạy, bao gồm:
- Thông tin application (ID, name, state, start time)
- Danh sách executors và trạng thái
- Thông tin từ SparkContext

### Sử dụng script độc lập get_spark_logs.py

Để lấy log chi tiết từ Spark master mà không cần chạy Spark job:

```bash
# Lấy log với app_id
python test/get_spark_logs.py <app_id> [master_host] [ui_port]

# Ví dụ:
python test/get_spark_logs.py app-20260107115021-0049 10.8.75.82 8080
```

Script này sẽ hiển thị:
- Thông tin application
- Chi tiết tất cả executors
- Danh sách jobs
- Danh sách stages

## Lưu ý

- **QUAN TRỌNG**: Luôn giới hạn executor khi submit job test để tránh log liên tục và đầy disk
- Đảm bảo Spark master đang chạy và có thể kết nối được
- Spark UI phải chạy trên port 8080 (mặc định) để lấy log qua REST API
- Nếu chạy trong Docker container, đảm bảo network đã được cấu hình đúng
- File `hello_spark.py` sẽ tự động tạo SparkSession và in "HELLO FROM SPARK!"
- Với job test/hello/ETL tuần tự, 1 executor là đủ

