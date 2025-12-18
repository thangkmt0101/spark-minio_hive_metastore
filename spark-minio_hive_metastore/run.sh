#!/usr/bin/env bash
set -euo pipefail

# Gói phụ thuộc cho spark-submit
PKG="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.644,org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0"

# Tham số truyền cho main.py:
#   ./run.sh 1                  # ví dụ job_type = 1
#   ./run.sh 1 2025-01-01       # nếu main.py nhận thêm input_date
spark-submit --packages "$PKG" ./main.py "$@"

