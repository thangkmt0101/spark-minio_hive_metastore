Build docker img

docker build -t etl-job-config:v1 .

docker rmi k135520214055/etl-job-config:v1
docker tag etl-job-config:v1 k135520214055/etl-job-config:v1
docker save -o "D:\DBA\backup\script\spark-minio_hive_metastore\etl-job-config_v1.tar" k135520214055/etl-job-config:v1


docker save -o "D:\DBA\backup\script\spark-minio_hive_metastore\etl-job-config.tar" k135520214055/etl-job-config:v1
docker tag etl-job-config:latest k135520214055/etl-job-config:latest
docker tag etl-job-config:latest k135520214055/etl-job-config:v1

- Load image từ file tar
docker load -i etl-job-config.tar

-- chạy docker compose file linux
docker compose -f docker-compose-etl-mart-prod.yml up -d
docker compose -f docker-compose-etl-mart-prod.yml stop spark-iceberg-etl
docker compose -f docker-compose-etl-mart-prod.yml stop etl-job-config

docker cp /u01/Services/etl/spark-minio_hive_metastore/web/.env etl-web:/app/.env
 
