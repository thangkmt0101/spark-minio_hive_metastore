import os
import sys
import shutil
import time
import subprocess
from tempfile import TemporaryDirectory
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from config.spark_session import SparkSessionBuilder
from config.time_config import TimeService
from config.utils import EtlUtils
from config.job_etl_logger import JobLogger
from config.settings import (
    PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD,
    OUTPUT_PATH, SPARK_JDBC_BATCH_SIZE, SPARK_JDBC_NUM_PARTITIONS,
    SPARK_JDBC_MAX_POOL_SIZE, MINIO_ENDPOINT, MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY, MINIO_BUCKET, ETL_DATA_EXPORT_PATH
)
from jinja2 import Template
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from pyspark import StorageLevel

    
class Etl_Runner:

    def __init__(self):
        """Khởi tạo và tạo connection info để tái sử dụng"""
        # Khởi tạo mặc định để tránh AttributeError trong finally khi lỗi sớm
        self.conn = None
        self.cursor = None
        self.spark = None

        # Tạo connection info từ cấu hình PostgreSQL (dạng dict để dùng với psycopg2)
        self.conn_str = {
            'host': PG_HOST,
            'port': PG_PORT,
            'database': PG_DATABASE,
            'user': PG_USER,
            'password': PG_PASSWORD
        }
    
        # Lazy initialization: Spark session sẽ được tạo khi cần (trong run method)
        self.spark = None
        # Lưu danh sách các DataFrame dim đã cache để giải phóng sau
        self.cached_dim_dataframes = {}

    def _cache_dim_tables(self):
        """
        Cache các dim tables từ folder sql/dim để tối ưu hiệu suất join.
        Đọc danh sách dim tables từ file dim_tables_config.txt trong folder sql/dim.
        Sử dụng StorageLevel.MEMORY_AND_DISK và tạo temp view để dùng trong SQL queries.
        """
        try:
            print(f"[CACHE DIM] Bắt đầu cache các dim tables từ folder sql/dim...")
            
            # Đọc file config trong folder sql/dim
            dim_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sql", "dim")
            config_file = os.path.join(dim_folder, "dim_tables_config.txt")
            dim_tables = {}
            
            if os.path.exists(config_file):
                try:
                    with open(config_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            # Bỏ qua comment và dòng trống
                            if not line or line.startswith('#'):
                                continue
                            
                            # Format: iceberg_table_path|temp_view_name
                            if '|' in line:
                                parts = line.split('|')
                                if len(parts) == 2:
                                    iceberg_table = parts[0].strip()
                                    temp_view_name = parts[1].strip()
                                    dim_tables[temp_view_name] = (iceberg_table, temp_view_name)
                                    print(f"[CACHE DIM] Đã đọc dim table từ config: {temp_view_name} -> {iceberg_table}")
                except Exception as config_error:
                    print(f"[CACHE DIM WARNING] Không thể đọc file config {config_file}: {config_error}")
            else:
                print(f"[CACHE DIM WARNING] File config không tồn tại: {config_file}")
            
            if not dim_tables:
                print(f"[CACHE DIM WARNING] Không tìm thấy dim tables nào trong config file")
                return
            
            for dim_name, (iceberg_table, temp_view) in dim_tables.items():
                try:
                    print(f"[CACHE DIM] Đang cache {dim_name} từ {iceberg_table}...")
                    
                    # Load dim table từ Iceberg
                    df_dim = self.spark.read.table(iceberg_table)
                    
                    # Cache với MEMORY_AND_DISK
                    df_dim.persist(StorageLevel.MEMORY_AND_DISK)
                    
                    # Lưu DataFrame vào danh sách để giải phóng sau
                    self.cached_dim_dataframes[dim_name] = df_dim
                    
                    # Materialize cache bằng count()
                    row_count = df_dim.count()
                    print(f"[CACHE DIM] Đã cache {dim_name}: {row_count} rows")
                    
                    # Tạo temp view để dùng trong SQL queries
                    df_dim.createOrReplaceTempView(temp_view)
                    print(f"[CACHE DIM] Đã tạo temp view: {temp_view}")
                    
                except Exception as dim_error:
                    print(f"[CACHE DIM WARNING] Không thể cache {dim_name}: {dim_error}")
                    # Tiếp tục với các dim khác nếu một dim lỗi
                    continue
            
            print(f"[CACHE DIM] Hoàn thành cache dim tables!")
            
        except Exception as e:
            print(f"[CACHE DIM ERROR] Lỗi khi cache dim tables: {str(e)}")
            # Không throw exception để job vẫn có thể tiếp tục chạy
            traceback.print_exc()

    def _uncache_dim_tables(self):
        """
        Giải phóng cache các dim tables đã cache trước đó.
        Unpersist các DataFrame và drop temp views.
        """
        try:
            print(f"[UNCACHE DIM] Bắt đầu giải phóng cache dim tables...")
            
            if not hasattr(self, 'cached_dim_dataframes') or not self.cached_dim_dataframes:
                print(f"[UNCACHE DIM] Không có dim tables nào cần giải phóng")
                return
            
            # Unpersist tất cả các DataFrame dim đã cache
            for dim_name, df_dim in self.cached_dim_dataframes.items():
                try:
                    print(f"[UNCACHE DIM] Đang giải phóng cache cho {dim_name}...")
                    df_dim.unpersist(blocking=False)  # Non-blocking để tránh treo
                    print(f"[UNCACHE DIM] Đã giải phóng cache cho {dim_name}")
                except Exception as unpersist_error:
                    print(f"[UNCACHE DIM WARNING] Không thể unpersist {dim_name}: {unpersist_error}")
                    continue
            
            # Clear danh sách cached DataFrames
            self.cached_dim_dataframes.clear()
            
            # Drop temp views nếu Spark session còn tồn tại
            if self.spark:
                try:
                    # Lấy danh sách temp views từ catalog
                    temp_views = self.spark.catalog.listTables("default")
                    for table in temp_views:
                        if table.name.startswith("dim_") or table.name.startswith("view_dim_"):
                            try:
                                self.spark.catalog.dropTempView(table.name)
                                print(f"[UNCACHE DIM] Đã drop temp view: {table.name}")
                            except Exception as drop_error:
                                print(f"[UNCACHE DIM WARNING] Không thể drop temp view {table.name}: {drop_error}")
                except Exception as view_error:
                    print(f"[UNCACHE DIM WARNING] Không thể list/drop temp views: {view_error}")
            
            print(f"[UNCACHE DIM] Hoàn thành giải phóng cache dim tables!")
            
        except Exception as e:
            print(f"[UNCACHE DIM ERROR] Lỗi khi giải phóng cache dim tables: {str(e)}")
            # Không throw exception để cleanup vẫn tiếp tục
            traceback.print_exc()

    def run(self, job_type=None, input_date=None):
        """
        Chạy job chính
        
        Args:
            job_type: Loại job để filter (optional). Nếu None thì lấy tất cả job active
            input_date: Ngay 'yyyy-mm-dd'; None -> mac dinh hom qua
        """

        self.spark = SparkSessionBuilder.get_spark("Daily Revenue Job")

        info = TimeService.get_target_date(input_date)
        y_year = info["year"]
        y_quarter = info["quarter"]
        y_month = info["month"]
        y_day = info["day"]

        # Xác định mức thời gian dựa trên job_type
        # job_type = 1 -> Daily, 2 -> Week, 3 -> Month, 4 -> Quarter, 5 -> Year
        job_type_int = int(job_type) if isinstance(job_type, (int, str)) and str(job_type).isdigit() else None
        
        if job_type_int == 1:
            # Daily: lấy thông tin UTC range từ ngày VN (UTC+7)
            time_level = 'day'
            utc_info = TimeService.get_utc_range_from_vn_date(info["date_str"])
        elif job_type_int == 2:
            # Week: lấy thông tin UTC range cho cả tuần (thứ 2 đến chủ nhật)
            time_level = 'week'
            utc_info = TimeService.get_utc_range_from_vn_week(info["date_str"])
        elif job_type_int == 3:
            # Month: lấy thông tin UTC range cho cả tháng (UTC+7)
            time_level = 'month'
            utc_info = TimeService.get_utc_range_from_vn_month(info["date_str"])
        elif job_type_int == 4:
            # Quarter: lấy thông tin UTC range cho cả quý (UTC+7)
            time_level = 'quarter'
            utc_info = TimeService.get_utc_range_from_vn_quarter(info["date_str"])
        elif job_type_int == 5:
            # Year: lấy thông tin UTC range cho cả năm (UTC+7)
            time_level = 'year'
            utc_info = TimeService.get_utc_range_from_vn_year(info["date_str"])
        else:
            # Mặc định: Daily nếu không xác định được
            print(f"[WARNING] job_type='{job_type}' khong hop le")
        
        print(f"[INFO] Mức thời gian: {time_level} (job_type={job_type})")
        
        utc_year = utc_info["day_ranges"]
        utc_quarter = utc_info["day_ranges"]
        utc_month = utc_info["day_ranges"]
        utc_week = utc_info["day_ranges"]
        utc_day_ranges = utc_info["day_ranges"]

        try: 
            # 0. Kết nối database PostgreSQL và đọc bảng etl_job để lấy table_name và sql_path
            # Sử dụng conn_str đã tạo trong __init__
            # Thêm timeout=30 để tránh treo khi kết nối
            self.conn = psycopg2.connect(
                host=self.conn_str['host'],
                port=self.conn_str['port'],
                database=self.conn_str['database'],
                user=self.conn_str['user'],
                password=self.conn_str['password'],
                connect_timeout=30
            )
            self.cursor = self.conn.cursor()
            
            # Đọc bảng etl_job để lấy danh sách table_name, sql_path và delete_column
            # Thêm điều kiện WHERE job_type nếu được truyền vào
            if job_type:
                etl_job_query = """
                    SELECT table_name, sql_path, delete_column, delete_condition
                    FROM view_etl_job 
                    WHERE is_active = true AND job_type = %s
                    ORDER BY id
                """
                self.cursor.execute(etl_job_query, (job_type,))
            else:
                # Nếu không có job_type, không chạy (theo yêu cầu chỉ chạy khi có job_type)
                raise ValueError("job_type la bat buoc. Vui long truyen job_type khi goi ham run()")
            
            etl_job_results = self.cursor.fetchall()
            
            if not etl_job_results:
                # Thêm thông tin chi tiết về job_type để dễ debug
                error_detail = f"Khong tim thay job active voi job_type='{job_type}' trong bang etl_job. "
                error_detail += f"Vui long kiem tra view view_etl_job co job_type='{job_type}' va is_active=true"
                print(f"[WARNING] {error_detail}")
                raise ValueError(error_detail)
            
            print(f"Tim thay {len(etl_job_results)} job(s) active. Bat dau xu ly...")
            
            # Cache các dim tables trước khi xử lý jobs
            self._cache_dim_tables()
             
            # Duyệt qua từng job và xử lý
            for job_idx, etl_job_result in enumerate(etl_job_results, 1):
                # Bắt đầu tính thời gian execution cho job con này
                single_job_start_time = time.time()
                
                try:
                    print(f"\n[{job_idx}/{len(etl_job_results)}] Dang xu ly job... {etl_job_result}")
                    
                    # Lấy các giá trị từ etl_job_result
                    table_name = etl_job_result[0]
                    sql_path = etl_job_result[1]
                    delete_column = etl_job_result[2]
                    delete_condition = etl_job_result[3]  # Lấy delete_condition từ bảng etl_job
                    
                    # Đọc file SQL từ sql_path
                    if not os.path.exists(sql_path):
                        error_msg = f"Khong tim thay file SQL: {sql_path}"
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> File Not Found for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="READ_SQL_FILE",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log file not found error: {log_err}")
                        raise FileNotFoundError(error_msg)
                    
                    try:
                        with open(sql_path, 'r', encoding='utf-8') as f:
                            sql_template = f.read()
                    except Exception as read_err:
                        error_msg = f"Loi khi doc file SQL {sql_path}: {str(read_err)}"
                        print(f"[ERROR] {error_msg}")
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> Read SQL File Error for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="READ_SQL_FILE",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log read SQL file error: {log_err}")
                        raise Exception(error_msg) from read_err
                    
                    # Render template với các biến UTC year, month, day và day_ranges
                    try:
                        template = Template(sql_template)
                        sql_query = template.render(
                            year_utc_7=str(y_year),
                            quarter_utc_7=str(y_quarter),
                            month_utc_7=str(y_month).zfill(2),
                            week_utc_7=str(utc_week),
                            day_utc_7=str(y_day).zfill(2),

                            year=utc_year,
                            quarter=utc_quarter,
                            month=utc_month, #kết quả định dạng: "01"
                            week=utc_week,
                            day_ranges=utc_day_ranges # Truyền day_ranges để template có thể loop qua
                        )

                    except Exception as render_err:
                        error_msg = f"Loi khi render template SQL cho bang {table_name}: {str(render_err)}"
                        print(f"[ERROR] {error_msg}")
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> Render Template Error for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="RENDER_TEMPLATE",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log render template error: {log_err}")
                        raise Exception(error_msg) from render_err      
                    #Chạy SQL query bằng spark.sql()
                    try:
                        df = self.spark.sql(sql_query)
                    except Exception as sql_err:
                        error_msg = f"Loi khi chay SQL query cho bang {table_name}: {str(sql_err)}"
                        print(f"[ERROR] {error_msg}")
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> Execute SQL Error for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="EXECUTE_SQL",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log execute SQL error: {log_err}")
                        raise Exception(error_msg) from sql_err

                    try:
                        # Chỉ scan 1 record, không full scan để kiểm tra có dữ liệu không
                        # Nếu executor crash, count() sẽ throw SparkException hoặc Py4JJavaError
                        has_data = df.limit(1).count() > 0
                    except Exception as check_error:
                        error_msg = f"SQL/Iceberg/S3/Executor loi khi kiem tra du lieu cho bang {table_name}: {str(check_error)}"
                        print(f"[ERROR] {error_msg}")
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> Check Data Error for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="CHECK_DATA",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log check data error: {log_err}")
                        raise Exception(error_msg) from check_error
                    
                    if not has_data:
                        print(f"[SKIP INSERT] {table_name} không có dữ liệu")
                        continue

                    csv_temp_dir = None
                    try:
                        # Tạo thư mục temp để lưu CSV
                        csv_temp_dir = os.path.join(OUTPUT_PATH, f"{table_name}")
                        os.makedirs(csv_temp_dir, exist_ok=True)

                        # Export DataFrame sang CSV
                        csv_file_path = EtlUtils.export_dataframe_to_csv(
                            df=df,
                            output_dir=csv_temp_dir,
                            delimiter=',',
                            datetime_csv=(y_year+y_month+y_day)
                        )
                        
                        # Export DataFrame sang Parquet (Iceberg table) - chỉ export nếu có đủ cột partition
                        try:
                            # Kiểm tra DataFrame có các cột partition không
                            df_columns = [col_name.lower() for col_name in df.columns]
                            print(df_columns)
                            required_partition_cols = ["year", "month", "day"]
                            has_partition_cols = all(col in df_columns for col in required_partition_cols)
                            
                            if has_partition_cols:
                                # Tạo table name: mart.{table_name}
                                mart_table_name = f"mart.{table_name}"
                                print(f"[EXPORT PARQUET] Bắt đầu export sang Iceberg table: {mart_table_name}")
                                EtlUtils.export_dataframe_to_parquet(
                                    df=df,
                                    table_name=mart_table_name
                                )
                                print(f"[EXPORT PARQUET] Export thành công vào {mart_table_name}")
                            else:
                                print(f"[SKIP PARQUET] Bỏ qua export Parquet vì thiếu cột partition (cần: year, month, day)")
                        except Exception as parquet_error:
                            # Không để lỗi Parquet ảnh hưởng đến CSV export
                            error_msg = f"Lỗi khi export Parquet cho bảng {table_name}: {str(parquet_error)}"
                            print(f"[WARNING] {error_msg}")
                            # Không log vào DB để tránh spam
                            
                    except Exception as bulk_error:
                        error_msg = f"Loi khi export CSV cho bang {table_name}: {str(bulk_error)}"
                        print(f"[ERROR] {error_msg}")
                        try:
                            JobLogger.log_error(
                                job_name=f"Etl_Runner --> Export CSV Error for {table_name}",
                                job_type=job_type,
                                message=error_msg,
                                conn_str=self.conn_str,
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="EXPORT_CSV",
                                year=y_year,
                                month=y_month,
                                day=y_day
                            )
                        except Exception as log_err:
                            print(f"[WARNING] Khong the ghi log export CSV error: {log_err}")
                        raise Exception(error_msg) from bulk_error
                    
                    # Cleanup: Unpersist DataFrame và clear cache để giải phóng memory (cho cả trường hợp rỗng)
                    try:
                        df.unpersist(blocking=False)  # Non-blocking để tránh treo
                    except Exception as unpersist_err:
                        # Chỉ in warning, không log vào DB (không quan trọng)
                        print(f"[WARNING] Loi khi unpersist DataFrame cho bang {table_name}: {str(unpersist_err)}")
                    
                    # TỐI ƯU: Cleanup broadcast variables và Spark catalog cache để giải phóng memory
                    try:
                        self.spark.catalog.clearCache()  # Clear cache và broadcast variables
                    except Exception as cache_error:
                        # Chỉ in warning, không log vào DB (không quan trọng)
                        print(f"[WARNING] Khong the clear cache cho bang {table_name}: {str(cache_error)}")
                    
                    # Tính toán thời gian execution cho job con này (từ đầu đến khi insert xong)
                    single_job_elapsed_time = time.time() - single_job_start_time
                    execution_time_ms = round(single_job_elapsed_time / 60, 2)  # Chuyển sang phút (giữ 2 chữ số thập phân)
                    
                    print(f"[INFO] Thoi gian thuc thi job con: {execution_time_ms} phut ({single_job_elapsed_time:.2f}s)")
                    
                    # Ghi log thong tin so ban ghi da insert với thời gian execution (tạo connection riêng và đóng sau khi insert log)
                    # Thêm try-except để tránh treo job nếu log fail
                    try:
                        JobLogger.log_info(
                            job_name="Etl_Runner --> Success insert",
                            info_message=f"Insert thanh vao {table_name}",
                            conn_str=self.conn_str,  # Hàm sẽ tạo connection riêng
                            table_name=table_name,
                            sql_path=sql_path,
                            operation_type="INSERT",
                            year=y_year,
                            month=y_month,
                            day=y_day,
                            execution_time_ms=execution_time_ms,  # Thời gian execution khi insert thành công
                            job_type=job_type
                        )
                    except Exception as log_err:
                        # Nếu log fail, không để job bị treo, chỉ in warning
                        print(f"[WARNING] Khong the ghi log execution_time_ms vao database: {log_err}")
                    
                    # Gộp và đổi tên file trên MinIO trước khi sync
                    try:
                        print(f"\n[MERGE] Đang gộp và đổi tên file trên MinIO cho table: {table_name}")
                        EtlUtils.merge_and_rename_files_in_minio(
                            source_path=ETL_DATA_EXPORT_PATH,
                            table_name=table_name,
                            input_date=input_date
                        )
                    except Exception as merge_err:
                        # Chỉ in warning, không log vào DB (không ảnh hưởng đến dữ liệu đã insert)
                        print(f"[WARNING] Loi khi gop va doi ten file cho table {table_name}: {str(merge_err)}")
                    
                    # Sync từ MinIO sau khi insert thành công cho table này
                    try:
                        print(f"\n[SYNC] Đang sync dữ liệu từ MinIO cho table: {table_name}")
                        EtlUtils.sync_from_minio(
                            source_path=ETL_DATA_EXPORT_PATH, 
                            dest_path="/data/etl_data_export", 
                            input_date=input_date,
                            table_name=table_name
                        )
                    except Exception as sync_err:
                        # Chỉ in warning, không log vào DB (không ảnh hưởng đến dữ liệu đã insert)
                        print(f"[WARNING] Loi khi sync tu MinIO cho table {table_name}: {str(sync_err)}")
                    
                    # Xóa dữ liệu cũ từ MinIO (3 ngày trước)
                    try:
                        print(f"\n[DELETE] Đang xóa dữ liệu cũ từ MinIO cho table: {table_name}")
                        EtlUtils.delete_old_data_from_minio(
                            source_path=ETL_DATA_EXPORT_PATH,
                            table_name=table_name,
                            input_date=input_date,
                            days_before=3
                        )
                    except Exception as delete_err:
                        # Chỉ in warning, không log vào DB (không quan trọng)
                        print(f"[WARNING] Loi khi xoa du lieu cu cho table {table_name}: {str(delete_err)}")
 
                except Exception as e:
                    error_msg = f"Loi khi xu ly job cho bang {table_name}: {str(e)}"
                    
                    # Ghi log lỗi vào database, bổ sung số bản ghi đã đếm (nếu có) (tạo connection riêng và đóng sau khi insert log)
                    try:
                        JobLogger.log_error(
                            job_name=f"Etl_Runner --> Error for job {job_idx}/{len(etl_job_results)}",
                            job_type=job_type,
                            message=error_msg,
                            conn_str=self.conn_str,  # Hàm sẽ tạo connection riêng
                            error_traceback=traceback.format_exc(),
                            error_level="ERROR",
                            table_name=table_name,
                            sql_path=sql_path,
                            operation_type="JOB_EXECUTION",
                            year=y_year,
                            month=y_month,
                            day=y_day
                        )
                    except Exception as log_err:
                        print(f"[WARNING] Khong the ghi log job execution error: {log_err}")
                    
                    # QUAN TRỌNG: Kiểm tra xem có phải lỗi executor crash không
                    # Nếu là lỗi executor crash hoặc lỗi nghiêm trọng → DỪNG job ngay
                    error_str = str(e).lower()
                    is_executor_crash = any(keyword in error_str for keyword in [
                        "executor", "command exited", "sparkexception", 
                        "py4jjavaerror", "executor crash", "task failed"
                    ])
                    
                    if is_executor_crash:
                        # Lỗi executor crash → DỪNG job ngay, không tiếp tục
                        print(f"[FATAL ERROR] Executor crash detected! Dừng job ngay lập tức.")
                        print(f"[FATAL ERROR] Lỗi: {error_msg}")
                        raise Exception(f"[FATAL] Executor crash khi xử lý bảng {table_name}. Job đã dừng.") from e
                    else:
                        # Lỗi khác (SQL, Iceberg, S3) → có thể tiếp tục hoặc dừng tùy vào yêu cầu
                        # Hiện tại: tiếp tục chạy các bảng khác, chỉ log lỗi
                        print(f"[ERROR] Lỗi khi xử lý bảng {table_name}, tiếp tục với các bảng khác...")
                        # Không raise exception để job tiếp tục chạy các bảng khác
                    
        except Exception as e:
            error_msg = f"Loi nghiem trong trong job Etl_Runner: {str(e)}"
            
            # Kiểm tra xem có phải lỗi không tìm thấy job không (không cần log ERROR)
            is_job_not_found = "Khong tim thay job active" in str(e)
            
            if is_job_not_found:
                # Không tìm thấy job là trường hợp bình thường, chỉ log WARNING
                print(f"[WARNING] {error_msg}")
                # Vẫn log vào database nhưng với level WARNING
                if self.conn:
                    try:
                        JobLogger.log_error(
                            job_name="Etl_Runner --> No active job found",
                            job_type=job_type,
                            message=error_msg,
                            conn_str=self.conn_str,
                            error_traceback=traceback.format_exc(),
                            error_level="WARNING",  # WARNING thay vì ERROR
                            operation_type="JOB_INITIALIZATION"
                        )
                    except:
                        pass  # Nếu không log được thì bỏ qua
            else:
                # Các lỗi khác vẫn log ERROR như bình thường
                print(f"[ERROR] {error_msg}")
                # Ghi log lỗi nghiêm trọng vào database
                # Sử dụng conn nếu có, nếu không thì dùng conn_str
                if self.conn:
                    JobLogger.log_error(
                        job_name="Etl_Runner --> Error for function run()",
                        job_type=job_type,
                        message=error_msg,
                        conn_str=self.conn_str,
                        error_traceback=traceback.format_exc(),
                        error_level="ERROR",
                        operation_type="JOB_INITIALIZATION"
                    )
                else:
                    # Nếu connection chưa được tạo, dùng conn_str
                    JobLogger.log_error(
                        job_name="Etl_Runner --> Error for function run()",
                        job_type=job_type,
                        message=error_msg,
                        conn_str=self.conn_str if hasattr(self, 'conn_str') else None,
                        error_traceback=traceback.format_exc(),
                        error_level="ERROR",
                        operation_type="JOB_INITIALIZATION"
                    )
        finally:
            # Đảm bảo đóng connection và spark khi xong job
            try:
                if self.cursor:
                    self.cursor.close()
            except Exception as e:
                error_msg = f"Loi khi dong cursor: {str(e)}"
                print(f"[WARNING] {error_msg}")
                try:
                    if hasattr(self, 'conn_str') and self.conn_str:
                        JobLogger.log_error(
                            job_name="Etl_Runner --> Close Cursor Error",
                            job_type=job_type if hasattr(self, 'job_type') else None,
                            message=error_msg,
                            conn_str=self.conn_str,
                            error_traceback=traceback.format_exc(),
                            error_level="WARNING",
                            operation_type="CLEANUP"
                        )
                except Exception as log_err:
                    print(f"[WARNING] Khong the ghi log close cursor error: {log_err}")
            
            try:
                if self.conn:
                    self.conn.close()
            except Exception as e:
                error_msg = f"Loi khi dong connection: {str(e)}"
                print(f"[WARNING] {error_msg}")
                try:
                    if hasattr(self, 'conn_str') and self.conn_str:
                        JobLogger.log_error(
                            job_name="Etl_Runner --> Close Connection Error",
                            job_type=job_type if hasattr(self, 'job_type') else None,
                            message=error_msg,
                            conn_str=self.conn_str,
                            error_traceback=traceback.format_exc(),
                            error_level="WARNING",
                            operation_type="CLEANUP"
                        )
                except Exception as log_err:
                    print(f"[WARNING] Khong the ghi log close connection error: {log_err}")
            
            try:
                if self.spark:
                    print("Dang stop Spark session...")
                    # Giải phóng cache dim tables trước
                    try:
                        self._uncache_dim_tables()
                    except Exception as uncache_error:
                        print(f"[WARNING] Lỗi khi giải phóng cache dim: {uncache_error}")
                    
                    # Clear cache và temp views trước khi stop để giải phóng connections
                    try:
                        df.unpersist()
                        self.spark.catalog.clearCache()

                    except Exception as cache_error:
                        pass
                    
                    # Stop Spark session và clear context
                    # Quan trọng: stop() sẽ đóng tất cả connections và giải phóng resources
                    self.spark.stop()
                    # Đợi một chút để đảm bảo Spark session được đóng hoàn toàn và JDBC connections được cleanup
                    time.sleep(2)  # Tăng từ 1 giây lên 2 giây để đảm bảo cleanup hoàn tất
                    self.spark = None  # Set về None để tránh tái sử dụng
            except Exception as e:
                error_msg = f"Loi khi stop Spark session: {str(e)}"
                print(f"[ERROR] {error_msg}")
                try:
                    if hasattr(self, 'conn_str') and self.conn_str:
                        JobLogger.log_error(
                            job_name="Etl_Runner --> Stop Spark Session Error",
                            job_type=job_type if hasattr(self, 'job_type') else None,
                            message=error_msg,
                            conn_str=self.conn_str,
                            error_traceback=traceback.format_exc(),
                            error_level="ERROR",
                            operation_type="CLEANUP"
                        )
                except Exception as log_err:
                    print(f"[WARNING] Khong the ghi log stop Spark session error: {log_err}")
                # Vẫn set về None để tránh tái sử dụng session lỗi
                self.spark = None


