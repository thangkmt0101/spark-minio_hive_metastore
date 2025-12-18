import os
import sys
import shutil
from tempfile import TemporaryDirectory
import traceback
from config.spark_session import SparkSessionBuilder
from config.time_config import TimeService
from config.utils import EtlUtils
from config.job_etl_logger import JobLogger
from config.settings import (
    SQL_SERVER_HOST, SQL_SERVER_PORT, SQL_SERVER_DATABASE,
    SQL_SERVER_USER, SQL_SERVER_PASSWORD, SQL_SERVER_DRIVER,
    SQL_SERVER_TRUST_SERVER_CERTIFICATE, SQL_SERVER_ENCRYPT,
    OUTPUT_PATH, SPARK_JDBC_BATCH_SIZE, SPARK_JDBC_NUM_PARTITIONS
)
from jinja2 import Template
import pyodbc
import pandas as pd

# Cấu hình encoding UTF-8 cho Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    
class Etl_Runner:

    def __init__(self):
        """Khởi tạo và tạo connection string để tái sử dụng"""
        # Khởi tạo mặc định để tránh AttributeError trong finally khi lỗi sớm
        self.conn = None
        self.cursor = None
        self.spark = None

        # Tạo connection string từ cấu hình SQL Server
        database = SQL_SERVER_DATABASE
        
        # ODBC connection string sử dụng format: host,port (dấu phẩy)
        server_part_odbc = f"{SQL_SERVER_HOST},{SQL_SERVER_PORT}" if SQL_SERVER_PORT else SQL_SERVER_HOST
        
        # JDBC URL sử dụng format: host:port (dấu hai chấm)
        if SQL_SERVER_PORT:
            server_part_jdbc = f"{SQL_SERVER_HOST}:{SQL_SERVER_PORT}"
        else:
            server_part_jdbc = SQL_SERVER_HOST
        
        # Chuyển đổi giá trị từ JDBC format (true/false) sang ODBC format (yes/no)
        # ODBC Driver 18+ yêu cầu "yes"/"no", không chấp nhận "true"/"false"
        def jdbc_to_odbc(value):
            """Chuyển đổi giá trị từ JDBC format sang ODBC format"""
            if value.lower() == "true":
                return "yes"
            elif value.lower() == "false":
                return "no"
            else:
                return value  # Giữ nguyên nếu là "strict" hoặc giá trị khác
        
        trust_cert_odbc = jdbc_to_odbc(SQL_SERVER_TRUST_SERVER_CERTIFICATE)
        encrypt_odbc = jdbc_to_odbc(SQL_SERVER_ENCRYPT)
        
        # ODBC Driver 18+ yêu cầu TrustServerCertificate và Encrypt
        # Thêm CharacterSet=UTF8 để hỗ trợ Unicode
        self.conn_str = f"DRIVER={{{SQL_SERVER_DRIVER}}};SERVER={server_part_odbc};DATABASE={database};UID={SQL_SERVER_USER};PWD={SQL_SERVER_PASSWORD};TrustServerCertificate={trust_cert_odbc};Encrypt={encrypt_odbc};CharacterSet=UTF8"
    
        # Tạo JDBC URL để tái sử dụng cho Spark DataFrame write
        # JDBC driver sử dụng true/false/strict và format host:port
        self.jdbc_url = f"jdbc:sqlserver://{server_part_jdbc};databaseName={database};trustServerCertificate={SQL_SERVER_TRUST_SERVER_CERTIFICATE};encrypt={SQL_SERVER_ENCRYPT}"
        # Tạo JDBC properties để tái sử dụng với bulk insert options
        self.jdbc_properties = {
            "user": SQL_SERVER_USER,
            "password": SQL_SERVER_PASSWORD,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "batchsize": str(SPARK_JDBC_BATCH_SIZE),  # Số dòng insert mỗi batch (tối ưu cho bulk insert)
            "isolationLevel": "READ_UNCOMMITTED"  # Tối ưu performance
        }
        # Lazy initialization: Spark session sẽ được tạo khi cần (trong run method)
        self.spark = None

    def run(self, job_type=None, input_date=None):
        """
        Chạy job chính
        
        Args:
            job_type: Loại job để filter (optional). Nếu None thì lấy tất cả job active
            input_date: Ngay 'yyyy-mm-dd'; None -> mac dinh hom qua
        """
        # Lazy initialization: Tạo Spark session chỉ khi cần
        if self.spark is None:
            self.spark = SparkSessionBuilder.get_spark("Daily Revenue Job")
        info = TimeService.get_target_date(input_date)
        y_year = info["year"]
        y_quarter = info["quarter"]
        y_month = info["month"]
        y_day = info["day"]

        try: 
            # 0. Kết nối database SQL và đọc bảng etl_job để lấy table_name và sql_path
            print("Dang ket noi database SQL...")
            
            # Sử dụng conn_str đã tạo trong __init__
            self.conn = pyodbc.connect(self.conn_str)
            self.cursor = self.conn.cursor()
            
            # Đọc bảng etl_job để lấy danh sách table_name, sql_path và delete_column
            # Thêm điều kiện WHERE job_type nếu được truyền vào
            if job_type:
                etl_job_query = """
                    SELECT table_name, sql_path, delete_column, delete_condition
                    FROM view_etl_job 
                    WHERE is_active = 1 AND job_type = ?
                    ORDER BY id
                """
                self.cursor.execute(etl_job_query, (job_type,))
            else:
                # Nếu không có job_type, không chạy (theo yêu cầu chỉ chạy khi có job_type)
                raise ValueError("job_type la bat buoc. Vui long truyen job_type khi goi ham run()")
            
            etl_job_results = self.cursor.fetchall()
            
            if not etl_job_results:
                raise ValueError("Khong tim thay job active trong bang etl_job")
            
            print(f"Tim thay {len(etl_job_results)} job(s) active. Bat dau xu ly...")
            # Duyệt qua từng job và xử lý
            for job_idx, etl_job_result in enumerate(etl_job_results, 1):
                try:
                    print(f"\n[{job_idx}/{len(etl_job_results)}] Dang xu ly job... {etl_job_result}")
                    
                    # Lấy các giá trị từ etl_job_result
                    table_name = etl_job_result[0]
                    sql_path = etl_job_result[1]
                    delete_column = etl_job_result[2]
                    delete_condition = etl_job_result[3]  # Lấy delete_condition từ bảng etl_job
                    
                    # Đọc file SQL từ sql_path
                    print(f"Dang doc file SQL: {sql_path}")
                    if not os.path.exists(sql_path):
                        raise FileNotFoundError(f"Khong tim thay file SQL: {sql_path}")
                    
                    with open(sql_path, 'r', encoding='utf-8') as f:
                        sql_template = f.read()
                    
                    # Render template với các biến year, month, day
                    template = Template(sql_template)
                    sql_query = template.render(
                        year=str(y_year),
                        quarter=str(y_quarter),
                        month=str(y_month).zfill(2), #kết quả định dạng: "01"
                        day=str(y_day).zfill(2) #kết quả định dạng: "01"
                    )
                    
                    print(f"Dang chay SQL query...")
                    # Chạy SQL query bằng spark.sql()
                    df = self.spark.sql(sql_query)
                    df.createOrReplaceTempView(table_name + '_temp')

                    # Cache kết quả count để tránh tính toán lại (tối ưu performance)
                    row_count = df.count()
                    print(f"So dong trong ket qua: {row_count}")
                    #df.show(10, truncate=False)
                    
                    # Delete dữ liệu cũ trước khi insert (nếu có delete_column)
                    if delete_column:
                        # Sử dụng delete_condition từ bảng etl_job, mặc định là "day" nếu không có
                        delete_condition_value = delete_condition if delete_condition else "day"
                        print(f"Dang xoa du lieu cu tu bang {table_name} dua tren cot {delete_column} voi dieu kien {delete_condition_value}...")
                        try:
                            # Sử dụng EtlUtils để xóa dữ liệu với connection hiện tại (tái sử dụng)
                            # Tái sử dụng connection thay vì tạo mới để tối ưu performance
                            EtlUtils.delete_by_config(
                                conn_str=self.conn,  # Tái sử dụng connection hiện tại thay vì tạo mới
                                table_name=table_name,
                                delete_column=delete_column,
                                delete_condition=delete_condition_value,  # Lấy từ bảng etl_job
                                year=y_year,
                                month=y_month,
                                day=y_day   
                            )
                            print(f"Da xoa du lieu cu thanh cong")
                        except Exception as delete_error:
                            error_msg = f"Loi khi xoa du lieu tu bang {table_name}: {str(delete_error)}"
                            
                            # Ghi log lỗi vào database (tái sử dụng connection)
                            JobLogger.log_error(
                                job_name="Etl_Runner --> delete_column: {delete_column}",
                                job_type=job_type,
                                message=error_msg,
                                conn=self.conn,  # Tái sử dụng connection
                                error_traceback=traceback.format_exc(),
                                error_level="ERROR",
                                table_name=table_name,
                                sql_path=sql_path,
                                operation_type="DELETE",
                                year=y_year,
                                month=y_month,
                                day=y_day,
                                delete_column=delete_column,
                                delete_condition=delete_condition_value
                            )
                    
                    # Insert dữ liệu vào bảng table_name trong SQL Server bằng bulk insert
                    print(f"Dang bulk insert du lieu vao bang: {table_name}")
                    print(f"So dong can insert: {row_count}")
                    
                    # Sử dụng bulk insert với các options tối ưu
                    # numPartitions: số partition để parallel insert (tối ưu performance)
                    # batchsize: đã được set trong jdbc_properties
                    # driver: chỉ định rõ ràng driver class để tránh ClassNotFoundException
                    df.write \
                        .mode("append") \
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                        .option("numPartitions", str(SPARK_JDBC_NUM_PARTITIONS)) \
                        .option("batchsize", str(SPARK_JDBC_BATCH_SIZE)) \
                        .jdbc(url=self.jdbc_url, table=table_name, properties=self.jdbc_properties)
                    
                    print(f"Da bulk insert thanh cong {row_count} dong vao bang {table_name}")
                    # Ghi log thong tin so ban ghi da insert (tái sử dụng connection)
                    JobLogger.log_info(
                        job_name="Etl_Runner --> Success insert",
                        info_message=f"Insert thanh cong {row_count} dong vao {table_name}",
                        conn=self.conn,  # Tái sử dụng connection
                        table_name=table_name,
                        sql_path=sql_path,
                        operation_type="INSERT",
                        year=y_year,
                        month=y_month,
                        day=y_day,
                        rows_inserted=row_count,
                        job_type=job_type
                    )
                    
                except Exception as e:
                    error_msg = f"Loi khi xu ly job cho bang {table_name}: {str(e)}"
                    
                    # Ghi log lỗi vào database, bổ sung số bản ghi đã đếm (nếu có) (tái sử dụng connection)
                    JobLogger.log_error(
                        job_name="Etl_Runner --> Error for {job_idx} + {etl_job_result}",
                        job_type=job_type,
                        message=error_msg,
                        conn=self.conn,  # Tái sử dụng connection
                        error_traceback=traceback.format_exc(),
                        error_level="ERROR",
                        table_name=table_name,
                        sql_path=sql_path,
                        operation_type="JOB_EXECUTION",
                        year=y_year,
                        month=y_month,
                        day=y_day,
                        rows_inserted=row_count if 'row_count' in locals() else None
                    )
                    
        except Exception as e:
            error_msg = f"Loi nghiem trong trong job Etl_Runner: {str(e)}"
            
            # Ghi log lỗi nghiêm trọng vào database
            # Sử dụng conn_str vì có thể connection chưa được tạo
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
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            if self.spark:
                self.spark.stop()
            print("Da dong tat ca ket noi va dung Spark session")

