"""
Module để ghi log lỗi vào bảng etl_job_log trong PostgreSQL
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import traceback
from datetime import datetime
import signal
from contextlib import contextmanager

class JobLogger:
    """Class để ghi log lỗi vào database"""
    
    # Timeout settings
    CONNECT_TIMEOUT = 10  # Tăng từ 5 lên 10 giây
    EXECUTE_TIMEOUT = 5  # Timeout cho execute và commit (5 giây)
    
    @staticmethod
    @contextmanager
    def _timeout_handler(timeout_seconds):
        """Context manager để set timeout cho database operations"""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Database operation timeout after {timeout_seconds} seconds")

        # Chỉ set timeout trên Unix systems (không hỗ trợ Windows)
        old_handler = None
        try:
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            yield
        except (AttributeError, OSError):
            # Windows không hỗ trợ SIGALRM, bỏ qua timeout
            yield
        finally:
            if old_handler is not None:
                try:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
                except:
                    pass

    @staticmethod
    def log_error(
        job_name,
        message,
        conn_str=None,
        error_traceback=None,
        error_level="ERROR",
        table_name=None,
        sql_path=None,
        operation_type=None,
        year=None,
        month=None,
        day=None,
        delete_column=None,
        delete_condition=None,
        rows_affected=None,
        rows_inserted=None,
        execution_time_ms=None,
        error_code=None,
        job_type=None
    ):
        """
        Ghi log lỗi vào bảng etl_job_log
        
        Args:
            job_name: Tên job (bắt buộc)
            message: Thông điệp lỗi (bắt buộc)
            conn_str: Connection string (bắt buộc) - hàm sẽ tạo connection riêng và đóng sau khi insert log
            error_traceback: Stack trace (optional)
            error_level: Mức độ lỗi (ERROR, WARNING, INFO) - mặc định ERROR
            table_name: Tên bảng đang xử lý (optional)
            sql_path: Đường dẫn file SQL (optional)
            operation_type: Loại thao tác (DELETE, INSERT, QUERY, etc.) (optional)
            year, month, day: Thông tin ngày tháng (optional)
            delete_column: Cột delete (optional)
            delete_condition: Điều kiện delete (optional)
            rows_affected: Số dòng bị ảnh hưởng (optional)
            rows_inserted: Số dòng đã insert (optional)
            execution_time_ms: Thời gian thực thi (optional)
            error_code: Mã lỗi (optional)
            job_type: Loại job (optional)
        """
        if not conn_str:
            print(f"[LOG ERROR] Không thể ghi log: conn_str không được để trống")
            print(f"[LOG ERROR] Original error: {message}")
            return
        
        conn = None
        try:
            # conn_str là dict chứa thông tin kết nối PostgreSQL
            # Format: {'host': ..., 'port': ..., 'database': ..., 'user': ..., 'password': ...}
            if isinstance(conn_str, dict):
                conn = psycopg2.connect(
                    host=conn_str.get('host'),
                    port=conn_str.get('port', 5432),
                    database=conn_str.get('database'),
                    user=conn_str.get('user'),
                    password=conn_str.get('password'),
                    connect_timeout=JobLogger.CONNECT_TIMEOUT
                )
            else:
                # Nếu là string, thử parse hoặc dùng trực tiếp
                conn = psycopg2.connect(conn_str, connect_timeout=JobLogger.CONNECT_TIMEOUT)
            
            cursor = conn.cursor()
            # Chuẩn bị query insert cho PostgreSQL
            insert_query = """
                INSERT INTO etl_job_log (
                    job_name, job_type, table_name, sql_path, operation_type,
                    error_level, message, error_traceback, error_code,
                    year, month, day, delete_column, delete_condition,
                    rows_inserted, execution_time_ms,
                    created_by, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Lấy traceback nếu chưa có
            if error_traceback is None:
                error_traceback = traceback.format_exc()
            
            # Giới hạn độ dài để tránh lỗi (PostgreSQL TEXT không giới hạn nhưng để an toàn)
            message = str(message)[:8000] if message else None
            error_traceback = str(error_traceback)[:8000] if error_traceback else None
            
            # Thực thi insert và commit với timeout
            try:
                with JobLogger._timeout_handler(JobLogger.EXECUTE_TIMEOUT):
                    cursor.execute(insert_query, (
                        job_name,
                        job_type,
                        table_name,
                        sql_path,
                        operation_type,
                        error_level,
                        message,
                        error_traceback,
                        error_code,
                        year,
                        month,
                        day,
                        delete_column,
                        delete_condition,
                        rows_inserted,
                        execution_time_ms,  # Có thể là None nếu không được truyền vào
                        'ETL_JOB',  # created_by
                        datetime.now()
                    ))
                    conn.commit()
            except TimeoutError:
                # Nếu timeout, rollback và raise
                conn.rollback()
                raise TimeoutError(f"Database operation timeout after {JobLogger.EXECUTE_TIMEOUT} seconds")
            
            print(f"[LOG] Đã ghi log vào etl_job_log: {error_level} - {message[:100] if message else ''}")
            
        except Exception as log_error:
            # Nếu không thể ghi log vào DB, in ra console
            print(f"[LOG ERROR] Không thể ghi log vào database: {log_error}")
            print(f"[LOG ERROR] Original error: {message}")
        finally:
            # Luôn đóng connection sau khi insert log thành công hoặc khi có lỗi
            if conn:
                try:
                    conn.close()
                except Exception as close_error:
                    print(f"[LOG WARNING] Lỗi khi đóng connection: {close_error}")
    
    @staticmethod
    def log_warning(job_name, warning_message, conn_str=None, **kwargs):
        """Ghi log cảnh báo"""
        JobLogger.log_error(
            job_name=job_name,
            message=warning_message,
            conn_str=conn_str,
            error_level="WARNING",
            **kwargs
        )
    
    @staticmethod
    def log_info(job_name, info_message, conn_str=None, **kwargs):
        """Ghi log thông tin"""
        JobLogger.log_error(
            job_name=job_name,
            message=info_message,
            conn_str=conn_str,
            error_level="INFO",
            **kwargs
        )

