"""
Module để ghi log lỗi vào bảng job_etl_log
"""

import pyodbc
import traceback
from datetime import datetime

class JobLogger:
    """Class để ghi log lỗi vào database"""
    
    @staticmethod
    def log_error(
        job_name,
        message,
        conn_str=None,
        conn=None,
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
        Ghi log lỗi vào bảng job_etl_log
        
        Args:
            job_name: Tên job (bắt buộc)
            message: Thông điệp lỗi (bắt buộc)
            conn: Connection object để tái sử dụng (ưu tiên)
            conn_str: Connection string nếu không có conn (fallback)
            error_traceback: Stack trace (optional)
            error_level: Mức độ lỗi (ERROR, WARNING, INFO) - mặc định ERROR
            table_name: Tên bảng đang xử lý (optional)
            sql_path: Đường dẫn file SQL (optional)
            operation_type: Loại thao tác (DELETE, INSERT, QUERY, etc.) (optional)
            year, month, day: Thông tin ngày tháng (optional)
            delete_column: Cột delete (optional)
            delete_condition: Điều kiện delete (optional)
            rows_affected: Số dòng bị ảnh hưởng (optional)
            execution_time_ms: Thời gian thực thi (optional)
            error_code: Mã lỗi (optional)
            job_type: Loại job (optional)
        """
        try:
            # Tối ưu: Tái sử dụng connection nếu có, nếu không thì tạo mới
            should_close = False
            if conn:
                # Sử dụng connection hiện có (tái sử dụng)
                pass
            elif conn_str:
                # Tạo connection mới nếu không có connection để tái sử dụng
                conn = pyodbc.connect(conn_str)
                should_close = True
            else:
                raise ValueError("Cần cung cấp conn hoặc conn_str để ghi log vào database")
            
            try:
                cursor = conn.cursor()
                
                # Chuẩn bị query insert (bỏ server_name và database_name vì không có trong bảng)
                insert_query = """
                    INSERT INTO [dbo].[job_etl_log] (
                        job_name, job_type, table_name, sql_path, operation_type,
                        error_level, message, error_traceback, error_code,
                        year, month, day, delete_column, delete_condition,
                        rows_inserted, execution_time_ms,
                        created_by, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Lấy traceback nếu chưa có
                if error_traceback is None:
                    error_traceback = traceback.format_exc()
                
                # Giới hạn độ dài để tránh lỗi
                message = str(message)[:8000] if message else None
                error_traceback = str(error_traceback)[:8000] if error_traceback else None
                
                # Thực thi insert
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
                    execution_time_ms,
                    'ETL_JOB',  # created_by
                    datetime.now()
                ))
                
                conn.commit()
                print(f"[LOG] Đã ghi log lỗi vào job_etl_log: {error_level} - {message[:100]}")
                
            finally:
                if should_close:
                    conn.close()
                
        except Exception as log_error:
            # Nếu không thể ghi log vào DB, in ra console
            print(f"[LOG ERROR] Không thể ghi log vào database: {log_error}")
            print(f"[LOG ERROR] Original error: {message}")
    
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

