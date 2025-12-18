"""
Script test cho hàm delete_by_config trong config/utils.py

Cách sử dụng:
1. Đảm bảo đã cấu hình biến môi trường trong file .env
2. Chạy script: python test_delete_by_config.py
3. Kiểm tra kết quả trong database

Lưu ý: Script này sẽ THỰC SỰ XÓA dữ liệu trong database!
Nên test trên database test hoặc backup trước khi chạy.
"""

import pyodbc
from config.utils import EtlUtils
from config.settings import (
    SQL_SERVER_HOST, SQL_SERVER_PORT, SQL_SERVER_DATABASE,
    SQL_SERVER_USER, SQL_SERVER_PASSWORD, SQL_SERVER_DRIVER,
    SQL_SERVER_TRUST_SERVER_CERTIFICATE, SQL_SERVER_ENCRYPT
)


def create_connection():
    """Tạo kết nối đến SQL Server"""
    server_part = f"{SQL_SERVER_HOST},{SQL_SERVER_PORT}" if SQL_SERVER_PORT else SQL_SERVER_HOST
    database = SQL_SERVER_DATABASE
    
    conn_str = (
        f"DRIVER={{{SQL_SERVER_DRIVER}}};"
        f"SERVER={server_part};"
        f"DATABASE={database};"
        f"UID={SQL_SERVER_USER};"
        f"PWD={SQL_SERVER_PASSWORD};"
        f"TrustServerCertificate={SQL_SERVER_TRUST_SERVER_CERTIFICATE};"
        f"Encrypt={SQL_SERVER_ENCRYPT};"
        f"CharacterSet=UTF8"
    )
    
    return pyodbc.connect(conn_str)


def test_delete_by_config():
    """Test hàm delete_by_config với các điều kiện khác nhau"""
    
    # Tạo kết nối
    print("Đang kết nối database...")
    conn = create_connection()
    etl_utils = EtlUtils(conn)
    
    # ============================================
    # CẤU HÌNH TEST - THAY ĐỔI THEO NHU CẦU
    # ============================================
    TABLE_NAME = "your_table_name"  # Thay bằng tên bảng thực tế
    DELETE_COLUMN = "your_date_column"  # Thay bằng tên cột thực tế
    
    # Thông tin ngày tháng để test
    TEST_YEAR = 2024
    TEST_MONTH = 12
    TEST_DAY = 7
    
    print("\n" + "="*60)
    print("TEST HÀM delete_by_config")
    print("="*60)
    
    # ============================================
    # TEST 1: delete_condition == "day"
    # ============================================
    print("\n[TEST 1] delete_condition = 'day'")
    print(f"  - Xóa dữ liệu với {DELETE_COLUMN} = '{TEST_YEAR}-{str(TEST_MONTH).zfill(2)}-{str(TEST_DAY).zfill(2)}'")
    print("  - Định dạng: yyyy-mm-dd")
    
    # Uncomment dòng dưới để chạy test thực tế
    # etl_utils.delete_by_config(
    #     table_name=TABLE_NAME,
    #     delete_column=DELETE_COLUMN,
    #     delete_condition="day",
    #     year=TEST_YEAR,
    #     month=TEST_MONTH,
    #     day=TEST_DAY
    # )
    
    # ============================================
    # TEST 2: delete_condition == "month"
    # ============================================
    print("\n[TEST 2] delete_condition = 'month'")
    print(f"  - Xóa dữ liệu với {DELETE_COLUMN} = '{TEST_YEAR}{str(TEST_MONTH).zfill(2)}'")
    print("  - Định dạng: yyyymm")
    
    # Uncomment dòng dưới để chạy test thực tế
    # etl_utils.delete_by_config(
    #     table_name=TABLE_NAME,
    #     delete_column=DELETE_COLUMN,
    #     delete_condition="month",
    #     year=TEST_YEAR,
    #     month=TEST_MONTH
    # )
    
    # ============================================
    # TEST 3: delete_condition == "year"
    # ============================================
    print("\n[TEST 3] delete_condition = 'year'")
    print(f"  - Xóa dữ liệu với {DELETE_COLUMN} = '{TEST_YEAR}'")
    print("  - Định dạng: yyyy")
    
    # Uncomment dòng dưới để chạy test thực tế
    # etl_utils.delete_by_config(
    #     table_name=TABLE_NAME,
    #     delete_column=DELETE_COLUMN,
    #     delete_condition="year",
    #     year=TEST_YEAR
    # )
    
    # ============================================
    # TEST 4: delete_condition == "time"
    # ============================================
    print("\n[TEST 4] delete_condition = 'time'")
    print(f"  - Xóa dữ liệu với {DELETE_COLUMN} BETWEEN '{TEST_YEAR}{str(TEST_MONTH).zfill(2)}{str(TEST_DAY).zfill(2)}00' AND '{TEST_YEAR}{str(TEST_MONTH).zfill(2)}{str(TEST_DAY).zfill(2)}23'")
    print("  - Định dạng: yyyymmdd00 đến yyyymmdd23")
    
    # Uncomment dòng dưới để chạy test thực tế
    # etl_utils.delete_by_config(
    #     table_name=TABLE_NAME,
    #     delete_column=DELETE_COLUMN,
    #     delete_condition="time",
    #     year=TEST_YEAR,
    #     month=TEST_MONTH,
    #     day=TEST_DAY
    # )
    
    # ============================================
    # TEST 5: Kiểm tra lỗi với delete_condition không hợp lệ
    # ============================================
    print("\n[TEST 5] delete_condition không hợp lệ")
    print("  - Kiểm tra ValueError khi truyền delete_condition không hợp lệ")
    
    try:
        etl_utils.delete_by_config(
            table_name=TABLE_NAME,
            delete_column=DELETE_COLUMN,
            delete_condition="invalid",
            year=TEST_YEAR
        )
    except ValueError as e:
        print(f"  ✓ Lỗi đúng như mong đợi: {e}")
    
    print("\n" + "="*60)
    print("HOÀN THÀNH TEST")
    print("="*60)
    print("\nLưu ý: Các lệnh DELETE đã được comment để tránh xóa dữ liệu.")
    print("       Uncomment các dòng tương ứng để chạy test thực tế.")
    
    # Đóng kết nối
    conn.close()
    print("\nĐã đóng kết nối database.")


def test_with_mock_connection():
    """
    Test với mock connection (không cần database thực)
    Sử dụng để test logic mà không cần kết nối database
    """
    print("\n" + "="*60)
    print("TEST VỚI MOCK CONNECTION (Không cần database)")
    print("="*60)
    
    class MockConnection:
        def __init__(self):
            self.executed_sql = []
            self.executed_params = []
        
        def cursor(self):
            return MockCursor(self)
        
        def commit(self):
            pass
    
    class MockCursor:
        def __init__(self, conn):
            self.conn = conn
        
        def execute(self, sql, params=None):
            self.conn.executed_sql.append(sql)
            self.conn.executed_params.append(params or [])
            print(f"\n  SQL: {sql}")
            print(f"  Params: {params or []}")
    
    # Tạo mock connection
    mock_conn = MockConnection()
    etl_utils = EtlUtils(mock_conn)
    
    # Test các điều kiện
    print("\n[TEST] delete_condition = 'day'")
    etl_utils.delete_by_config(
        table_name="test_table",
        delete_column="date_col",
        delete_condition="day",
        year=2024,
        month=12,
        day=7
    )
    
    print("\n[TEST] delete_condition = 'month'")
    etl_utils.delete_by_config(
        table_name="test_table",
        delete_column="month_col",
        delete_condition="month",
        year=2024,
        month=12
    )
    
    print("\n[TEST] delete_condition = 'year'")
    etl_utils.delete_by_config(
        table_name="test_table",
        delete_column="year_col",
        delete_condition="year",
        year=2024
    )
    
    print("\n[TEST] delete_condition = 'time'")
    etl_utils.delete_by_config(
        table_name="test_table",
        delete_column="time_col",
        delete_condition="time",
        year=2024,
        month=12,
        day=7
    )
    
    print("\n✓ Đã test tất cả các điều kiện với mock connection")


if __name__ == "__main__":
    import sys
    
    print("Chọn phương thức test:")
    print("1. Test với database thực (cần cấu hình .env)")
    print("2. Test với mock connection (không cần database)")
    
    choice = input("\nNhập lựa chọn (1 hoặc 2, mặc định 2): ").strip() or "2"
    
    if choice == "1":
        try:
            test_delete_by_config()
        except Exception as e:
            print(f"\n❌ Lỗi: {e}")
            import traceback
            traceback.print_exc()
    else:
        test_with_mock_connection()

