import pyodbc


class EtlUtils:
    # --- Format helpers ---
    @staticmethod
    def format_day(year, month, day):
        return f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}"

    @staticmethod
    def format_month_id(year, month):
        return f"{year}{str(month).zfill(2)}"

    @staticmethod
    def format_year_id(year):
        return f"{year}"

    @staticmethod
    def format_time_start(year, month, day):
        """Format thời gian bắt đầu: yyyymmdd00"""
        return f"{year}{str(month).zfill(2)}{str(day).zfill(2)}00"

    @staticmethod
    def format_time_end(year, month, day):
        """Format thời gian kết thúc: yyyymmdd23"""
        return f"{year}{str(month).zfill(2)}{str(day).zfill(2)}23"


   # Tái sử dụng connection thay vì tạo mới để tối ưu performance
                           

    # --- Main delete function ---.
    @staticmethod
    def delete_by_config(conn_str, table_name, delete_column, delete_condition,
                         year=None, month=None, day=None):

        if delete_condition == "day":
            value = EtlUtils.format_day(year, month, day)  # yyyy-mm-dd
            where = f"{delete_column} = ?"
            params = [value]

        elif delete_condition == "month":
            value = EtlUtils.format_month_id(year, month)  # yyyymm
            where = f"{delete_column} = ?"
            params = [value]

        elif delete_condition == "year":
            value = EtlUtils.format_year_id(year) # yyyy
            where = f"{delete_column} = ?"
            params = [value]

        elif delete_condition == "time":
            value_start = EtlUtils.format_time_start(year, month, day)  # yyyymmdd00
            value_end = EtlUtils.format_time_end(year, month, day)  # yyyymmdd23
            where = f"{delete_column} BETWEEN ? AND ?"
            params = [value_start, value_end]

        elif delete_condition == "quarter":
            # format: YYYYQn (n=1..4) dựa trên month được truyền vào
            if month is None or year is None:
                raise ValueError("delete_condition=quarter yêu cầu year và month")
            quarter_num = (int(month) - 1) // 3 + 1
            value = f"{year}Q{quarter_num}"
            where = f"{delete_column} = ?"
            params = [value]

        elif delete_condition in (None, "", "none"):
            # Không xóa nếu không cấu hình
            return

        else:
            raise ValueError("delete_condition phải là: day | month | year | time | quarter | none")

        sql = f"DELETE FROM {table_name} WHERE {where}"
        
        # Sử dụng self.conn_str để tạo connection mới, dùng xong và đóng lại
        if conn_str:
            conn = pyodbc.connect(conn_str)
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                conn.commit()
                print(f"[ETL] Deleted from {table_name} where {where} params={params}")
            finally:
                conn.close()
