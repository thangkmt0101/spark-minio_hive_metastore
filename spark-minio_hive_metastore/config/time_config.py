from datetime import datetime, timedelta

class TimeService:
    """
    Service xử lý ngày tháng phục vụ partition Iceberg:
    - Lấy ngày mặc định = hôm qua
    - Lấy ngày từ input (yyyy-mm-dd)
    """

    @staticmethod
    def get_target_date(input_date: str = None):
        """
        Trả về dict: {year, month, day, quarter, date_str}
        - input_date = None → yesterday
        - input_date = 'yyyy-mm-dd' → dùng ngày truyền vào
        """

        if input_date:
            # Ngày truyền vào dạng yyyy-mm-dd
            d = datetime.strptime(input_date, "%Y-%m-%d")
        else:
            # Mặc định = hôm qua
            d = datetime.now() - timedelta(days=1)

        quarter_label = (d.month - 1) // 3 + 1  # 1..4
        quarter = f"Q{quarter_label} {d.strftime('%Y')}"

        return {
            "year": d.strftime("%Y"),
            "month": d.strftime("%m"),
            "day": d.strftime("%d"),
            "quarter": quarter,
            "date_str": d.strftime("%Y-%m-%d")
        }


 
    # info = get_target_date('2025-12-11')
    # y_year = info["year"]
    # y_quarter = info["quarter"]
    # y_month = info["month"]
    # y_day = info["day"]

    # print(y_year, y_quarter, y_month, y_day)
        