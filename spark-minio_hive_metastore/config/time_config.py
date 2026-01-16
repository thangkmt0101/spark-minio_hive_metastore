from datetime import datetime, timedelta, timezone
from calendar import monthrange
class TimeService:
    """
    Service xử lý ngày tháng phục vụ partition Iceberg:
    - Lấy ngày mặc định = hôm qua
    - Lấy ngày từ input (yyyy-mm-dd)
    """

    @staticmethod
    def get_target_date(input_date: str = None):
        """
        Trả về dict: {year, month, day, quarter, week, date_str}
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

        # Tính tuần ISO (tuần bắt đầu từ thứ 2)
        iso_year, iso_week, iso_weekday = d.isocalendar()
        week = f"{iso_year}{iso_week:02d}"

        return {
            "year": d.strftime("%Y"),
            "month": d.strftime("%m"),
            "day": d.strftime("%d"),
            "quarter": quarter,
            "week": week,
            "date_str": d.strftime("%Y-%m-%d")
        }

    @staticmethod
    def get_utc_range_from_vn_date(vn_date_str: str):
        """
        Tính toán khoảng UTC tương ứng với ngày UTC+7 (giờ VN)
        
        Input:
            vn_date_str: 'YYYY-MM-DD' (theo giờ VN / UTC+7)
        
        Output:
            dict chứa:
            - year: năm UTC
            - month: tháng UTC
            - day_ranges: list các dict {day, hour_start, hour_end}
                để dùng trong WHERE clause SQL
        """
        VN_TZ = timezone(timedelta(hours=7))
        UTC_TZ = timezone.utc

        # 00:00 ngày VN
        start_vn = datetime.strptime(vn_date_str, "%Y-%m-%d").replace(tzinfo=VN_TZ)
        end_vn = start_vn + timedelta(days=1)

        # Quy đổi sang UTC
        start_utc = start_vn.astimezone(UTC_TZ)
        end_utc = end_vn.astimezone(UTC_TZ)

        day_ranges = []

        # Case 1: cùng ngày UTC
        if start_utc.date() == end_utc.date():
            day_ranges.append({
                "year": start_utc.strftime("%Y"),
                "month": start_utc.strftime("%m"),
                "day": start_utc.strftime("%d"),
                "hour_start": f"{start_utc.hour:02d}",
                "hour_end": f"{end_utc.hour - 1:02d}",
            })
        else:
            # Case 2: qua 2 ngày UTC (case phổ biến)
            day_ranges.append({
                "year": start_utc.strftime("%Y"),
                "month": start_utc.strftime("%m"),
                "day": start_utc.strftime("%d"),
                "hour_start": f"{start_utc.hour:02d}",
                "hour_end": "23",
            })
            day_ranges.append({
                "year": end_utc.strftime("%Y"),
                "month": end_utc.strftime("%m"),
                "day": end_utc.strftime("%d"),
                "hour_start": "00",
                "hour_end": f"{end_utc.hour - 1:02d}",
            })

        return {
            "year": start_utc.strftime("%Y"),  # Giữ lại để tương thích
            "month": start_utc.strftime("%m"),  # Giữ lại để tương thích
            "day_ranges": day_ranges,
        }

    @staticmethod
    def get_utc_range_from_vn_month(vn_date_str: str):
        """
        Tính toán khoảng UTC tương ứng với tháng theo giờ VN (UTC+7)
        
        Input:
            vn_date_str: 'YYYY-MM-DD' (ngày theo giờ VN, ví dụ: '2026-01-07')
                        Hàm sẽ tự động lấy năm-tháng từ ngày này để tính cho cả tháng
        
        Output:
            dict chứa:
            - year: năm UTC đầu tiên
            - month: tháng UTC đầu tiên
            - day_ranges: list tất cả các dict {year, month, day, hour_start, hour_end}
                của tất cả các ngày trong tháng
        """
       
        
        # Parse input date string 'yyyy-mm-dd' để lấy năm và tháng
        try:
            d = datetime.strptime(vn_date_str, "%Y-%m-%d")
            vn_year = d.strftime("%Y")
            vn_month = d.strftime("%m")
            year_int = d.year
            month_int = d.month
        except ValueError:
            raise ValueError(f"Format input_date không hợp lệ: {vn_date_str}. Cần format 'YYYY-MM-DD' (ví dụ: '2026-01-07')")
        
        # Lấy số ngày trong tháng
        _, num_days = monthrange(year_int, month_int)
        
        # Gộp tất cả day_ranges từ các ngày trong tháng
        all_day_ranges = []
        for day in range(1, num_days + 1):
            vn_date_full = f"{vn_year}-{vn_month}-{day:02d}"
            day_info = TimeService.get_utc_range_from_vn_date(vn_date_full)
            all_day_ranges.extend(day_info["day_ranges"])
        
        return {
            "year": vn_year,  # Giữ lại để tương thích
            "month": vn_month,  # Giữ lại để tương thích
            "day_ranges": all_day_ranges,
        }

    @staticmethod
    def get_utc_range_from_vn_week(vn_date_str: str):
        """
        Tính toán khoảng UTC tương ứng với tuần theo giờ VN (UTC+7)
        
        Input:
            vn_date_str: 'YYYY-MM-DD' (ngày theo giờ VN, ví dụ: '2026-01-07')
                        Hàm sẽ tự động tính tuần chứa ngày này (thứ 2 đến chủ nhật)
        
        Output:
            dict chứa:
            - year: năm UTC đầu tiên
            - month: tháng UTC đầu tiên
            - day_ranges: list tất cả các dict {year, month, day, hour_start, hour_end}
                của tất cả các ngày trong tuần (7 ngày)
        """
        # Parse input date string 'yyyy-mm-dd'
        try:
            d = datetime.strptime(vn_date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Format input_date không hợp lệ: {vn_date_str}. Cần format 'YYYY-MM-DD' (ví dụ: '2026-01-07')")
        
        # Tính thứ 2 đầu tuần chứa ngày input
        # weekday() trả về: Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6
        # Ví dụ: Nếu input là thứ 5 (Thursday=3), thì days_from_monday = 3
        # monday = input - 3 ngày = thứ 2 đầu tuần
        days_from_monday = d.weekday()  # 0 = Monday, 1 = Tuesday, ..., 6 = Sunday
        monday = d - timedelta(days=days_from_monday)
        
        # Gộp tất cả day_ranges từ 7 ngày trong tuần (thứ 2 đến chủ nhật)
        # day_offset: 0=thứ 2, 1=thứ 3, 2=thứ 4, 3=thứ 5, 4=thứ 6, 5=thứ 7, 6=chủ nhật
        all_day_ranges = []
        for day_offset in range(7):
            week_day = monday + timedelta(days=day_offset)
            vn_date_full = week_day.strftime("%Y-%m-%d")
            day_info = TimeService.get_utc_range_from_vn_date(vn_date_full)
            all_day_ranges.extend(day_info["day_ranges"])
        
        # Lấy năm và tháng từ thứ 2 đầu tuần để tương thích
        vn_year = monday.strftime("%Y")
        vn_month = monday.strftime("%m")
        
        return {
            "year": vn_year,  # Giữ lại để tương thích
            "month": vn_month,  # Giữ lại để tương thích
            "day_ranges": all_day_ranges,
        }

    @staticmethod
    def get_utc_range_from_vn_quarter(vn_date_str: str):
        """
        Tính toán khoảng UTC tương ứng với quý theo giờ VN (UTC+7)
        
        Input:
            vn_date_str: 'YYYY-MM-DD' (ngày theo giờ VN, ví dụ: '2026-01-07')
                        Hàm sẽ tự động lấy năm và tính quý từ tháng của ngày này
        
        Output:
            dict chứa:
            - year: năm UTC đầu tiên
            - month: tháng UTC đầu tiên
            - day_ranges: list tất cả các dict {year, month, day, hour_start, hour_end}
                của tất cả các ngày trong quý
        """
        # Parse input date string 'yyyy-mm-dd' để lấy năm và tháng
        try:
            d = datetime.strptime(vn_date_str, "%Y-%m-%d")
            vn_year = d.strftime("%Y")
            vn_month = d.month
            year_int = d.year
        except ValueError:
            raise ValueError(f"Format input_date không hợp lệ: {vn_date_str}. Cần format 'YYYY-MM-DD' (ví dụ: '2026-01-07')")
        
        # Tính quý từ tháng (1-3: Q1, 4-6: Q2, 7-9: Q3, 10-12: Q4)
        quarter = (vn_month - 1) // 3 + 1
        
        # Tính các tháng trong quý
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3
        
        # Gộp tất cả day_ranges từ các ngày trong quý
        all_day_ranges = []
        for month in range(start_month, end_month + 1):
            _, num_days = monthrange(year_int, month)
            for day in range(1, num_days + 1):
                vn_date_full = f"{vn_year}-{month:02d}-{day:02d}"
                day_info = TimeService.get_utc_range_from_vn_date(vn_date_full)
                all_day_ranges.extend(day_info["day_ranges"])
        
        return {
            "year": vn_year,  # Giữ lại để tương thích
            "month": f"{start_month:02d}",  # Giữ lại để tương thích
            "day_ranges": all_day_ranges,
        }

    @staticmethod
    def get_utc_range_from_vn_year(vn_date_str: str):
        """
        Tính toán khoảng UTC tương ứng với năm theo giờ VN (UTC+7)
        
        Input:
            vn_date_str: 'YYYY-MM-DD' (ngày theo giờ VN, ví dụ: '2026-01-07')
                        Hàm sẽ tự động lấy năm từ ngày này để tính cho cả năm
        
        Output:
            dict chứa:
            - year: năm UTC đầu tiên
            - month: tháng UTC đầu tiên
            - day_ranges: list tất cả các dict {year, month, day, hour_start, hour_end}
                của tất cả các ngày trong năm
        """
        # Parse input date string 'yyyy-mm-dd' để lấy năm
        try:
            d = datetime.strptime(vn_date_str, "%Y-%m-%d")
            vn_year = d.strftime("%Y")
            year_int = d.year
        except ValueError:
            raise ValueError(f"Format input_date không hợp lệ: {vn_date_str}. Cần format 'YYYY-MM-DD' (ví dụ: '2026-01-07')")
        
        # Gộp tất cả day_ranges từ tất cả các ngày trong năm
        all_day_ranges = []
        for month in range(1, 13):
            _, num_days = monthrange(year_int, month)
            for day in range(1, num_days + 1):
                vn_date_full = f"{vn_year}-{month:02d}-{day:02d}"
                day_info = TimeService.get_utc_range_from_vn_date(vn_date_full)
                all_day_ranges.extend(day_info["day_ranges"])
        
        return {
            "year": vn_year,  # Giữ lại để tương thích
            "month": "01",  # Giữ lại để tương thích
            "day_ranges": all_day_ranges,
        }
        



# #Test code (comment out khi không dùng)
# info = TimeService.get_utc_range_from_vn_week('2026-01-08')

# y_day = info["day_ranges"]

# print(y_day)   