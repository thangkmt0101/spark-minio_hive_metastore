import sys
import argparse
from datetime import datetime, timedelta

# Thêm /app vào PYTHONPATH để import được modules
sys.path.insert(0, '/app')

from jobs.etl_runner import Etl_Runner


def _parse_cli_args():
    """
    Ưu tiên tham số truyền qua CLI.
    Hỗ trợ cả kiểu cũ (positional) và kiểu mới (--job_type, --from_date, --to_date).
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--job_type", dest="job_type_flag", help="Loại job cần chạy")
    parser.add_argument("--from_date", dest="from_date", help="Ngày bắt đầu (yyyy-mm-dd)")
    parser.add_argument("--to_date", dest="to_date", help="Ngày kết thúc (yyyy-mm-dd)")
    parser.add_argument("job_type_pos", nargs="?", help="job_type positional (giữ tương thích)")
    parser.add_argument("input_date_pos", nargs="?", help="input_date positional (giữ tương thích)")
    args, _ = parser.parse_known_args()

    job_type = args.job_type_flag or args.job_type_pos
    input_date = args.input_date_pos

    return job_type, input_date, args.from_date, args.to_date


def _build_date_list(from_date: str, to_date: str):
    """Trả về danh sách ngày (chuỗi yyyy-mm-dd) từ from_date đến to_date, bao gồm hai đầu."""
    if not from_date or not to_date:
        raise ValueError("from_date và to_date đều là bắt buộc khi chạy theo khoảng ngày")

    try:
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("from_date/to_date phải theo định dạng yyyy-mm-dd")

    if start > end:
        raise ValueError("from_date phải nhỏ hơn hoặc bằng to_date")

    days = []
    current = start
    while current <= end:
        days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def main(job_type=None, input_date=None, from_date=None, to_date=None):
    """
    Chạy job Etl_Runner
    
    Args:
        job_type: Loại job để filter (bắt buộc). Ví dụ: 'daily_revenue_summary'
        input_date: Ngày xử lý dạng 'yyyy-mm-dd' (tùy chọn)
    
    Raises:
        ValueError: Nếu job_type không được cung cấp
    """
    # Ưu tiên tham số truyền trực tiếp vào hàm; nếu thiếu thì lấy từ CLI
    cli_job_type, cli_input_date, cli_from_date, cli_to_date = _parse_cli_args()
    job_type = job_type or cli_job_type
    input_date = input_date or cli_input_date
    from_date = from_date or cli_from_date
    to_date = to_date or cli_to_date

    if not job_type:
        raise ValueError("job_type là bắt buộc. Vui lòng truyền --job_type hoặc positional <job_type>")

    # Xác định danh sách ngày cần chạy
    if from_date or to_date:
        date_list = _build_date_list(from_date, to_date)
        print(f"Đang chạy job với job_type: {job_type}")
        print(f"Chạy lần lượt từ {from_date} đến {to_date} ({len(date_list)} ngày)")
    else:
        # Giữ nguyên hành vi cũ: nếu không truyền input_date sẽ dùng mặc định (hôm qua)
        date_list = [input_date]
        print(f"Đang chạy job với job_type: {job_type}")
        if input_date:
            print(f"Input date: {input_date}")
        else:
            print("Input date: None (sẽ dùng ngày mặc định - hôm qua)")

    job = Etl_Runner()
    try:
        # Truyền list ngày một lần để tái sử dụng Spark session cho toàn bộ batch
        job.run(job_type=job_type, input_date=date_list[0], date_list=date_list)
        # Sync đã được gọi trong vòng lặp của hàm run() cho từng table_name
    finally:
        # Cleanup Spark resources sau khi job kết thúc (phòng trường hợp run() lỗi giữa chừng)
        if hasattr(job, 'spark') and job.spark is not None:
            try:
                print("[CLEANUP] Đang cleanup Spark resources...")
                job.spark.catalog.clearCache()
                job.spark.stop()
                print("[CLEANUP] Đã cleanup Spark resources thành công")
            except Exception as cleanup_error:
                print(f"[WARNING] Lỗi khi cleanup Spark resources: {cleanup_error}")

# Chạy job
if __name__ == "__main__":
    main()

