import sys
import os

# Thêm /app vào PYTHONPATH để import được modules
sys.path.insert(0, '/app')

from jobs.etl_runner import Etl_Runner

def main(job_type=None, input_date=None):
    """
    Chạy job Etl_Runner
    
    Args:
        job_type: Loại job để filter (bắt buộc). Ví dụ: 'daily_revenue_summary'
        input_date: Ngày xử lý dạng 'yyyy-mm-dd' (tùy chọn)
    
    Raises:
        ValueError: Nếu job_type không được cung cấp
    """
    # Kiểm tra job_type là bắt buộc
    if not job_type:
        # Thử lấy từ command line arguments
        if len(sys.argv) > 1:
            job_type = sys.argv[1]
        else:
            raise ValueError("job_type là bắt buộc. Vui lòng truyền job_type khi gọi hàm hoặc qua command line: python main.py <job_type> [input_date]")
    
    # Lấy input_date từ command line nếu có
    if not input_date and len(sys.argv) > 2:
        input_date = sys.argv[2]
    
    # Debug: In tất cả arguments để kiểm tra
    print(f"[DEBUG] sys.argv: {sys.argv}")
    print(f"[DEBUG] Số lượng arguments: {len(sys.argv)}")
    
    print(f"Đang chạy job với job_type: {job_type}")
    if input_date:
        print(f"Input date: {input_date}")
    else:
        print("Input date: None (sẽ dùng ngày mặc định - hôm qua)")
    
    job = Etl_Runner()
    try:
        job.run(job_type=job_type, input_date=input_date)
        # Sync đã được gọi trong vòng lặp của hàm run() cho từng table_name
    finally:
        # Cleanup Spark resources sau khi job kết thúc
        # Lưu ý: Cleanup work directories trên worker sẽ được thực hiện bởi entrypoint.sh
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

