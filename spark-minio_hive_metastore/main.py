import sys
from jobs.etl_runner import Etl_Runner

def main(job_type=None, input_date=None):
    """
    Chạy job Etl_Runner
    
    Args:
        job_type: Loại job để filter (bắt buộc). Ví dụ: 'daily_revenue_summary'
    
    Raises:
        ValueError: Nếu job_type không được cung cấp
    """
    # Kiểm tra job_type là bắt buộc
    if not job_type:
        # Thử lấy từ command line arguments
        if len(sys.argv) > 1:
            job_type = sys.argv[1]
        else:
            raise ValueError("job_type là bắt buộc. Vui lòng truyền job_type khi gọi hàm hoặc qua command line: python main.py <job_type>")
    
    print(f"Đang chạy job với job_type: {job_type}")
    job = Etl_Runner()
    job.run(job_type=job_type, input_date=input_date)


# Chạy job
if __name__ == "__main__":
    main()

