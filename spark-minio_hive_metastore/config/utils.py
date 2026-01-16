import os
import shutil
import time
import glob
import subprocess
import traceback
from tempfile import mkdtemp
from pyspark.sql.functions import col, lpad
from config.settings import (
    MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, OUTPUT_PATH
)
from config.time_config import TimeService

class EtlUtils:
  
    @staticmethod
    def export_dataframe_to_csv(df, output_dir, delimiter=',', datetime_csv=None):
        """
        Export Spark DataFrame sang file CSV duy nhất (an toàn cho BULK INSERT)

        Args:
            df: Spark DataFrame cần export
            output_dir (str): Thư mục output trên MinIO
            delimiter (str): Ký tự phân cách (mặc định ',')

        Returns:
            str: Đường dẫn đến file CSV cuối cùng (data.csv)
        """
        try:
            print(f"[EXPORT CSV] Bat dau export DataFrame sang CSV voi delimiter='{delimiter}'")

            # Tạo đường dẫn S3A để lưu vào MinIO
            # Format: s3a://bucket/path/to/file
            temp_csv_dir = f"s3a://{MINIO_BUCKET}/{output_dir}/{datetime_csv}"
            print(f"[EXPORT CSV] Dang write DataFrame vao MinIO: {temp_csv_dir}")

            # Spark write trực tiếp (không coalesce) -> nhiều part files tùy partition
            df.write \
                .mode("overwrite") \
                .option("header", "false") \
                .option("delimiter", delimiter) \
                .option("encoding", "UTF-8") \
                .option("emptyValue", "") \
                .option("nullValue", "") \
                .csv(temp_csv_dir)
        except Exception as e:
            error_msg = f"Loi khi export DataFrame sang CSV: {str(e)}"
            print(f"[EXPORT CSV ERROR] {error_msg}")
            raise Exception(error_msg)

    @staticmethod
    def export_dataframe_to_parquet(df, table_name):
        """
        Export Spark DataFrame sang Parquet và lưu vào Iceberg table (mart layer)
        Luôn dùng overwrite mode (createOrReplace)
        
        Args:
            df: Spark DataFrame cần export
            table_name (str): Tên table trong Iceberg catalog (ví dụ: "mart.table_name")
        
        Returns:
            None
        """
        try:
            print(f"[EXPORT PARQUET] Bắt đầu export DataFrame sang Iceberg table: {table_name}")
            
            # Kiểm tra DataFrame có các cột partition không
            required_partition_cols = ["year", "month", "day"]
            df_columns = [col_name.lower() for col_name in df.columns]
            missing_cols = [col for col in required_partition_cols if col not in df_columns]
            
            if missing_cols:
                error_msg = f"DataFrame thiếu các cột partition bắt buộc: {missing_cols}"
                print(f"[EXPORT PARQUET ERROR] {error_msg}")
                print(f"[EXPORT PARQUET] Các cột hiện có: {df_columns}")
                raise ValueError(error_msg)
            
            # Tạo full table name với catalog prefix
            full_table_name = f"ice.{table_name}" if not table_name.startswith("ice.") else table_name
            print(table_name)
            print(f"[EXPORT PARQUET] Full table name: {full_table_name}")
            
            # Namespace cố định = "mart"
            namespace = "mart"
            
            # Tạo database/namespace nếu chưa tồn tại (qua Iceberg catalog)
            from config.spark_session import SparkSessionBuilder
            spark = SparkSessionBuilder.get_spark()
            
            try:
                # Tạo database qua Iceberg catalog
                spark.sql(f"CREATE DATABASE IF NOT EXISTS ice.{namespace}")
                print(f"[EXPORT PARQUET] Đã tạo/kiểm tra database: {namespace}")
            except Exception as db_error:
                print(f"[EXPORT PARQUET WARNING] Không thể tạo database {namespace}: {db_error}")
                # Tiếp tục thử tạo table, có thể database đã tồn tại
            
            # Format các cột partition: đảm bảo year, month, day là STRING và có leading zero cho month, day
            df_formatted = df.withColumn("year", col("year").cast("string")) \
                .withColumn("month", lpad(col("month").cast("string"), 2, "0")) \
                .withColumn("day", lpad(col("day").cast("string"), 2, "0"))
            
            print(f"[EXPORT PARQUET] Đã format các cột partition: year (string), month (string với leading zero), day (string với leading zero)")
            
            # Write DataFrame vào Iceberg table với overwrite mode
            df_formatted.writeTo(full_table_name) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .partitionedBy(
                    col("year"),
                    col("month"),
                    col("day")
                ) \
                .overwritePartitions()
            
            print(f"[EXPORT PARQUET] Đã tạo/thay thế table: {full_table_name}")
            print(f"[EXPORT PARQUET] Export thành công!")
            
        except Exception as e:
            error_msg = f"Lỗi khi export DataFrame sang Iceberg table {table_name}: {str(e)}"
            print(f"[EXPORT PARQUET ERROR] {error_msg}")
            traceback.print_exc()
            raise Exception(error_msg)

    @staticmethod
    def merge_and_rename_files_in_minio(source_path=None, table_name=None, input_date=None):
        """
        Gộp các part files từ Spark CSV export và đổi tên thành {table_name}.csv trên MinIO
        
        Args:
            source_path (str): Đường dẫn source trên MinIO (relative to bucket). 
                             Nếu None, sẽ dùng OUTPUT_PATH từ settings
            table_name (str): Tên bảng để đặt tên file cuối cùng: {table_name}.csv
            input_date (str): Ngày 'yyyy-mm-dd' để tạo đường dẫn. Nếu None thì dùng hôm qua
        
        Returns:
            bool: True nếu merge và rename thành công, False nếu có lỗi
        """
        try:
            print(f"=== Starting Merge and Rename Files in MinIO ===")
            print(f"Table name: {table_name}")
            
            # Lấy thông tin ngày nếu có
            if input_date:
                info = TimeService.get_target_date(input_date)
                year = int(info['year'])
                month = int(info['month'])
                day = int(info['day'])
                date_str = f"{year}{month:02d}{day:02d}"
            else:
                date_str = None
            
            # Xác định đường dẫn source trên MinIO
            if source_path is None:
                source_path = OUTPUT_PATH
            
            # Thêm table_name và date vào đường dẫn
            if table_name:
                source_path = f"{source_path}/{table_name}"
            
            if date_str:
                source_path = f"{source_path}/{date_str}"
            
            minio_source = f"{MINIO_BUCKET}/{source_path}"
            minio_endpoint = MINIO_ENDPOINT
            minio_access_key = MINIO_ACCESS_KEY
            minio_secret_key = MINIO_SECRET_KEY
            
            print(f"MinIO Source Path: {minio_source}")
            print(f"[DEBUG] source_path parameter: {source_path}")
            print(f"[DEBUG] table_name: {table_name}")
            print(f"[DEBUG] input_date: {input_date}")
            print(f"[DEBUG] date_str: {date_str}")
            print(f"[DEBUG] Final source_path: {source_path}")
            print(f"[DEBUG] Final minio_source: {minio_source}")
            
            # Tạo alias cho MinIO
            print("Setting up MinIO alias...")
            alias_cmd = [
                "mc", "alias", "set", "myminio",
                minio_endpoint,
                minio_access_key,
                minio_secret_key
            ]
            
            result = subprocess.run(
                alias_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"[ERROR] Failed to set MinIO alias: {result.stderr}")
                return False
            
            # List các part files trong thư mục
            print(f"Listing files in: myminio/{minio_source}")
            list_cmd = ["mc", "ls", f"myminio/{minio_source}/"]
            
            result = subprocess.run(
                list_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"[ERROR] Failed to list files: {result.stderr}")
                print(f"[DEBUG] Command: {' '.join(list_cmd)}")
                return False
            
            # Debug: In output để kiểm tra
            print(f"[DEBUG] mc ls output:\n{result.stdout}")
            print(f"[DEBUG] Output lines count: {len(result.stdout.strip().split(chr(10)))}")
            
            # Lọc các part files (part-*.csv)
            output_lines = result.stdout.strip().split('\n')
            part_files = []
            all_items = []
            
            for line in output_lines:
                if not line.strip():
                    continue
                
                # Extract item name từ output của mc ls
                parts = line.strip().split()
                if len(parts) > 0:
                    item_name = parts[-1].rstrip('/')  # Lấy phần cuối và bỏ dấu /
                    all_items.append(item_name)
                    
                    # Kiểm tra xem có phải là part file không
                    if 'part-' in item_name and '.csv' in item_name and not item_name.endswith('.crc'):
                        part_files.append(item_name)
                        print(f"[DEBUG] Found part file: {item_name}")
            
            print(f"[DEBUG] Tất cả items trong thư mục: {all_items}")
            
            # Nếu không tìm thấy part files, có thể part files nằm trong thư mục con (date_str)
            # Kiểm tra xem có thư mục con nào không
            if not part_files and date_str:
                # Có thể part files nằm trong thư mục con với tên là date_str
                # Nhưng đường dẫn đã có date_str rồi, nên không cần check thêm
                pass
            elif not part_files:
                # Nếu không có date_str và không tìm thấy part files, có thể cần list sâu hơn
                # Kiểm tra xem có thư mục con nào giống format date không
                date_folders = []
                for item in all_items:
                    # Kiểm tra xem có phải là thư mục ngày không (8 chữ số)
                    if len(item) == 8 and item.isdigit():
                        date_folders.append(item)
                
                if date_folders:
                    print(f"[DEBUG] Tìm thấy {len(date_folders)} thư mục ngày: {sorted(date_folders)}")
                    print(f"[DEBUG] Có thể part files nằm trong các thư mục này")
                    # Thử list trong thư mục ngày mới nhất
                    if date_folders:
                        latest_date = sorted(date_folders)[-1]
                        print(f"[DEBUG] Thử list trong thư mục: {latest_date}")
                        sub_list_cmd = ["mc", "ls", f"myminio/{minio_source}/{latest_date}/"]
                        sub_result = subprocess.run(
                            sub_list_cmd,
                            capture_output=True,
                            text=True,
                            timeout=30
                        )
                        if sub_result.returncode == 0:
                            print(f"[DEBUG] mc ls trong {latest_date}:\n{sub_result.stdout}")
                            # Parse lại từ output này
                            sub_lines = sub_result.stdout.strip().split('\n')
                            for line in sub_lines:
                                if not line.strip():
                                    continue
                                parts = line.strip().split()
                                if len(parts) > 0:
                                    item_name = parts[-1].rstrip('/')
                                    if 'part-' in item_name and '.csv' in item_name and not item_name.endswith('.crc'):
                                        part_files.append(item_name)
                                        print(f"[DEBUG] Found part file in subfolder: {item_name}")
                            # Cập nhật đường dẫn để bao gồm date folder
                            if part_files:
                                minio_source = f"{minio_source}/{latest_date}"
                                print(f"[DEBUG] Updated minio_source to: {minio_source}")
            
            if not part_files:
                print(f"[WARNING] Không tìm thấy part files trong {minio_source}")
                print(f"[DEBUG] Tất cả items: {all_items}")
                return False
            
            print(f"Tìm thấy {len(part_files)} part files: {part_files}")
            
            # Tạo file tạm để gộp các part files
            temp_dir = mkdtemp()
            temp_merged_file = os.path.join(temp_dir, f"{table_name}_merged.csv")
            final_filename = f"{table_name}.csv"
            
            print(f"Đang gộp {len(part_files)} files thành {final_filename}...")
            
            # Download và gộp các part files
            with open(temp_merged_file, 'wb') as merged_file:
                for part_file in sorted(part_files):  # Sắp xếp để đảm bảo thứ tự
                    part_path = f"myminio/{minio_source}/{part_file}"
                    print(f"  Đang xử lý: {part_file}")
                    
                    # Download file từ MinIO
                    cat_cmd = ["mc", "cat", part_path]
                    cat_result = subprocess.run(
                        cat_cmd,
                        capture_output=True,
                        timeout=60
                    )
                    
                    if cat_result.returncode != 0:
                        print(f"[WARNING] Không thể đọc file {part_file}: {cat_result.stderr}")
                        continue
                    
                    # Ghi vào file gộp
                    merged_file.write(cat_result.stdout)
            
            # Upload file đã gộp lên MinIO với tên mới
            print(f"Đang upload file đã gộp lên MinIO: {final_filename}")
            cp_cmd = [
                "mc", "cp",
                temp_merged_file,
                f"myminio/{minio_source}/{final_filename}"
            ]
            
            result = subprocess.run(
                cp_cmd,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode != 0:
                print(f"[ERROR] Failed to upload merged file: {result.stderr}")
                return False
            
            print(f"Đã upload file {final_filename} thành công")
            
            # Xóa các part files cũ (optional - có thể giữ lại để backup)
            print(f"Đang xóa các part files cũ...")
            for part_file in part_files:
                rm_cmd = ["mc", "rm", f"myminio/{minio_source}/{part_file}"]
                rm_result = subprocess.run(
                    rm_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if rm_result.returncode == 0:
                    print(f"  Đã xóa: {part_file}")
                else:
                    print(f"  [WARNING] Không thể xóa {part_file}: {rm_result.stderr}")
            
            # Cleanup temp directory
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
            
            print(f"=== Merge and Rename Completed: {final_filename} ===")
            return True
            
        except subprocess.TimeoutExpired:
            print(f"[ERROR] Merge and rename timeout")
            return False
        except Exception as e:
            print(f"[ERROR] Exception during merge and rename: {str(e)}")
            traceback.print_exc()
            return False

    @staticmethod
    def sync_from_minio(source_path=None, dest_path="/data/etl_data_export", input_date=None, table_name=None):
        """
        Sync dữ liệu từ MinIO về local filesystem sử dụng mc mirror
        
        Args:
            source_path (str): Đường dẫn source trên MinIO (relative to bucket). 
                             Nếu None, sẽ dùng OUTPUT_PATH từ settings
            dest_path (str): Đường dẫn destination local. Mặc định: /data/etl_data_export
            input_date (str): Ngày 'yyyy-mm-dd' để tạo đường dẫn. Nếu None thì dùng hôm qua
            table_name (str): Tên bảng để tạo đường dẫn đúng format: /data/etl_data_export/{table_name}/{input_date}
        
        Returns:
            bool: True nếu sync thành công, False nếu có lỗi
        """
        try:
            print("=== Starting MinIO Sync ===")
            
            # Lấy thông tin ngày nếu có
            if input_date:
                info = TimeService.get_target_date(input_date)
                year = int(info['year'])
                month = int(info['month'])
                day = int(info['day'])
                date_str = f"{year}{month:02d}{day:02d}"
            else:
                date_str = None
            
            # Xác định đường dẫn source trên MinIO
            if source_path is None:
                source_path = OUTPUT_PATH
            
            # Thêm table_name và date vào đường dẫn nếu có
            # Đường dẫn đúng: /data/etl_data_export/{table_name}/{input_date}
            if table_name:
                source_path = f"{source_path}/{table_name}"
                dest_path = f"{dest_path}/{table_name}"
            
            if date_str:
                source_path = f"{source_path}/{date_str}"
                dest_path = f"{dest_path}/{date_str}"
            
            minio_source = f"{MINIO_BUCKET}/{source_path}"
            minio_endpoint = MINIO_ENDPOINT
            minio_access_key = MINIO_ACCESS_KEY
            minio_secret_key = MINIO_SECRET_KEY
            
            print(f"MinIO Endpoint: {minio_endpoint}")
            print(f"Source: {minio_source}")
            print(f"Destination: {dest_path}")
            
            # Với --overwrite và --remove, mc mirror sẽ tự động:
            # - Ghi đè files đã tồn tại (--overwrite)
            # - Xóa files không còn trên source (--remove)
            # Vậy không cần xóa thư mục trước, nhưng vẫn xóa để đảm bảo đồng bộ hoàn toàn và tránh lỗi
            # Nếu muốn tối ưu, có thể bỏ bước này và chỉ dùng --overwrite + --remove
            # if os.path.exists(dest_path):
            #     print(f"[SYNC] Cleaning up existing destination directory: {dest_path}")
            #     try:
            #         shutil.rmtree(dest_path)
            #         print(f"[SYNC] Successfully removed old directory")
            #     except Exception as cleanup_error:
            #         print(f"[WARNING] Failed to remove old directory: {cleanup_error}")
            #         print("[WARNING] Continuing with sync anyway (--overwrite + --remove will handle it)...")
            
            # Đảm bảo thư mục destination tồn tại
            os.makedirs(dest_path, exist_ok=True)
            
            # Tạo alias cho MinIO
            print("Setting up MinIO alias...")
            alias_cmd = [
                "mc", "alias", "set", "myminio",
                minio_endpoint,
                minio_access_key,
                minio_secret_key
            ]
            
            result = subprocess.run(
                alias_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"[ERROR] Failed to set MinIO alias: {result.stderr}")
                return False
            
            print("MinIO alias set successfully")
            
            # Kiểm tra kết nối MinIO
            print("Checking MinIO connection...")
            info_cmd = ["mc", "admin", "info", "myminio"]
            result = subprocess.run(
                info_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"[WARNING] Failed to get MinIO info: {result.stderr}")
                print("Continuing with sync anyway...")
            else:
                print("MinIO connection verified")
            
            # Mirror dữ liệu từ MinIO về local
            # QUAN TRỌNG: Thêm các flags để tránh treo
            # --overwrite: Ghi đè files đã tồn tại
            # --quiet: Giảm output để tránh buffer overflow
            # LƯU Ý: Không dùng --remove để giữ lại files cũ trên destination (không xóa files không còn trên source)
            print(f"Syncing data from MinIO to local...")
            print(f"Source: myminio/{minio_source}")
            print(f"Destination: {dest_path}")
            
            mirror_cmd = [
                "mc", "mirror",
                "--overwrite",  # Ghi đè files đã tồn tại
                "--quiet",      # Giảm output để tránh buffer overflow
                f"myminio/{minio_source}",
                dest_path
            ]
            
            # Giảm timeout xuống 120 giây (2 phút) để phát hiện treo sớm hơn
            # Nếu file lớn, có thể tăng timeout hoặc chia nhỏ
            sync_timeout = 120  # 2 phút
            
            print(f"[SYNC] Bắt đầu sync với timeout {sync_timeout} giây...")
            start_time = time.time()
            
            try:
                result = subprocess.run(
                    mirror_cmd,
                    capture_output=True,
                    text=True,
                    timeout=sync_timeout
                )
                
                elapsed_time = time.time() - start_time
                print(f"[SYNC] Sync hoàn thành sau {elapsed_time:.2f} giây")
                
                if result.returncode != 0:
                    print(f"[ERROR] Failed to sync from MinIO: {result.stderr}")
                    print(f"Command output: {result.stdout}")
                    return False

                print(f"Sync completed successfully! dest_path: {dest_path}")
                if input_date:
                    print(f"Sync completed successfully! input_date: {input_date}")
                if table_name:
                    print(f"Sync completed successfully! table_name: {table_name}")
                if result.stdout:
                    print(f"Output: {result.stdout}")
                print("=== MinIO Sync Completed ===")
                return True
                
            except subprocess.TimeoutExpired:
                elapsed_time = time.time() - start_time
                print(f"[ERROR] Sync timeout sau {elapsed_time:.2f} giây (timeout={sync_timeout}s)")
                print(f"[ERROR] Có thể do network MinIO chậm hoặc file quá lớn")
                print(f"[ERROR] Source: myminio/{minio_source}")
                print(f"[ERROR] Destination: {dest_path}")
                print(f"[ERROR] Khuyến nghị: Kiểm tra kết nối MinIO hoặc tăng timeout nếu file lớn")
                return False
            
        except subprocess.TimeoutExpired:
            # Fallback nếu timeout ở level ngoài
            print(f"[ERROR] Sync timeout (timeout không xác định)")
            return False
        except Exception as e:
            print(f"[ERROR] Exception during MinIO sync: {str(e)}")
            traceback.print_exc()
            return False

    @staticmethod
    def delete_old_data_from_minio(source_path=None, table_name=None, input_date=None, days_before=3):
        """
        Xóa dữ liệu cũ từ MinIO (mặc định 3 ngày trước và tất cả các ngày trước đó)
        
        Args:
            source_path (str): Đường dẫn source trên MinIO (relative to bucket). 
                             Nếu None, sẽ dùng OUTPUT_PATH từ settings
            table_name (str): Tên bảng để xác định đường dẫn cần xóa
            input_date (str): Ngày 'yyyy-mm-dd' để tính ngày cần xóa. Nếu None thì dùng hôm qua
            days_before (int): Số ngày trước để tính cutoff (mặc định: 3). Sẽ xóa từ ngày cutoff trở về trước
        
        Returns:
            bool: True nếu xóa thành công, False nếu có lỗi
        """
        try:
            from datetime import datetime, timedelta
            import re
            
            print(f"=== Starting Delete Old Data from MinIO ===")
            print(f"Table name: {table_name}")
            print(f"Days before: {days_before}")
            
            # Tính toán ngày cần xóa (3 ngày trước từ input_date)
            if input_date:
                base_date = datetime.strptime(input_date, "%Y-%m-%d")
            else:
                # Mặc định = hôm qua
                base_date = datetime.now() - timedelta(days=1)
            
            # Trừ đi số ngày cần xóa
            cutoff_date = base_date - timedelta(days=days_before)
            cutoff_date_str = cutoff_date.strftime("%Y%m%d")
            
            print(f"Đang xóa dữ liệu từ ngày {cutoff_date.strftime('%Y-%m-%d')} ({cutoff_date_str}) trở về trước")
            
            # Xác định đường dẫn source trên MinIO
            if source_path is None:
                source_path = OUTPUT_PATH
            
            # Thêm table_name vào đường dẫn (không thêm date vì sẽ list tất cả các ngày)
            if table_name:
                table_path = f"{source_path}/{table_name}"
            else:
                table_path = source_path
            
            minio_table_path = f"{MINIO_BUCKET}/{table_path}"
            minio_endpoint = MINIO_ENDPOINT
            minio_access_key = MINIO_ACCESS_KEY
            minio_secret_key = MINIO_SECRET_KEY
            
            print(f"MinIO Table Path: {minio_table_path}")
            
            # Tạo alias cho MinIO
            print("Setting up MinIO alias...")
            alias_cmd = [
                "mc", "alias", "set", "myminio",
                minio_endpoint,
                minio_access_key,
                minio_secret_key
            ]
            
            result = subprocess.run(
                alias_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                print(f"[ERROR] Failed to set MinIO alias: {result.stderr}")
                return False
            
            # List tất cả các thư mục ngày trong thư mục table
            print(f"Đang list các thư mục ngày trong: myminio/{minio_table_path}")
            list_cmd = ["mc", "ls", f"myminio/{minio_table_path}/"]
            
            list_result = subprocess.run(
                list_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if list_result.returncode != 0:
                print(f"[INFO] Không tìm thấy thư mục hoặc không có dữ liệu: {minio_table_path}")
                print(f"[INFO] Không cần xóa")
                return True
            
            # Parse danh sách các thư mục ngày
            output_lines = list_result.stdout.strip().split('\n')
            date_folders = []
            
            # Pattern để match thư mục ngày: yyyymmdd (8 chữ số)
            date_pattern = re.compile(r'(\d{8})')
            
            for line in output_lines:
                if not line.strip():
                    continue
                # Extract tên thư mục từ output của mc ls (format: [DATE] SIZE NAME)
                parts = line.split()
                if len(parts) > 0:
                    folder_name = parts[-1].rstrip('/')  # Lấy phần cuối và bỏ dấu /
                    # Kiểm tra xem có phải là thư mục ngày không (8 chữ số)
                    match = date_pattern.match(folder_name)
                    if match:
                        date_str = match.group(1)
                        date_folders.append(date_str)
            
            if not date_folders:
                print(f"[INFO] Không tìm thấy thư mục ngày nào trong: {minio_table_path}")
                return True
            
            print(f"Tìm thấy {len(date_folders)} thư mục ngày: {sorted(date_folders)}")
            
            # Lọc các thư mục cần xóa (ngày <= cutoff_date) - xóa từ ngày cutoff trở về trước
            folders_to_delete = []
            for date_str in date_folders:
                if date_str <= cutoff_date_str:
                    folders_to_delete.append(date_str)
            
            if not folders_to_delete:
                print(f"[INFO] Không có thư mục nào cần xóa (tất cả đều mới hơn {cutoff_date.strftime('%Y-%m-%d')})")
                return True
            
            print(f"Sẽ xóa {len(folders_to_delete)} thư mục: {sorted(folders_to_delete)}")
            
            # Xóa từng thư mục
            deleted_count = 0
            failed_count = 0
            
            for date_str in sorted(folders_to_delete):
                folder_path = f"{minio_table_path}/{date_str}"
                print(f"Đang xóa: {folder_path}")
                
                rm_cmd = [
                    "mc", "rm",
                    "--recursive",
                    "--force",
                    f"myminio/{folder_path}"
                ]
                
                result = subprocess.run(
                    rm_cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 phút timeout cho mỗi thư mục
                )
                
                if result.returncode == 0:
                    print(f"  ✓ Đã xóa thành công: {date_str}")
                    deleted_count += 1
                else:
                    print(f"  ✗ Lỗi khi xóa {date_str}: {result.stderr}")
                    failed_count += 1
            
            print(f"\n=== Delete Old Data Completed ===")
            print(f"Đã xóa thành công: {deleted_count} thư mục")
            if failed_count > 0:
                print(f"Lỗi: {failed_count} thư mục")
            
            return deleted_count > 0
            
        except subprocess.TimeoutExpired:
            print(f"[ERROR] Delete timeout after 5 minutes")
            return False
        except Exception as e:
            print(f"[ERROR] Exception during delete old data: {str(e)}")
            traceback.print_exc()
            return False

   # --- Format helpers ---
    @staticmethod
    def format_day(year, month, day):
        return f"{year}{str(month).zfill(2)}{str(day).zfill(2)}00"

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
                           

    # # --- Main delete function ---.
    # @staticmethod
    # def delete_by_config(conn_str, table_name, delete_column, delete_condition,
    #                      year=None, month=None, day=None):

    #     if delete_condition == "day":
    #         value = EtlUtils.format_day(year, month, day)  # yyyymmdd00
    #         where = f"{delete_column} = ?"
    #         params = [value]

    #     elif delete_condition == "month":
    #         value = EtlUtils.format_month_id(year, month)  # yyyymm
    #         where = f"{delete_column} = ?"
    #         params = [value]

    #     elif delete_condition == "year":
    #         value = EtlUtils.format_year_id(year) # yyyy
    #         where = f"{delete_column} = ?"
    #         params = [value]

    #     elif delete_condition == "time":
    #         value_start = EtlUtils.format_time_start(year, month, day)  # yyyymmdd00
    #         value_end = EtlUtils.format_time_end(year, month, day)  # yyyymmdd23
    #         where = f"{delete_column} BETWEEN ? AND ?"
    #         params = [value_start, value_end]

    #     elif delete_condition == "quarter":
    #         # format: YYYYQn (n=1..4) dựa trên month được truyền vào
    #         if month is None or year is None:
    #             raise ValueError("delete_condition=quarter yêu cầu year và month")
    #         quarter_num = (int(month) - 1) // 3 + 1
    #         value = f"{year}Q{quarter_num}"
    #         where = f"{delete_column} = ?"
    #         params = [value]

    #     elif delete_condition in (None, "", "none"):
    #         # Không xóa nếu không cấu hình
    #         return

    #     else:
    #         raise ValueError("delete_condition phải là: day | month | year | time | quarter | none")

    #     sql = f"DELETE FROM {table_name} WHERE {where}"
        
    #     # Luôn tạo connection riêng để xóa và đóng sau khi xóa xong
    #     # Nếu truyền vào connection object, cần lấy connection string từ nó
    #     # Nếu là connection string, sử dụng trực tiếp
    #     connection_string = None
        
    #     if conn_str:
    #         # Kiểm tra xem có phải là connection object không
    #         if hasattr(conn_str, 'cursor'):
    #             # Đây là connection object, không thể lấy connection string từ nó
    #             # Yêu cầu truyền connection string thay vì connection object
    #             raise ValueError("delete_by_config() yêu cầu connection string, không nhận connection object. Vui lòng truyền conn_str thay vì conn.")
    #         else:
    #             # Đây là connection string, sử dụng trực tiếp
    #             connection_string = conn_str
        
    #     if not connection_string:
    #         raise ValueError("conn_str không được để trống")
        
    #     # Tạo connection mới riêng để xóa (với timeout để tránh treo)
    #     conn = None
    #     try:
    #         with pyodbc.connect(connection_string, timeout=30) as conn:
    #             cur = conn.cursor()
    #             cur.execute(sql, params)
    #             rows_deleted = cur.rowcount
    #             conn.commit()
    #             print(f"[ETL SUCCESS] Deleted {rows_deleted} rows from {table_name} where {where} params={params}")
    #     except pyodbc.OperationalError as op_error:
    #         error_msg = f"Lỗi kết nối hoặc timeout khi DELETE từ {table_name}: {op_error}"
    #         print(f"[ETL ERROR] {error_msg}")
    #         raise Exception(error_msg)  # Raise exception để caller biết delete thất bại
    #     except Exception as e:
    #         error_msg = f"Lỗi khi DELETE từ {table_name} ({delete_column}, {delete_condition}): {e}"
    #         print(f"[ETL ERROR] {error_msg}")
    #         raise Exception(error_msg)  # Raise exception để caller biết delete thất bại
