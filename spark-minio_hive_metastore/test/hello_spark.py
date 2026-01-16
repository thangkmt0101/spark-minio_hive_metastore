#!/usr/bin/env python3
"""
File Spark đơn giản để print "hello" và submit vào Spark master
"""

from pyspark.sql import SparkSession
import traceback
import requests
import json
import os
from urllib.parse import urlparse

def get_spark_master_logs(spark_context, max_lines=100):
    """
    Lấy log từ Spark master thông qua REST API
    
    Args:
        spark_context: SparkContext object
        max_lines: Số dòng log tối đa cần lấy
    """
    try:
        # Lấy thông tin application
        app_id = spark_context.applicationId
        master_url = spark_context.master
        
        print(f"\n{'='*50}")
        print("[INFO] Lay log tu Spark Master")
        print(f"{'='*50}")
        print(f"Application ID: {app_id}")
        print(f"Master URL: {master_url}")
        
        # Parse master URL để lấy host và port
        if master_url.startswith("spark://"):
            # Format: spark://host:port
            parts = master_url.replace("spark://", "").split(":")
            if len(parts) == 2:
                master_host = parts[0]
                # Spark UI thường chạy trên port 8080
                ui_port = 8080
                ui_url = f"http://{master_host}:{ui_port}"
                
                print(f"Spark UI URL: {ui_url}")
                
                # Thử lấy thông tin application từ REST API
                try:
                    # API endpoint để lấy thông tin application
                    app_url = f"{ui_url}/api/v1/applications/{app_id}"
                    print(f"\nĐang lấy thông tin từ: {app_url}")
                    
                    response = requests.get(app_url, timeout=5)
                    if response.status_code == 200:
                        app_info = response.json()
                        print(f"[OK] Application Name: {app_info.get('name', 'N/A')}")
                        print(f"[OK] Application State: {app_info.get('state', 'N/A')}")
                        print(f"[OK] Start Time: {app_info.get('startTime', 'N/A')}")
                        
                        # Lấy danh sách executors
                        executors_url = f"{ui_url}/api/v1/applications/{app_id}/executors"
                        exec_response = requests.get(executors_url, timeout=5)
                        if exec_response.status_code == 200:
                            executors = exec_response.json()
                            print(f"\n[STATS] Executors ({len(executors)}):")
                            for i, exec_info in enumerate(executors[:5]):  # Chỉ hiển thị 5 executor đầu
                                print(f"  Executor {i+1}:")
                                print(f"    - ID: {exec_info.get('id', 'N/A')}")
                                print(f"    - Host: {exec_info.get('hostPort', 'N/A')}")
                                print(f"    - State: {exec_info.get('isActive', 'N/A')}")
                                print(f"    - Cores: {exec_info.get('totalCores', 'N/A')}")
                    else:
                        print(f"[WARN] Khong the ket noi den Spark UI (Status: {response.status_code})")
                        print(f"   URL: {ui_url}")
                        
                except requests.exceptions.RequestException as e:
                    print(f"[WARN] Khong the ket noi den Spark UI: {str(e)}")
                    print(f"   Dam bao Spark UI dang chay tren {ui_url}")
                except Exception as e:
                    print(f"[WARN] Loi khi lay log: {str(e)}")
        else:
            print(f"[WARN] Master URL format khong duoc ho tro: {master_url}")
            
    except Exception as e:
        print(f"[WARN] Loi khi lay log tu Spark master: {str(e)}")
        traceback.print_exc()

def get_executor_logs_from_context(spark_context):
    """
    Lấy thông tin executor từ SparkContext
    Lưu ý: StatusTracker API có thể khác nhau giữa các phiên bản Spark
    """
    try:
        print(f"\n{'='*50}")
        print("[STATS] Thong tin Executors tu SparkContext")
        print(f"{'='*50}")
        
        # Lấy thông tin từ SparkContext
        status_tracker = spark_context.statusTracker()
        
        if status_tracker:
            # Thử các cách khác nhau để lấy thông tin executor tùy theo phiên bản Spark
            try:
                # Cách 1: Thử getExecutorInfos() (Spark 2.x+)
                if hasattr(status_tracker, 'getExecutorInfos'):
                    executor_infos = status_tracker.getExecutorInfos()
                    if executor_infos:
                        print(f"Tong so executors: {len(executor_infos)}")
                        for i, exec_info in enumerate(executor_infos):
                            print(f"\nExecutor {i+1}:")
                            print(f"  - ID: {exec_info.executorId}")
                            print(f"  - Host: {exec_info.executorHost}")
                            print(f"  - Cores: {exec_info.totalCores}")
                            print(f"  - Max Tasks: {exec_info.maxTasks}")
                            print(f"  - Active Tasks: {len(exec_info.activeTasks)}")
                    else:
                        print("[WARN] Khong co executor nao duoc tim thay")
                else:
                    # Cách 2: In thông tin cơ bản từ SparkContext
                    print(f"Application ID: {spark_context.applicationId}")
                    print(f"Master: {spark_context.master}")
                    print(f"[INFO] Thong tin chi tiet executor se duoc lay qua REST API")
            except AttributeError:
                # Nếu không có method này, chỉ in thông tin cơ bản
                print(f"Application ID: {spark_context.applicationId}")
                print(f"Master: {spark_context.master}")
                print(f"[INFO] Thong tin chi tiet executor se duoc lay qua REST API")
        else:
            print("[WARN] Khong the lay StatusTracker")
            
    except Exception as e:
        print(f"[WARN] Loi khi lay thong tin executor: {str(e)}")
        # Không in traceback để tránh làm rối output
        pass

def main():
    """Hàm chính để chạy Spark job"""
    spark = None
    try:
        # Tạo SparkSession với config tối thiểu
        spark = SparkSession.builder \
            .appName("Hello Spark Job") \
            .getOrCreate()
        
        # Print hello
        print("=" * 50)
        print("HELLO FROM SPARK!")
        print("=" * 50)
        
        # In thông tin Spark context
        print(f"Spark Version: {spark.version}")
        print(f"App Name: {spark.sparkContext.appName}")
        print(f"Master: {spark.sparkContext.master}")
        
        # Lấy log từ Spark master
        get_spark_master_logs(spark.sparkContext)
        
        # Lấy thông tin executor từ SparkContext
        get_executor_logs_from_context(spark.sparkContext)
        
        # Test với RDD trước (đơn giản hơn DataFrame)
        print("\nTesting with RDD...")
        rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
        result = rdd.collect()
        print(f"RDD result: {result}")
        
        # Tạo một DataFrame đơn giản
        print("\nCreating DataFrame...")
        data = [("Hello", 1), ("World", 2)]
        df = spark.createDataFrame(data, ["message", "id"])
        print("DataFrame created successfully!")
        
        # Collect thay vì show để tránh lỗi
        print("\nCollecting DataFrame data...")
        rows = df.collect()
        print("DataFrame rows:")
        for row in rows:
            print(f"  {row}")
        
        print("\n" + "=" * 50)
        print("Job completed successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n[ERROR] Error occurred: {str(e)}")
        print("\nFull traceback:")
        traceback.print_exc()
        raise
    finally:
        # Đóng SparkSession
        if spark:
            try:
                spark.stop()
                print("SparkSession stopped.")
            except:
                pass

if __name__ == "__main__":
    main()

