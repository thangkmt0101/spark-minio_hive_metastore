#!/usr/bin/env python3
"""
Script độc lập để lấy log từ Spark master
Sử dụng: python test/get_spark_logs.py <app_id> [master_host] [ui_port]
"""

import sys
import requests
import json
from urllib.parse import urlparse

def get_spark_logs(app_id, master_host="10.8.75.82", ui_port=8080, max_lines=100):
    """
    Lấy log từ Spark master thông qua REST API
    
    Args:
        app_id: Application ID (ví dụ: app-20260107115021-0049)
        master_host: Host của Spark master
        ui_port: Port của Spark UI (mặc định 8080)
        max_lines: Số dòng log tối đa
    """
    ui_url = f"http://{master_host}:{ui_port}"
    
    print(f"{'='*60}")
    print(f"[INFO] Lay log tu Spark Master")
    print(f"{'='*60}")
    print(f"Application ID: {app_id}")
    print(f"Spark UI URL: {ui_url}")
    print()
    
    try:
        # 1. Lấy thông tin application
        app_url = f"{ui_url}/api/v1/applications/{app_id}"
        print(f"[SEARCH] Dang lay thong tin application...")
        print(f"   URL: {app_url}")
        
        response = requests.get(app_url, timeout=10)
        if response.status_code == 200:
            app_info = response.json()
            print(f"[OK] Application Name: {app_info.get('name', 'N/A')}")
            print(f"[OK] Application State: {app_info.get('state', 'N/A')}")
            print(f"[OK] Start Time: {app_info.get('startTime', 'N/A')}")
            print(f"[OK] End Time: {app_info.get('endTime', 'N/A')}")
            print(f"[OK] Duration: {app_info.get('duration', 'N/A')} ms")
        else:
            print(f"[ERROR] Khong the lay thong tin application (Status: {response.status_code})")
            return
        
        # 2. Lấy danh sách executors
        print(f"\n[SEARCH] Dang lay thong tin executors...")
        executors_url = f"{ui_url}/api/v1/applications/{app_id}/executors"
        exec_response = requests.get(executors_url, timeout=10)
        
        if exec_response.status_code == 200:
            executors = exec_response.json()
            print(f"[OK] Tim thay {len(executors)} executors")
            print(f"\n{'='*60}")
            print("[STATS] Chi tiet Executors:")
            print(f"{'='*60}")
            
            for i, exec_info in enumerate(executors):
                print(f"\nExecutor {i+1}:")
                print(f"  - ID: {exec_info.get('id', 'N/A')}")
                print(f"  - Host: {exec_info.get('hostPort', 'N/A')}")
                print(f"  - Active: {exec_info.get('isActive', 'N/A')}")
                print(f"  - Cores: {exec_info.get('totalCores', 'N/A')}")
                print(f"  - Max Tasks: {exec_info.get('maxTasks', 'N/A')}")
                print(f"  - Active Tasks: {len(exec_info.get('activeTasks', []))}")
                print(f"  - Completed Tasks: {exec_info.get('completedTasks', 'N/A')}")
                print(f"  - Failed Tasks: {exec_info.get('failedTasks', 'N/A')}")
                
                # Lấy log URL nếu có
                executor_logs = exec_info.get('executorLogs', {})
                if executor_logs:
                    print(f"  - Log URLs:")
                    for log_type, log_url in executor_logs.items():
                        print(f"    {log_type}: {log_url}")
        else:
            print(f"[WARN] Khong the lay thong tin executors (Status: {exec_response.status_code})")
        
        # 3. Lấy danh sách jobs
        print(f"\n[SEARCH] Dang lay thong tin jobs...")
        jobs_url = f"{ui_url}/api/v1/applications/{app_id}/jobs"
        jobs_response = requests.get(jobs_url, timeout=10)
        
        if jobs_response.status_code == 200:
            jobs = jobs_response.json()
            print(f"[OK] Tim thay {len(jobs)} jobs")
            if jobs:
                print(f"\n{'='*60}")
                print("[INFO] Chi tiet Jobs:")
                print(f"{'='*60}")
                for i, job in enumerate(jobs[:5]):  # Chỉ hiển thị 5 job đầu
                    print(f"\nJob {i+1}:")
                    print(f"  - ID: {job.get('jobId', 'N/A')}")
                    print(f"  - Name: {job.get('name', 'N/A')}")
                    print(f"  - Status: {job.get('status', 'N/A')}")
                    print(f"  - Stages: {len(job.get('stageIds', []))}")
        
        # 4. Lấy danh sách stages
        print(f"\n[SEARCH] Dang lay thong tin stages...")
        stages_url = f"{ui_url}/api/v1/applications/{app_id}/stages"
        stages_response = requests.get(stages_url, timeout=10)
        
        if stages_response.status_code == 200:
            stages = stages_response.json()
            print(f"[OK] Tim thay {len(stages)} stages")
            if stages:
                print(f"\n{'='*60}")
                print("[STATS] Chi tiet Stages (5 stages dau):")
                print(f"{'='*60}")
                for i, stage in enumerate(stages[:5]):
                    print(f"\nStage {i+1}:")
                    print(f"  - ID: {stage.get('stageId', 'N/A')}")
                    print(f"  - Name: {stage.get('name', 'N/A')}")
                    print(f"  - Status: {stage.get('status', 'N/A')}")
                    print(f"  - Tasks: {stage.get('numTasks', 'N/A')}")
                    print(f"  - Active Tasks: {stage.get('numActiveTasks', 'N/A')}")
                    print(f"  - Failed Tasks: {stage.get('numFailedTasks', 'N/A')}")
        
        print(f"\n{'='*60}")
        print("[OK] Hoan thanh!")
        print(f"{'='*60}")
        
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] Khong the ket noi den Spark UI tai {ui_url}")
        print(f"   Dam bao Spark UI dang chay va co the truy cap duoc")
    except requests.exceptions.Timeout:
        print(f"[ERROR] Timeout khi ket noi den Spark UI")
    except Exception as e:
        print(f"[ERROR] Loi: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Sử dụng: python test/get_spark_logs.py <app_id> [master_host] [ui_port]")
        print("Ví dụ: python test/get_spark_logs.py app-20260107115021-0049 10.8.75.82 8080")
        sys.exit(1)
    
    app_id = sys.argv[1]
    master_host = sys.argv[2] if len(sys.argv) > 2 else "10.8.75.82"
    ui_port = int(sys.argv[3]) if len(sys.argv) > 3 else 8080
    
    get_spark_logs(app_id, master_host, ui_port)

