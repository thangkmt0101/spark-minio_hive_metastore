from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables từ file .env
load_dotenv()

app = Flask(__name__)
CORS(app)

# Cấu hình kết nối database
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def get_db_connection():
    """Tạo kết nối đến database"""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

@app.route('/')
def index():
    """Trang chính"""
    return render_template('index.html')

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """Lấy danh sách tất cả các ETL jobs"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT id, job_type, schema_name, table_name, sql_path, description, 
                   is_active, created_at, updated_at
            FROM etl_job
            ORDER BY id asc
        """)
        
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        
        # Chuyển đổi datetime sang string (optimize với list comprehension)
        for job in jobs:
            job['created_at'] = job['created_at'].isoformat() if job.get('created_at') else None
            job['updated_at'] = job['updated_at'].isoformat() if job.get('updated_at') else None
        
        return jsonify({'success': True, 'data': jobs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs', methods=['POST'])
def create_job():
    """Tạo mới một ETL job"""
    try:
        data = request.json
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Lấy MAX(id) hiện tại
        cur.execute("SELECT MAX(id) FROM etl_job")
        max_id_result = cur.fetchone()[0]
        
        # Tính ID mới = MAX(id) + 1 (hoặc 1 nếu bảng rỗng)
        new_id = (max_id_result + 1) if max_id_result is not None else 1
        
        # Insert với ID cụ thể
        cur.execute("""
            INSERT INTO etl_job (
                id, job_type, schema_name, table_name, sql_path,
                description, is_active, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            new_id,
            data.get('job_type'),
            data.get('schema_name'),
            data.get('table_name'),
            data.get('sql_path'),
            data.get('description'),
            data.get('is_active', True),
            datetime.now(),
            datetime.now()
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Tạo job thành công', 'id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs/<int:job_id>', methods=['PUT'])
def update_job(job_id):
    """Cập nhật một ETL job"""
    try:
        data = request.json
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE etl_job SET
                job_type = %s,
                schema_name = %s,
                table_name = %s,
                sql_path = %s,
                description = %s,
                is_active = %s,
                updated_at = %s
            WHERE id = %s
        """, (
            data.get('job_type'),
            data.get('schema_name'),
            data.get('table_name'),
            data.get('sql_path'),
            data.get('description'),
            data.get('is_active', True),
            datetime.now(),
            job_id
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Cập nhật job thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs/<int:job_id>', methods=['DELETE'])
def delete_job(job_id):
    """Xóa một ETL job"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("DELETE FROM etl_job WHERE id = %s", (job_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Xóa job thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    # Lấy port từ environment variable, mặc định
    port = int(os.getenv('APP_PORT'))
    app.run(debug=True, host='0.0.0.0', port=port)
