from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os
from dotenv import load_dotenv
import pandas as pd
from werkzeug.utils import secure_filename

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


@app.route('/api/import/preview', methods=['POST'])
def preview_import():
    """Preview dữ liệu từ file Excel trước khi import"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Không có file được upload'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Chưa chọn file'}), 400
        
        # Check file extension
        if not file.filename.lower().endswith(('.xlsx', '.xls')):
            return jsonify({'success': False, 'error': 'File phải có định dạng .xlsx hoặc .xls'}), 400
        
        # Read Excel file
        df = pd.read_excel(file)
        
        # Validate columns
        required_columns = ['job_type', 'schema_name', 'table_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return jsonify({
                'success': False,
                'error': f'File thiếu các cột bắt buộc: {", ".join(missing_columns)}'
            }), 400
        
        # Convert DataFrame to list of dicts
        records = []
        errors = []
        
        for idx, row in df.iterrows():
            # Parse is_active
            is_active_value = row.get('is_active', '')
            if pd.notna(is_active_value):
                is_active_str = str(is_active_value).strip().upper()
                is_active = is_active_str in ['TRUE', '1', 'YES', 'Y']
            else:
                is_active_str = ''
                is_active = None
            
            record = {
                'row_number': idx + 2,  # Excel row number (1-based + header)
                'job_type': str(row.get('job_type', '')).strip(),
                'schema_name': str(row.get('schema_name', '')).strip(),
                'table_name': str(row.get('table_name', '')).strip(),
                'sql_path': str(row.get('sql_path', '')).strip() if pd.notna(row.get('sql_path')) else '',
                'description': str(row.get('description', '')).strip() if pd.notna(row.get('description')) else '',
                'is_active': is_active,
                'valid': True,
                'errors': []
            }
            
            # Validation
            if not record['job_type'] or record['job_type'] not in ['1', '2', '3', '4', '5']:
                record['valid'] = False
                record['errors'].append('Loại job không hợp lệ (phải là 1-5)')
            
            if not record['schema_name']:
                record['valid'] = False
                record['errors'].append('Schema name không được rỗng')
            
            if not record['table_name']:
                record['valid'] = False
                record['errors'].append('Table name không được rỗng')
            
            if not record['sql_path']:
                record['valid'] = False
                record['errors'].append('SQL Path không được rỗng')
            
            if record['is_active'] is None:
                record['valid'] = False
                record['errors'].append('is_active không hợp lệ (phải là TRUE/FALSE)')
            
            if not record['valid']:
                errors.append(f"Dòng {record['row_number']}: {', '.join(record['errors'])}")
            
            records.append(record)
        
        valid_count = sum(1 for r in records if r['valid'])
        
        return jsonify({
            'success': True,
            'data': records,
            'summary': {
                'total': len(records),
                'valid': valid_count,
                'invalid': len(records) - valid_count,
                'errors': errors
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Lỗi đọc file: {str(e)}'}), 500

@app.route('/api/import/confirm', methods=['POST'])
def confirm_import():
    """Import dữ liệu đã được preview và validate"""
    try:
        data = request.json
        records = data.get('records', [])
        
        if not records:
            return jsonify({'success': False, 'error': 'Không có dữ liệu để import'}), 400
        
        # Filter only valid records
        valid_records = [r for r in records if r.get('valid', False)]
        
        if not valid_records:
            return jsonify({'success': False, 'error': 'Không có bản ghi hợp lệ để import'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get current MAX ID
        cur.execute("SELECT MAX(id) FROM etl_job")
        max_id = cur.fetchone()[0] or 0
        
        inserted_count = 0
        errors = []
        
        for record in valid_records:
            try:
                max_id += 1
                cur.execute("""
                    INSERT INTO etl_job (
                        id, job_type, schema_name, table_name, sql_path,
                        description, is_active, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    max_id,
                    record.get('job_type'),
                    record.get('schema_name'),
                    record.get('table_name'),
                    record.get('sql_path'),
                    record.get('description'),
                    record.get('is_active', True),
                    datetime.now(),
                    datetime.now()
                ))
                inserted_count += 1
            except Exception as e:
                errors.append(f"Dòng {record.get('row_number')}: {str(e)}")
                max_id -= 1  # Rollback ID increment
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Đã import thành công {inserted_count}/{len(valid_records)} bản ghi',
            'inserted': inserted_count,
            'errors': errors
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    # Lấy port từ environment variable, mặc định
    port = int(os.getenv('APP_PORT'))
    app.run(debug=True, host='0.0.0.0', port=port)
