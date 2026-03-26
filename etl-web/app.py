from flask import Flask, render_template, request, jsonify, session
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os
import json
from functools import wraps
from dotenv import load_dotenv
import pandas as pd
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

# Load environment variables từ file .env
load_dotenv()

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('APP_SECRET_KEY', 'etl-web-secret-key')

# Cấu hình kết nối database
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}
ENV_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
APP_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_ROOT_DIR = os.getenv('SQL_ROOT_DIR', '/app/sql')
USER_LIST_FILE_PATH = os.path.join(APP_BASE_DIR, 'users.json')
AUDIT_LOG_FILE_PATH = os.path.join(APP_BASE_DIR, 'audit_logs.json')

ROLE_ADMIN = 'admin'
ROLE_USER = 'user'

def get_db_connection():
    """Tạo kết nối đến database"""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def read_users_from_file():
    if not os.path.exists(USER_LIST_FILE_PATH):
        return []
    with open(USER_LIST_FILE_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else []

def write_users_to_file(users):
    with open(USER_LIST_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

def sanitize_user_output(user):
    if not user:
        return None
    return {
        'id': user.get('id'),
        'username': user.get('username'),
        'full_name': user.get('full_name'),
        'role': user.get('role'),
        'is_active': user.get('is_active', True),
        'created_at': user.get('created_at'),
        'updated_at': user.get('updated_at')
    }

def ensure_user_file():
    """Đảm bảo file users.json tồn tại và có admin."""
    users = read_users_from_file()
    now = datetime.now().isoformat()

    # Chuẩn hóa dữ liệu cũ (nếu thiếu trường)
    normalized = []
    for u in users:
        item = dict(u)
        item.setdefault('role', ROLE_USER)
        item.setdefault('is_active', True)
        item.setdefault('full_name', None)
        item.setdefault('created_at', now)
        item.setdefault('updated_at', now)
        normalized.append(item)
    users = normalized

    admin = next((u for u in users if u.get('username') == 'admin'), None)
    if not admin:
        default_password = os.getenv('DEFAULT_ADMIN_PASSWORD', 'admin123')
        next_id = max([int(u.get('id', 0)) for u in users] + [0]) + 1
        users.append({
            'id': next_id,
            'username': 'admin',
            'password_hash': generate_password_hash(default_password),
            'full_name': 'Administrator',
            'role': ROLE_ADMIN,
            'is_active': True,
            'created_at': now,
            'updated_at': now
        })
    else:
        if not admin.get('password_hash'):
            default_password = os.getenv('DEFAULT_ADMIN_PASSWORD', 'admin123')
            admin['password_hash'] = generate_password_hash(default_password)
            admin['updated_at'] = now
        admin['role'] = ROLE_ADMIN
        admin['is_active'] = True

    write_users_to_file(users)

def read_audit_logs():
    if not os.path.exists(AUDIT_LOG_FILE_PATH):
        return []
    try:
        with open(AUDIT_LOG_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []

def write_audit_logs(logs):
    with open(AUDIT_LOG_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)

def append_audit_log(action, detail=None, status='success', username=None):
    actor = username
    if not actor:
        user = get_current_user()
        actor = user.get('username') if user else 'anonymous'

    logs = read_audit_logs()
    logs.append({
        'created_at': datetime.now().isoformat(),
        'username': actor,
        'action': action,
        'status': status,
        'detail': detail or ''
    })

    # Giữ tối đa 2000 log gần nhất để file không quá lớn
    if len(logs) > 2000:
        logs = logs[-2000:]

    write_audit_logs(logs)

def get_current_user():
    ensure_user_file()
    user_id = session.get('user_id')
    if not user_id:
        return None
    users = read_users_from_file()
    user = next((u for u in users if str(u.get('id')) == str(user_id)), None)
    return sanitize_user_output(user)

def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Vui lòng đăng nhập'}), 401
        return func(*args, **kwargs)
    return wrapper

def admin_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        user = get_current_user()
        if not user:
            return jsonify({'success': False, 'error': 'Vui lòng đăng nhập'}), 401
        if not user.get('is_active'):
            return jsonify({'success': False, 'error': 'Tài khoản đã bị khóa'}), 403
        if user.get('role') != ROLE_ADMIN:
            return jsonify({'success': False, 'error': 'Không có quyền thực hiện thao tác này'}), 403
        return func(*args, **kwargs)
    return wrapper

def resolve_sql_path(sql_path):
    """Chuẩn hóa đường dẫn SQL để ghi file (ưu tiên SQL_ROOT_DIR)."""
    if not sql_path:
        return None
    normalized = sql_path.strip().replace('\\', '/')
    if os.path.isabs(normalized):
        return os.path.normpath(normalized)

    if normalized.startswith('sql/'):
        relative_part = normalized[len('sql/'):]
        return os.path.normpath(os.path.join(SQL_ROOT_DIR, relative_part))

    return os.path.normpath(os.path.join(SQL_ROOT_DIR, normalized))

def resolve_sql_path_candidates(sql_path):
    """Tạo danh sách candidate path để đọc file SQL linh hoạt hơn."""
    if not sql_path:
        return []

    normalized = sql_path.strip().replace('\\', '/')
    candidates = []

    if os.path.isabs(normalized):
        candidates.append(os.path.normpath(normalized))
        return candidates

    # Ưu tiên mapping chuẩn của hệ thống
    candidates.append(resolve_sql_path(normalized))

    # Fallback cho môi trường local/dev chưa mount /app/sql
    # 1) Theo root project: ./sql/...
    if normalized.startswith('sql/'):
        relative_part = normalized[len('sql/'):]
        candidates.append(os.path.normpath(os.path.join(APP_BASE_DIR, 'sql', relative_part)))

    # 2) Theo path relative trực tiếp từ APP_BASE_DIR
    candidates.append(os.path.normpath(os.path.join(APP_BASE_DIR, normalized)))

    # remove duplicate while preserving order
    unique_candidates = []
    seen = set()
    for c in candidates:
        if c and c not in seen:
            unique_candidates.append(c)
            seen.add(c)
    return unique_candidates

def load_sql_command_from_path(sql_path):
    """Đọc nội dung câu lệnh SQL từ file theo sql_path."""
    if not sql_path:
        return None

    candidates = resolve_sql_path_candidates(sql_path)
    for file_path in candidates:
        if os.path.isfile(file_path):
            with open(file_path, 'r', encoding='utf-8') as sql_file:
                content = sql_file.read().strip()
            if not content:
                raise ValueError(f'File SQL rỗng: {sql_path}')
            return content

    attempted = '; '.join(candidates)
    raise ValueError(f'Không tìm thấy file SQL: {sql_path}. Đã thử: {attempted}')

def save_sql_command_to_path(sql_path, sql_command):
    """Lưu nội dung SQL vào file theo sql_path."""
    if not sql_path:
        raise ValueError('Đường dẫn SQL không được rỗng')
    if sql_command is None:
        raise ValueError('Nội dung SQL không được rỗng')

    file_path = resolve_sql_path(sql_path)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as sql_file:
        sql_file.write(sql_command)

def update_env_file(updates):
    """Cập nhật các key DB_* trong file .env, giữ nguyên các dòng khác."""
    lines = []
    if os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()

    updated_keys = set()
    new_lines = []

    for line in lines:
        stripped_line = line.strip()
        if not stripped_line or stripped_line.startswith('#') or '=' not in line:
            new_lines.append(line)
            continue

        key, _ = line.split('=', 1)
        key = key.strip()

        if key in updates:
            new_lines.append(f"{key}={updates[key]}\n")
            updated_keys.add(key)
        else:
            new_lines.append(line)

    for key, value in updates.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}\n")

    with open(ENV_FILE_PATH, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

@app.route('/')
def index():
    """Trang chính"""
    user = get_current_user()
    is_admin = bool(user and user.get('role') == ROLE_ADMIN)
    return render_template('index.html', is_admin=is_admin)

@app.route('/etl-job-logs')
@login_required
def etl_job_logs_page():
    """Trang lịch sử ETL Job."""
    return render_template('etl_job_logs.html')

@app.route('/etl-table-migrate-brand')
@admin_required
def etl_table_migrate_brand_page():
    """Trang thêm/sửa/xóa etl_table_migrate_brand (chỉ admin)."""
    return render_template('etl_table_migrate_brand.html')

@app.route('/api/auth/login', methods=['POST'])
def login():
    try:
        ensure_user_file()
        data = request.json or {}
        username = str(data.get('username', '')).strip()
        password = str(data.get('password', ''))
        if not username or not password:
            append_audit_log('auth.login', 'Thiếu username hoặc password', status='failed', username=username or 'anonymous')
            return jsonify({'success': False, 'error': 'Vui lòng nhập username và password'}), 400

        users = read_users_from_file()
        user = next((u for u in users if u.get('username') == username), None)

        if not user or not user.get('password_hash') or not check_password_hash(user['password_hash'], password):
            append_audit_log('auth.login', 'Sai tài khoản hoặc mật khẩu', status='failed', username=username)
            return jsonify({'success': False, 'error': 'Sai tài khoản hoặc mật khẩu'}), 401
        if not user.get('is_active'):
            append_audit_log('auth.login', 'Tài khoản bị khóa', status='failed', username=username)
            return jsonify({'success': False, 'error': 'Tài khoản đã bị khóa'}), 403

        session['user_id'] = user['id']
        append_audit_log('auth.login', 'Đăng nhập thành công', username=user.get('username'))
        return jsonify({
            'success': True,
            'message': 'Đăng nhập thành công',
            'data': {
                'id': user['id'],
                'username': user['username'],
                'full_name': user.get('full_name'),
                'role': user.get('role')
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    username = None
    current = get_current_user()
    if current:
        username = current.get('username')
    session.pop('user_id', None)
    append_audit_log('auth.logout', 'Đăng xuất', username=username or 'anonymous')
    return jsonify({'success': True, 'message': 'Đăng xuất thành công'})

@app.route('/api/auth/me', methods=['GET'])
@login_required
def auth_me():
    try:
        ensure_user_file()
        user = get_current_user()
        if not user:
            return jsonify({'success': False, 'error': 'Vui lòng đăng nhập'}), 401
        return jsonify({'success': True, 'data': user})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/audit-logs', methods=['GET'])
@admin_required
def get_audit_logs():
    try:
        limit = int(request.args.get('limit', 200))
        limit = max(1, min(limit, 1000))
        logs = read_audit_logs()
        logs = logs[-limit:]
        logs.reverse()
        return jsonify({'success': True, 'data': logs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/audit-logs', methods=['DELETE'])
@admin_required
def delete_audit_logs():
    """Xóa toàn bộ lịch sử thao tác người dùng."""
    try:
        logs = read_audit_logs()
        deleted_count = len(logs)
        write_audit_logs([])
        append_audit_log('audit_logs.delete_all', f'Xóa toàn bộ lịch sử thao tác ({deleted_count} bản ghi)')
        return jsonify({
            'success': True,
            'message': f'Đã xóa {deleted_count} bản ghi lịch sử thao tác',
            'deleted_count': deleted_count
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users', methods=['GET'])
@admin_required
def get_users():
    try:
        ensure_user_file()
        users = [sanitize_user_output(u) for u in read_users_from_file()]
        users.sort(key=lambda x: int(x.get('id') or 0))
        return jsonify({'success': True, 'data': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users', methods=['POST'])
@admin_required
def create_user():
    try:
        ensure_user_file()
        data = request.json or {}
        username = str(data.get('username', '')).strip()
        password = str(data.get('password', '')).strip()
        full_name = str(data.get('full_name', '')).strip() or None
        role = str(data.get('role', ROLE_USER)).strip().lower()
        is_active = bool(data.get('is_active', True))

        if not username or not password:
            return jsonify({'success': False, 'error': 'Username và password là bắt buộc'}), 400
        if role not in [ROLE_ADMIN, ROLE_USER]:
            return jsonify({'success': False, 'error': 'Role không hợp lệ'}), 400

        users = read_users_from_file()
        if any((u.get('username') or '').lower() == username.lower() for u in users):
            return jsonify({'success': False, 'error': 'Username đã tồn tại'}), 400

        now = datetime.now().isoformat()
        next_id = max([int(u.get('id', 0)) for u in users] + [0]) + 1
        users.append({
            'id': next_id,
            'username': username,
            'password_hash': generate_password_hash(password),
            'full_name': full_name,
            'role': role,
            'is_active': is_active,
            'created_at': now,
            'updated_at': now
        })
        write_users_to_file(users)
        append_audit_log('users.create', f'Tạo user: {username}')
        return jsonify({'success': True, 'message': 'Thêm user thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users/<int:user_id>', methods=['PUT'])
@admin_required
def update_user(user_id):
    try:
        ensure_user_file()
        data = request.json or {}
        username = str(data.get('username', '')).strip()
        full_name = str(data.get('full_name', '')).strip() or None
        role = str(data.get('role', ROLE_USER)).strip().lower()
        is_active = bool(data.get('is_active', True))
        password = str(data.get('password', '')).strip()

        if not username:
            return jsonify({'success': False, 'error': 'Username là bắt buộc'}), 400
        if role not in [ROLE_ADMIN, ROLE_USER]:
            return jsonify({'success': False, 'error': 'Role không hợp lệ'}), 400

        users = read_users_from_file()
        user = next((u for u in users if str(u.get('id')) == str(user_id)), None)
        if not user:
            return jsonify({'success': False, 'error': 'Không tìm thấy user'}), 404

        duplicated = next(
            (u for u in users if str(u.get('id')) != str(user_id) and (u.get('username') or '').lower() == username.lower()),
            None
        )
        if duplicated:
            return jsonify({'success': False, 'error': 'Username đã tồn tại'}), 400

        user['username'] = username
        user['full_name'] = full_name
        user['role'] = role
        user['is_active'] = is_active
        user['updated_at'] = datetime.now().isoformat()
        if password:
            user['password_hash'] = generate_password_hash(password)

        # Bắt buộc giữ admin luôn có quyền admin và active
        if user.get('username') == 'admin':
            user['role'] = ROLE_ADMIN
            user['is_active'] = True

        write_users_to_file(users)
        append_audit_log('users.update', f'Cập nhật user id={user_id}, username={username}')
        return jsonify({'success': True, 'message': 'Cập nhật user thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@admin_required
def delete_user(user_id):
    try:
        ensure_user_file()
        current_user_id = session.get('user_id')
        if current_user_id == user_id:
            return jsonify({'success': False, 'error': 'Không thể tự xóa chính mình'}), 400

        users = read_users_from_file()
        target = next((u for u in users if str(u.get('id')) == str(user_id)), None)
        if not target:
            return jsonify({'success': False, 'error': 'Không tìm thấy user'}), 404
        if target.get('username') == 'admin':
            return jsonify({'success': False, 'error': 'Không thể xóa admin'}), 400

        users = [u for u in users if str(u.get('id')) != str(user_id)]
        write_users_to_file(users)
        append_audit_log('users.delete', f'Xóa user id={user_id}, username={target.get("username")}')
        return jsonify({'success': True, 'message': 'Xóa user thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs', methods=['GET'])
@login_required
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
            
            # Preview nội dung SQL từ sql_path để hiển thị trực tiếp trên list.
            # Không lưu vào DB thêm cột mới.
            job['sql_command_preview'] = ''
            sql_path = job.get('sql_path')
            if sql_path:
                try:
                    content = load_sql_command_from_path(sql_path)
                    preview = content.replace('\r', '').replace('\n', ' ').strip()
                    if len(preview) > 180:
                        preview = preview[:180] + '...'
                    job['sql_command_preview'] = preview
                except ValueError:
                    job['sql_command_preview'] = ''
        
        return jsonify({'success': True, 'data': jobs})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/etl-job-logs', methods=['GET'])
@login_required
def get_etl_job_logs():
    """Lấy lịch sử chạy job từ bảng etl_job_log."""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))
        page = max(1, page)
        page_size = max(1, min(page_size, 200))
        offset = (page - 1) * page_size
        table_name = str(request.args.get('table_name', '')).strip()

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if table_name:
            like_value = f'%{table_name.lower()}%'
            cur.execute("""
                SELECT COUNT(1) AS total
                FROM etl_job_log
                WHERE LOWER(COALESCE(table_name, '')) LIKE %s
            """, (like_value,))
            total = int(cur.fetchone().get('total') or 0)

            cur.execute("""
                SELECT id, job_name, job_type, table_name, sql_path, operation_type,
                       error_level, message, error_traceback, error_code,
                       year, month, day, delete_column, delete_condition,
                       rows_inserted, execution_time_ms, created_by, created_at
                FROM etl_job_log
                WHERE LOWER(COALESCE(table_name, '')) LIKE %s
                ORDER BY id DESC
                LIMIT %s OFFSET %s
            """, (like_value, page_size, offset))
        else:
            cur.execute("SELECT COUNT(1) AS total FROM etl_job_log")
            total = int(cur.fetchone().get('total') or 0)

            cur.execute("""
                SELECT id, job_name, job_type, table_name, sql_path, operation_type,
                       error_level, message, error_traceback, error_code,
                       year, month, day, delete_column, delete_condition,
                       rows_inserted, execution_time_ms, created_by, created_at
                FROM etl_job_log
                ORDER BY id DESC
                LIMIT %s OFFSET %s
            """, (page_size, offset))
        logs = cur.fetchall()
        cur.close()
        conn.close()

        for item in logs:
            item['created_at'] = item['created_at'].isoformat() if item.get('created_at') else None

        total_pages = (total + page_size - 1) // page_size if total > 0 else 1
        return jsonify({
            'success': True,
            'data': logs,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total': total,
                'total_pages': total_pages
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/etl-job-logs/delete-by-date', methods=['DELETE'])
@admin_required
def delete_etl_job_logs_by_date():
    """Xóa lịch sử etl_job_log theo ngày (YYYY-MM-DD)."""
    try:
        payload = request.json or {}
        target_date = str(payload.get('date', '')).strip()
        if not target_date:
            return jsonify({'success': False, 'error': 'Thiếu tham số date'}), 400

        # Validate định dạng ngày
        datetime.strptime(target_date, '%Y-%m-%d')

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM etl_job_log
            WHERE created_at::date = %s::date
        """, (target_date,))
        deleted_count = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        append_audit_log('etl_job_logs.delete_by_date', f'Xóa {deleted_count} bản ghi ngày {target_date}')
        return jsonify({
            'success': True,
            'message': f'Đã xóa {deleted_count} bản ghi ngày {target_date}',
            'deleted_count': deleted_count
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

def _parse_int_or_none(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == '':
        return None
    return int(value)

def _parse_date_or_none(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == '':
        return None
    # Định dạng: YYYY-MM-DD
    if isinstance(value, str):
        return datetime.strptime(value.strip(), '%Y-%m-%d').date()
    return value

@app.route('/api/etl-table-migrate-brand', methods=['GET'])
@admin_required
def get_etl_table_migrate_brand():
    """Lấy danh sách etl_table_migrate_brand."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT id, process_group_id, process_group_name, process_id, process_name,
                   status, updated_at, record_count
            FROM public.etl_table_migrate_brand
            ORDER BY id DESC
            LIMIT 500
        """)
        rows = cur.fetchall()
        for r in rows:
            if r.get('updated_at'):
                # date -> string for JSON
                r['updated_at'] = r['updated_at'].isoformat()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/etl-table-migrate-brand', methods=['POST'])
@admin_required
def create_etl_table_migrate_brand():
    """Thêm mới etl_table_migrate_brand."""
    try:
        data = request.json or {}
        process_group_id = data.get('process_group_id')
        process_group_name = data.get('process_group_name')
        process_id = data.get('process_id')
        process_name = data.get('process_name')
        status = _parse_int_or_none(data.get('status'))
        updated_at = _parse_date_or_none(data.get('updated_at'))
        record_count = _parse_int_or_none(data.get('record_count'))

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.etl_table_migrate_brand (
                process_group_id, process_group_name, process_id, process_name,
                status, updated_at, record_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            process_group_id, process_group_name, process_id, process_name,
            status, updated_at, record_count
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()

        append_audit_log('etl_table_migrate_brand.create', f'Tạo id={new_id}')
        return jsonify({'success': True, 'message': 'Thêm mới thành công', 'id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/etl-table-migrate-brand/<int:row_id>', methods=['PUT'])
@admin_required
def update_etl_table_migrate_brand(row_id):
    """Cập nhật etl_table_migrate_brand."""
    try:
        data = request.json or {}
        process_group_id = data.get('process_group_id')
        process_group_name = data.get('process_group_name')
        process_id = data.get('process_id')
        process_name = data.get('process_name')
        status = _parse_int_or_none(data.get('status'))
        updated_at = _parse_date_or_none(data.get('updated_at'))
        record_count = _parse_int_or_none(data.get('record_count'))

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE public.etl_table_migrate_brand SET
                process_group_id = %s,
                process_group_name = %s,
                process_id = %s,
                process_name = %s,
                status = %s,
                updated_at = %s,
                record_count = %s
            WHERE id = %s
        """, (
            process_group_id, process_group_name, process_id, process_name,
            status, updated_at, record_count,
            row_id
        ))
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()

        if affected == 0:
            return jsonify({'success': False, 'error': 'Không tìm thấy bản ghi'}), 404

        append_audit_log('etl_table_migrate_brand.update', f'Cập nhật id={row_id}')
        return jsonify({'success': True, 'message': 'Cập nhật thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/etl-table-migrate-brand/<int:row_id>', methods=['DELETE'])
@admin_required
def delete_etl_table_migrate_brand(row_id):
    """Xóa etl_table_migrate_brand."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM public.etl_table_migrate_brand WHERE id = %s", (row_id,))
        conn.commit()
        affected = cur.rowcount
        cur.close()
        conn.close()

        if affected == 0:
            return jsonify({'success': False, 'error': 'Không tìm thấy bản ghi'}), 404

        append_audit_log('etl_table_migrate_brand.delete', f'Xóa id={row_id}')
        return jsonify({'success': True, 'message': 'Xóa thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs', methods=['POST'])
@login_required
def create_job():
    """Tạo mới một ETL job"""
    try:
        data = request.json or {}
        sql_path = (data.get('sql_path') or '').strip()
        sql_command = data.get('sql_command')
        if sql_path and sql_command is not None:
            save_sql_command_to_path(sql_path, sql_command)
        
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
            sql_path or None,
            data.get('description'),
            data.get('is_active', True),
            datetime.now(),
            datetime.now()
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        append_audit_log('jobs.create', f'Tạo job id={new_id}, table={data.get("table_name")}')
        return jsonify({'success': True, 'message': 'Tạo job thành công', 'id': new_id})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs/<int:job_id>', methods=['PUT'])
@login_required
def update_job(job_id):
    """Cập nhật một ETL job"""
    try:
        data = request.json or {}
        sql_path = (data.get('sql_path') or '').strip()
        sql_command = data.get('sql_command')
        if sql_path and sql_command is not None:
            save_sql_command_to_path(sql_path, sql_command)
        
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
            sql_path or None,
            data.get('description'),
            data.get('is_active', True),
            datetime.now(),
            job_id
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        append_audit_log('jobs.update', f'Cập nhật job id={job_id}, table={data.get("table_name")}')
        return jsonify({'success': True, 'message': 'Cập nhật job thành công'})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/jobs/<int:job_id>', methods=['DELETE'])
@login_required
def delete_job(job_id):
    """Xóa một ETL job"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("DELETE FROM etl_job WHERE id = %s", (job_id,))
        
        conn.commit()
        cur.close()
        conn.close()
        append_audit_log('jobs.delete', f'Xóa job id={job_id}')
        return jsonify({'success': True, 'message': 'Xóa job thành công'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/connection', methods=['GET'])
@admin_required
def get_connection():
    """Lấy cấu hình connection database hiện tại."""
    try:
        return jsonify({
            'success': True,
            'data': {
                'host': os.getenv('DB_HOST', ''),
                'port': os.getenv('DB_PORT', ''),
                'database': os.getenv('DB_NAME', ''),
                'user': os.getenv('DB_USER', ''),
                'password': os.getenv('DB_PASSWORD', '')
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/connection', methods=['PUT'])
@admin_required
def update_connection():
    """Cập nhật cấu hình connection database và ghi vào file .env."""
    try:
        data = request.json or {}

        host = str(data.get('host', '')).strip()
        port = str(data.get('port', '')).strip()
        database = str(data.get('database', '')).strip()
        user = str(data.get('user', '')).strip()
        password = str(data.get('password', ''))

        if not all([host, port, database, user, password]):
            return jsonify({'success': False, 'error': 'Vui lòng nhập đầy đủ thông tin connection'}), 400

        new_config = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }

        env_updates = {
            'DB_HOST': host,
            'DB_PORT': port,
            'DB_NAME': database,
            'DB_USER': user,
            'DB_PASSWORD': password
        }
        update_env_file(env_updates)

        os.environ['DB_HOST'] = host
        os.environ['DB_PORT'] = port
        os.environ['DB_NAME'] = database
        os.environ['DB_USER'] = user
        os.environ['DB_PASSWORD'] = password

        DB_CONFIG['host'] = host
        DB_CONFIG['port'] = port
        DB_CONFIG['database'] = database
        DB_CONFIG['user'] = user
        DB_CONFIG['password'] = password
        append_audit_log('connection.update', f'Cập nhật DB connection {host}:{port}/{database}')

        # Test kết nối sau khi lưu để trả warning nếu chưa connect được,
        # nhưng không chặn thao tác "sửa connection".
        warning = None
        try:
            test_conn = psycopg2.connect(**new_config)
            test_conn.close()
        except Exception as test_err:
            warning = f'Đã lưu cấu hình nhưng chưa kết nối được DB: {str(test_err)}'

        return jsonify({
            'success': True,
            'message': 'Cập nhật connection thành công',
            'warning': warning
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'Lỗi cập nhật connection: {str(e)}'}), 400

@app.route('/api/sql-content', methods=['GET'])
@login_required
def get_sql_content():
    """Đọc nội dung SQL từ đường dẫn sql_path."""
    try:
        sql_path = (request.args.get('sql_path') or '').strip()
        if not sql_path:
            return jsonify({'success': False, 'error': 'Thiếu tham số sql_path'}), 400
        content = load_sql_command_from_path(sql_path)
        return jsonify({'success': True, 'data': {'sql_path': sql_path, 'sql_command': content}})
    except ValueError as e:
        # Nếu file chưa tồn tại thì vẫn cho phép UI hiển thị rỗng,
        # lúc bấm "Lưu" thì backend sẽ tạo file .sql theo nội dung user nhập.
        msg = str(e)
        if msg.startswith('Không tìm thấy file SQL:'):
            return jsonify({'success': True, 'data': {'sql_path': sql_path, 'sql_command': ''}, 'missing': True})
        return jsonify({'success': False, 'error': msg}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/sql-content', methods=['PUT'])
@login_required
def update_sql_content():
    """Cập nhật nội dung SQL vào file theo đường dẫn sql_path."""
    try:
        data = request.json or {}
        sql_path = (data.get('sql_path') or '').strip()
        sql_command = data.get('sql_command')
        save_sql_command_to_path(sql_path, sql_command)
        append_audit_log('sql.update', f'Cập nhật SQL path={sql_path}')
        return jsonify({'success': True, 'message': 'Lưu nội dung SQL thành công'})
    except ValueError as e:
        return jsonify({'success': False, 'error': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/import/preview', methods=['POST'])
@admin_required
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
@admin_required
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
        append_audit_log('import.confirm', f'Import {inserted_count}/{len(valid_records)} bản ghi')
        
        return jsonify({
            'success': True,
            'message': f'Đã import thành công {inserted_count}/{len(valid_records)} bản ghi',
            'inserted': inserted_count,
            'errors': errors
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    ensure_user_file()
    # Lấy port từ environment variable, mặc định -v111
    port = int(os.getenv('APP_PORT'))
    app.run(debug=True, host='0.0.0.0', port=port)



###codemonkey