# Hệ thống Quản lý ETL Jobs

Giao diện web để quản lý thông tin các ETL jobs trong cơ sở dữ liệu PostgreSQL.

## Tính năng

- ✅ Thêm mới ETL job
- ✅ Chỉnh sửa thông tin job
- ✅ Xóa job
- ✅ Hiển thị danh sách jobs
- ✅ **Lọc jobs theo loại (Daily/Weekly/Month/Quarter/Year)**
- ✅ **Tìm kiếm theo tên bảng (realtime search)**
- ✅ **Lọc theo trạng thái (Active/Inactive)**
- ✅ **Click SQL path để xem full content trong modal**
- ✅ **Copy SQL path với 1 click**
- ✅ **Tối ưu hóa hiệu suất**: DOM caching, event delegation, giảm 12% code
- ✅ Đếm số lượng jobs theo filter
- ✅ Giao diện đẹp, hiện đại
- ✅ Validation dữ liệu
- ✅ Responsive design

## Cấu trúc thư mục

```
Input_text/
├── app.py                  # Flask backend API
├── requirements.txt        # Python dependencies
├── .env.example           # File cấu hình mẫu
├── templates/
│   └── index.html         # Giao diện web
├── static/
│   ├── css/
│   │   └── style.css     # Styles
│   └── js/
│       └── main.js       # JavaScript logic
└── README.md             # Tài liệu
```

## Cài đặt

### 1. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Cấu hình database và ứng dụng

Sao chép file `.env.example` thành `.env` và cập nhật thông tin:

```bash
cp .env.example .env
```

Sửa file `.env`:

```
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Application
APP_PORT=5333
```

### 3. Tạo bảng trong PostgreSQL

Chạy SQL script sau để tạo bảng `etl_job`:

```sql
CREATE TABLE etl_job (
    id serial4 NOT NULL,
    job_type varchar(200) NULL,
    schema_name varchar(50) NULL,
    table_name varchar(200) NULL,
    sql_path varchar(500) NULL,
    batch_size int4 NULL,
    delete_column varchar(100) NULL,
    delete_condition varchar(200) NULL,
    description varchar(500) NULL,
    is_active bool NULL,
    created_at timestamp NULL,
    updated_at timestamp NULL,
    CONSTRAINT etl_job_pkey PRIMARY KEY (id)
);
```

## Chạy ứng dụng

### Development mode

```bash
python app.py
```

Ứng dụng sẽ chạy tại: `http://localhost:5333`

### Production mode (với Gunicorn)

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5333 app:app
```

## Sử dụng

### 1. Xem và lọc dữ liệu

1. Truy cập `http://localhost:5333` trên trình duyệt
2. Sử dụng các nút filter để lọc jobs theo loại:
   - **Tất cả**: Hiển thị tất cả jobs
   - **Daily**: Jobs chạy hàng ngày
   - **Weekly**: Jobs chạy hàng tuần
   - **Month**: Jobs chạy hàng tháng
   - **Quarter**: Jobs chạy hàng quý
   - **Year**: Jobs chạy hàng năm
3. Số lượng jobs hiện tại sẽ được hiển thị bên phải

### 2. Thêm Job mới

1. Click "Thêm Job Mới"
2. Điền thông tin vào form:
   
   **Trường bắt buộc:**
   - **Loại Job**: Chọn từ dropdown (1-Daily, 2-Weekly, 3-Month, 4-Quarter, 5-Year)
   - **Tên Schema**: Schema trong database (bắt buộc)
   - **Tên Bảng**: Tên bảng dữ liệu (bắt buộc)
   
   **Trường tùy chọn:**
   - **Đường dẫn SQL**: Path đến file SQL query
   - **Mô tả**: Mô tả chi tiết về job
   - **Kích hoạt**: Checkbox để bật/tắt job (mặc định: bật)
   
   **Lưu ý**: Các trường `batch_size`, `delete_column`, `delete_condition` sẽ tự động được set NULL

3. Click "Lưu" để lưu thông tin

### 3. Quản lý Jobs

- **Sửa**: Click nút "Sửa" ở cột Thao tác để chỉnh sửa thông tin job
- **Xóa**: Click nút "Xóa" để xóa job (có xác nhận trước khi xóa)
- **Filter**: Các filter sẽ được giữ nguyên khi thêm/sửa/xóa job

## API Endpoints

**Base URL:** `http://localhost:{APP_PORT}/api` (mặc định: 5333)

### GET /api/jobs
Lấy danh sách tất cả jobs

### GET /api/jobs/<id>
Lấy thông tin chi tiết một job

### POST /api/jobs
Tạo job mới

### PUT /api/jobs/<id>
Cập nhật thông tin job

### DELETE /api/jobs/<id>
Xóa job

### POST /api/fix-sequence
Fix sequence ID của bảng

## Công nghệ sử dụng

- **Backend**: Python Flask
- **Database**: PostgreSQL
- **Frontend**: HTML5, CSS3, JavaScript (Vanilla)
- **Icons**: Font Awesome 6
- **Database Driver**: psycopg2

## Lưu ý

- Đảm bảo PostgreSQL đang chạy và có thể kết nối
- Cấu hình đúng thông tin database trong file `.env`
- Đảm bảo user database có quyền đầy đủ với bảng `etl_job`
- Để chạy trong môi trường production, nên sử dụng WSGI server như Gunicorn hoặc uWSGI

## Quản lý ID

Hệ thống sử dụng phương pháp **MAX(id) + 1** để tạo ID cho jobs mới:
- Lấy giá trị MAX(id) hiện tại từ database
- ID mới = MAX(id) + 1 (hoặc 1 nếu bảng rỗng)
- Insert với ID cụ thể thay vì dùng auto-increment

**Lợi ích**:
- Kiểm soát hoàn toàn ID
- Biết chính xác ID trước khi insert
- Tránh lỗi sequence conflict

Chi tiết xem file `ID_MANAGEMENT.md`

## Bảo mật

Trong môi trường production, nên:
- Sử dụng HTTPS
- Thêm authentication/authorization
- Validate và sanitize input
- Sử dụng environment variables cho thông tin nhạy cảm
- Không commit file `.env` vào git

## License

MIT License
