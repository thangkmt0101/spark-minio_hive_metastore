# Oracle Sync — Công cụ đồng bộ dữ liệu Oracle → Oracle

Ứng dụng web Flask cho phép khai báo kết nối, định nghĩa mapping cột và thực thi đồng bộ dữ liệu giữa hai database Oracle. Toàn bộ cấu hình lưu trong file (XML/CSV), không cần bảng trong database.

---

## Mục lục

1. [Cấu trúc dự án](#cấu-trúc-dự-án)
2. [Cách chạy](#cách-chạy)
3. [Luồng nghiệp vụ chi tiết](#luồng-nghiệp-vụ-chi-tiết)
   - [1. Khai báo kết nối](#1-khai-báo-kết-nối)
   - [2. Tạo job đồng bộ](#2-tạo-job-đồng-bộ)
   - [3. Mapping cột và sinh SQL](#3-mapping-cột-và-sinh-sql)
   - [4. Thực thi đồng bộ](#4-thực-thi-đồng-bộ)
   - [5. Theo dõi lịch sử và lỗi](#5-theo-dõi-lịch-sử-và-lỗi)
4. [Lưu trữ dữ liệu](#lưu-trữ-dữ-liệu)
5. [API endpoints](#api-endpoints)
6. [Docker](#docker)

---

## Cấu trúc dự án

```
Clone_data/
├── config/
│   ├── file_config.py        # Đọc/ghi sync_config.xml (kết nối Oracle)
│   ├── csv_statements.py     # Đọc/ghi job_sync.csv (danh sách job)
│   └── xml_history.py        # Đọc/ghi job_his.xml (lịch sử chạy)
│
├── src/
│   ├── metadata_loader.py    # Truy vấn user_tables / user_tab_columns từ Oracle
│   └── config_loader.py      # Adapter đọc kết nối (dùng cho run_sync legacy)
│
├── scripts/                  # *** Data files — mount volume Docker ***
│   ├── sync_config.xml       # Danh sách kết nối Oracle
│   ├── job_sync.csv          # Danh sách job đồng bộ + câu lệnh SQL
│   └── job_his.xml           # Lịch sử chạy từng job
│
├── web/
│   ├── app.py                # Flask application — routes + business logic
│   ├── static/css/main.css   # Stylesheet
│   └── templates/
│       ├── _menu.html              # Navigation sidebar
│       ├── index.html              # Danh sách job (trang chủ)
│       ├── job_new.html            # Form tạo job mới
│       ├── man_hinh_them_moi.html  # Mapping cột + sinh SQL
│       ├── connection.html         # Quản lý kết nối
│       ├── connection_edit.html    # Form sửa kết nối
│       ├── job_errors.html         # Danh sách job lỗi gần nhất
│       ├── job_history.html        # Lịch sử chạy của 1 job
│       └── run_result.html         # Kết quả chạy (non-SSE)
│
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Cách chạy

### Chạy trực tiếp (Python)

```bash
# Cài dependencies
pip install -r requirements.txt

# Chạy từ thư mục gốc (Clone_data)
PYTHONPATH=. python web/app.py

# Windows (PowerShell)
$env:PYTHONPATH="."; python web/app.py
```

Truy cập: `http://localhost:5000`

---

### Chạy bằng Docker

> **Yêu cầu:** Docker Desktop đang chạy (icon taskbar chuyển xanh).  
> Kiểm tra: `docker info` — nếu in thông tin engine là Docker đang chạy.

#### Bước 1 — Build image

```powershell
cd D:\Clone_data
docker compose build
```

> Image được build từ `Dockerfile`. Bước này chỉ cần chạy lại khi thay đổi source code.

#### Bước 2 — Khởi động container

```powershell
docker compose up -d
```

> Khi container start, `entrypoint.sh` tự động:
> 1. Reset `scripts/job_his.xml` về trạng thái rỗng `<histories />`
> 2. Khởi động Flask tại cổng 5000

Truy cập: `http://localhost:5000`

#### Kiểm tra log

```powershell
docker compose logs -f oracle-sync
```

#### Dừng container

```powershell
docker compose down
```

#### Rebuild sau khi sửa code

```powershell
docker compose build
docker compose up -d
```

> **Lưu ý:** Thư mục `scripts/` được mount làm volume — `sync_config.xml`, `job_sync.csv`  
> **không bị mất** khi rebuild image. Chỉ `job_his.xml` (lịch sử chạy) được reset mỗi lần start.

---

## Luồng nghiệp vụ chi tiết

### 1. Khai báo kết nối

**Màn hình:** `Cấu hình → Kết nối`  
**File lưu:** `scripts/sync_config.xml`

```
Người dùng điền: Tên, Loại (source/target), Host, Port, Service name, Username, Password
       ↓
POST /connection/add
       ↓
file_config.py: add_connection()
  - Kiểm tra tên kết nối không trùng (case-insensitive)
  - Gán ID tự tăng
  - Ghi vào sync_config.xml
```

Mỗi kết nối lưu dưới dạng:
```xml
<connection id="1">
  <name>QLPN_OLD</name>
  <connection_type>source</connection_type>
  <host>192.168.1.10</host>
  <port>1521</port>
  <service_name>ORCL</service_name>
  <username>user1</username>
  <password_enc>...</password_enc>
</connection>
```

---

### 2. Tạo job đồng bộ

**Màn hình:** `Đồng bộ → Thêm job mới`  
**File lưu:** `scripts/job_sync.csv`

```
Người dùng chọn: Tên job, Kết nối nguồn, Kết nối đích
Người dùng tích chọn: Bảng nguồn + Bảng đích (load từ Oracle qua API)
       ↓
POST /job/create
       ↓
csv_statements.py: add_statement_csv()
  - Tạo bản ghi mới với ID tự tăng
  - Lưu: id, name, source_connection_name, target_connection_name,
          source_table, target_table, sql_text=""
       ↓
Redirect → Màn hình Mapping cột (/job/<id>/them-moi)
```

**API tải danh sách bảng** (`GET /api/connections/<id>/tables`):
```
metadata_loader.py: get_tables()
  → Kết nối Oracle → SELECT table_name FROM user_tables
  → Trả về danh sách tên bảng (hỗ trợ filter LIKE)
```

---

### 3. Mapping cột và sinh SQL

**Màn hình:** `/job/<id>/them-moi`  
**File lưu:** `scripts/job_sync.csv` (cột `sql_text`)

```
Tải cột bảng đích (tự động khi vào màn hình):
  GET /api/connections/<id>/columns?table=<tên_bảng>
  → metadata_loader.py: get_columns()
  → SELECT column_name FROM user_tab_columns WHERE table_name = :tname

Tải cột bảng nguồn (theo yêu cầu, hoặc khi sửa tên bảng nguồn):
  Tương tự, từ kết nối nguồn

Người dùng kéo/thêm cột vào vùng Mapping:
  - Cột đích (Target Column) ← bên trái, readonly (lấy từ Oracle)
  - Cột nguồn (Source Column) ← bên phải, có thể nhập tự do
    (ví dụ: tên cột, hằng số "0 AS MTG_NAM", biểu thức, SYSDATE...)

Sinh SQL (client-side JavaScript):
  → INSERT INTO <target_conn>.<target_table> (col1, col2, ...)
    SELECT expr1, expr2, ...
    FROM <source_conn>.<source_table>

Lưu SQL:
  POST /job/<id>/save
  → csv_statements.py: update_statement_csv()
  → Ghi sql_text vào dòng id=<id> trong job_sync.csv
```

**Lưu ý:** Nếu đã có `sql_text` trong CSV, màn hình tự động parse lại SQL để hiển thị mapping hiện tại (dùng regex trong JavaScript).

---

### 4. Thực thi đồng bộ

**Màn hình:** `Danh sách job → Chọn job → Chạy đã chọn`

#### 4a. Luồng SSE (giao diện chính)

```
Người dùng chọn checkbox các job cần chạy
Bấm "Chạy đã chọn"
       ↓
Dialog chọn số luồng song song (1–4)
       ↓
JavaScript mở modal tiến độ + kết nối SSE:
  GET /api/jobs/run-stream?ids=1,2,3&workers=4
       ↓
app.py: jobs_run_stream()
  - Tạo run_id (uuid), stop_event, conns_map (thread-safe)
  - Đẩy event "started" → client nhận run_id

  ThreadPoolExecutor(max_workers=N):
    Mỗi job chạy trong thread riêng (_run_one):

    ┌──────────────────────────────────────┐
    │ _run_one(idx, stmt_id, ...)          │
    │                                      │
    │ 1. Kiểm tra stop_event               │
    │ 2. Đọc thông tin job từ job_sync.csv │
    │ 3. Push event "running" → modal      │
    │ 4. Validate (bảng đích, sql, kết nối)│
    │ 5. Kết nối Oracle target             │
    │    → Lưu conn vào conns_map          │
    │ 6. DELETE FROM target_conn.table     │
    │    → commit()                        │
    │ 7. _execute_insert():                │
    │    a. COUNT(*) source rows           │
    │    b. Nếu > LARGE_TABLE_THRESHOLD:   │
    │       → INSERT /*+ APPEND */ INTO... │
    │       → Push event "chunk" (⚡)      │
    │    c. Nếu <= ngưỡng:                 │
    │       → INSERT thông thường          │
    │    → commit()                        │
    │ 8. Ghi lịch sử vào job_his.xml      │
    │    (dùng _history_lock)              │
    │ 9. Push event "progress" (success)   │
    └──────────────────────────────────────┘

  Generator đọc từ Queue → yield SSE events → client
  Khi tất cả job xong: push event "done"
```

**INSERT /*+ APPEND */ (Oracle Direct-Path Write):**
- Áp dụng khi số dòng nguồn > `LARGE_TABLE_THRESHOLD` (mặc định 100,000)
- Oracle ghi thẳng vào datafile, bỏ qua buffer cache
- Nhanh hơn 3–10× so với INSERT thông thường cho bảng lớn
- Yêu cầu: COMMIT DELETE trước, COMMIT INSERT sau (không thể trong cùng transaction)

#### 4b. Dừng job đang chạy

```
Người dùng bấm "Dừng"
       ↓
POST /api/jobs/stop/<run_id>
       ↓
app.py: jobs_stop()
  - Đặt stop_event = True
  - Duyệt conns_map → gọi oc.cancel() trên mỗi Oracle connection
    (Oracle ngắt ngay câu lệnh đang blocking)
  - Các thread chưa bắt đầu → kiểm tra stop_event → bỏ qua
```

#### 4c. Chạy tiếp (resume)

Sau khi dừng hoặc có lỗi, nút **"Chạy tiếp"** xuất hiện:
- Chỉ chạy lại các job **chưa hoàn thành** (loại trừ job đã success hoặc error)
- Giữ nguyên số luồng của lần chạy trước

---

### 5. Theo dõi lịch sử và lỗi

#### Lịch sử chạy

```
Mỗi lần chạy job (thành công hoặc lỗi):
  xml_history.py: add_history()
  → Ghi 1 record vào job_his.xml:
     stmt_id, job_name, target_table, run_at,
     status (success/error), delete_rows, insert_rows, message
```

**Xem lịch sử:** Click icon ⊙ cạnh ID job → `/job/<id>/history`

#### Bảng lỗi

**Màn hình:** `Đồng bộ → Bảng lỗi`

```
GET /jobs/errors
  → xml_history.py: get_latest_errors()
  → Lấy lần chạy gần nhất của mỗi job
  → Lọc: chỉ giữ job có status = "error"
  → Hiển thị: tên job, bảng đích, thời gian, thông báo lỗi
```

Click tên job → xem lịch sử chi tiết của job đó.

---

## Lưu trữ dữ liệu

| File | Loại | Nội dung |
|---|---|---|
| `scripts/sync_config.xml` | XML | Danh sách kết nối Oracle (host, port, user, pass) |
| `scripts/job_sync.csv` | CSV | Danh sách job: id, name, kết nối nguồn/đích, bảng, sql_text |
| `scripts/job_his.xml` | XML | Lịch sử mỗi lần chạy job |

**Không có database trung gian** — tất cả config và lịch sử lưu trong file.

### Cấu trúc job_sync.csv

```
id,name,source_connection_name,target_connection_name,source_table,target_table,sql_text
77,JOB_DM_MA_THOI_GIANS,QLPN_OLD,QLPN,MA_MUC_THOI_GIAN,DM_MA_THOI_GIANS,"INSERT INTO QLPN.DM_MA_THOI_GIANS (...) SELECT ... FROM QLPN_OLD.MA_MUC_THOI_GIAN"
```

---

## API Endpoints

| Method | URL | Mô tả |
|---|---|---|
| GET | `/` | Danh sách job |
| GET | `/connection` | Quản lý kết nối |
| POST | `/connection/add` | Thêm kết nối |
| GET | `/connection/<id>/edit` | Form sửa kết nối |
| POST | `/connection/<id>/update` | Cập nhật kết nối |
| POST | `/connection/<id>/delete` | Xóa kết nối |
| GET | `/api/connections/<id>/tables` | Lấy danh sách bảng từ Oracle |
| GET | `/api/connections/<id>/columns` | Lấy danh sách cột của bảng |
| GET | `/job/new` | Form tạo job mới |
| POST | `/job/create` | Tạo job + redirect sang mapping |
| GET | `/job/<id>/them-moi` | Màn hình mapping cột |
| POST | `/job/<id>/save` | Lưu câu lệnh SQL |
| POST | `/job/<id>/delete` | Xóa 1 job |
| POST | `/jobs/delete-bulk` | Xóa nhiều job |
| GET | `/api/jobs/run-stream` | Chạy job (SSE real-time, đa luồng) |
| POST | `/api/jobs/stop/<run_id>` | Dừng run đang chạy |
| GET | `/jobs/errors` | Danh sách job lỗi gần nhất |
| GET | `/job/<id>/history` | Lịch sử chạy của 1 job |

---

## Docker

### Các lệnh thường dùng

| Mục đích | Lệnh |
|---|---|
| Kiểm tra Docker đang chạy | `docker info` |
| Build image | `docker compose build` |
| Start container | `docker compose up -d` |
| Build + Start (lần đầu hoặc sau khi sửa code) | `docker compose build && docker compose up -d` |
| Xem log realtime | `docker compose logs -f oracle-sync` |
| Dừng container | `docker compose down` |
| Xem trạng thái container | `docker compose ps` |

### Hành vi khi container start

Mỗi lần `docker compose up -d`, `entrypoint.sh` tự động chạy:
```
[init] Da reset job_his.xml   ← xóa lịch sử chạy cũ
[start] Khoi dong Flask...    ← Flask lắng nghe port 5000
```

### Volume và data

```
Host (D:\Clone_data\scripts\)  ←→  Container (/app/scripts/)
```

| File | Xử lý khi start |
|---|---|
| `sync_config.xml` | Giữ nguyên (kết nối Oracle) |
| `job_sync.csv` | Giữ nguyên (danh sách job) |
| `job_his.xml` | **Reset về rỗng** mỗi lần start |

> **Lưu ý:** Docker Desktop phải đang chạy (icon taskbar xanh) trước khi dùng lệnh docker.
