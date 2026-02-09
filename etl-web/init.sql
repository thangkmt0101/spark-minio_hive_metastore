-- Script khởi tạo database cho ETL Jobs Management

-- Tạo bảng etl_job nếu chưa tồn tại
CREATE TABLE IF NOT EXISTS etl_job (
    id SERIAL PRIMARY KEY,
    job_type VARCHAR(200),
    schema_name VARCHAR(50),
    table_name VARCHAR(200),
    sql_path VARCHAR(500),
    description VARCHAR(500),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo index cho các cột thường xuyên query
CREATE INDEX IF NOT EXISTS idx_job_type ON etl_job(job_type);
CREATE INDEX IF NOT EXISTS idx_is_active ON etl_job(is_active);
CREATE INDEX IF NOT EXISTS idx_table_name ON etl_job(table_name);

-- Insert dữ liệu mẫu (optional)
INSERT INTO etl_job (job_type, schema_name, table_name, sql_path, description, is_active) 
VALUES 
    ('1', 'public', 'users', '/sql/extract_users.sql', 'Extract user data daily', true),
    ('2', 'analytics', 'sales', '/sql/weekly_sales.sql', 'Weekly sales report', true),
    ('3', 'public', 'orders', '/sql/monthly_orders.sql', 'Monthly order summary', false)
ON CONFLICT DO NOTHING;

-- Thông báo
SELECT 'Database initialized successfully!' AS message;
