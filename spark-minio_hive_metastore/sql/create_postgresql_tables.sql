-- Script tạo bảng và view cho ETL trong PostgreSQL
-- Tương thích với cấu trúc SQL Server đã có

-- Tạo bảng etl_job
CREATE TABLE IF NOT EXISTS etl_job (
    id SERIAL PRIMARY KEY,
    job_type VARCHAR(200),
    schema_name VARCHAR(50),
    table_name VARCHAR(200),
    sql_path VARCHAR(500),
    batch_size INTEGER,
    delete_column VARCHAR(100),
    delete_condition VARCHAR(200),
    description VARCHAR(500),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Tạo bảng job_etl_log (giữ nguyên cấu trúc như SQL Server)
CREATE TABLE IF NOT EXISTS job_etl_log (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(255),
    job_type VARCHAR(100),
    table_name VARCHAR(255),
    sql_path VARCHAR(500),
    operation_type VARCHAR(50),
    error_level VARCHAR(20),
    message TEXT,
    error_traceback TEXT,
    error_code VARCHAR(50),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    delete_column VARCHAR(255),
    delete_condition VARCHAR(50),
    rows_inserted BIGINT,
    execution_time_ms BIGINT,
    created_by VARCHAR(100),
    created_at TIMESTAMP
);

-- Tạo view view_etl_job (tương tự SQL Server view)
CREATE OR REPLACE VIEW view_etl_job AS
SELECT 
    id,
    job_type,
    schema_name,
    table_name,
    sql_path,
    batch_size,
    delete_column,
    delete_condition,
    description,
    is_active,
    created_at,
    updated_at
FROM etl_job
WHERE table_name IN ('plaza_traffic');

-- Tạo index để tối ưu query
CREATE INDEX IF NOT EXISTS idx_etl_job_job_type ON etl_job(job_type);
CREATE INDEX IF NOT EXISTS idx_etl_job_is_active ON etl_job(is_active);
CREATE INDEX IF NOT EXISTS idx_etl_job_table_name ON etl_job(table_name);

CREATE INDEX IF NOT EXISTS idx_job_etl_log_job_type ON job_etl_log(job_type);
CREATE INDEX IF NOT EXISTS idx_job_etl_log_table_name ON job_etl_log(table_name);
CREATE INDEX IF NOT EXISTS idx_job_etl_log_created_at ON job_etl_log(created_at);
CREATE INDEX IF NOT EXISTS idx_job_etl_log_error_level ON job_etl_log(error_level);

