-- =============================================
-- Bảng lưu log lỗi khi chạy job
-- =============================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[job_etl_error_log]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[job_etl_error_log] (
        -- Primary key
        [id] BIGINT IDENTITY(1,1) NOT NULL,
        
        -- Thông tin job
        [job_name] NVARCHAR(255) NOT NULL,                    -- Tên job (ví dụ: Get_Daily_Revenue_Summary)
        [job_type] NVARCHAR(100) NULL,                         -- Loại job (ví dụ: daily_revenue_summary)
        
        -- Thông tin xử lý
        [table_name] NVARCHAR(255) NULL,                       -- Tên bảng đang xử lý
        [sql_path] NVARCHAR(500) NULL,                         -- Đường dẫn file SQL
        [operation_type] NVARCHAR(50) NULL,                     -- Loại thao tác (DELETE, INSERT, QUERY, etc.)
        
        -- Thông tin lỗi
        [error_level] NVARCHAR(20) NOT NULL DEFAULT 'ERROR',   -- Mức độ lỗi: ERROR, WARNING, INFO
        [error_message] NVARCHAR(MAX) NULL,                    -- Thông điệp lỗi
        [error_traceback] NVARCHAR(MAX) NULL,                   -- Stack trace đầy đủ
        [error_code] NVARCHAR(50) NULL,                         -- Mã lỗi (nếu có)
        
        -- Thông tin ngữ cảnh
        [year] INT NULL,                                        -- Năm đang xử lý
        [month] INT NULL,                                       -- Tháng đang xử lý
        [day] INT NULL,                                         -- Ngày đang xử lý
        [delete_column] NVARCHAR(255) NULL,                    -- Cột delete (nếu có)
        [delete_condition] NVARCHAR(50) NULL,                  -- Điều kiện delete (day, month, year, time)
        
        -- Thông tin kỹ thuật
        [rows_affected] BIGINT NULL,                            -- Số dòng bị ảnh hưởng (nếu có)
        [execution_time_ms] BIGINT NULL,                        -- Thời gian thực thi (milliseconds)
        
        -- Metadata
        [created_by] NVARCHAR(100) NULL,                        -- User/system tạo log
        [created_at] DATETIME2 NOT NULL DEFAULT GETDATE(),      -- Thời gian tạo log
        
        -- Indexes
        CONSTRAINT [PK_job_etl_error_log] PRIMARY KEY CLUSTERED ([id] ASC)
    );
    
    -- Tạo indexes để tìm kiếm nhanh
    CREATE NONCLUSTERED INDEX [IX_job_etl_error_log_job_name] 
        ON [dbo].[job_etl_error_log] ([job_name] ASC);
    
    CREATE NONCLUSTERED INDEX [IX_job_etl_error_log_created_at] 
        ON [dbo].[job_etl_error_log] ([created_at] DESC);
    
    CREATE NONCLUSTERED INDEX [IX_job_etl_error_log_table_name] 
        ON [dbo].[job_etl_error_log] ([table_name] ASC);
    
    CREATE NONCLUSTERED INDEX [IX_job_etl_error_log_error_level] 
        ON [dbo].[job_etl_error_log] ([error_level] ASC);
    
    CREATE NONCLUSTERED INDEX [IX_job_etl_error_log_date] 
        ON [dbo].[job_etl_error_log] ([year] ASC, [month] ASC, [day] ASC);
    
    PRINT 'Bảng job_etl_error_log đã được tạo thành công!';
END
ELSE
BEGIN
    PRINT 'Bảng job_etl_error_log đã tồn tại!';
END
GO

-- =============================================
-- View để xem log lỗi gần đây
-- =============================================
CREATE TABLE [dbo].[etl_job](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[job_type] [nvarchar](250) NULL,
	[schema_name] [varchar](50) NULL,
	[table_name] [varchar](200) NULL,
	[sql_path] [varchar](500) NULL,
	[batch_size] [int] NULL,
	[delete_column] [varchar](100) NULL,
	[delete_condition] [varchar](200) NULL,
	[description] [varchar](500) NULL,
	[is_active] [bit] NULL,
	[created_at] [datetime] NULL,
	[updated_at] [datetime] NULL,
 CONSTRAINT [PK__etl_job__3213E83F3A88A63C] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[etl_job] ADD  CONSTRAINT [DF__etl_job__delete___2B0A656D]  DEFAULT ('1=1') FOR [delete_condition]
GO

ALTER TABLE [dbo].[etl_job] ADD  CONSTRAINT [DF__etl_job__is_acti__29221CFB]  DEFAULT ((1)) FOR [is_active]
GO

ALTER TABLE [dbo].[etl_job] ADD  CONSTRAINT [DF__etl_job__created__2BFE89A6]  DEFAULT (getdate()) FOR [created_at]
GO

ALTER TABLE [dbo].[etl_job] ADD  CONSTRAINT [DF__etl_job__updated__2CF2ADDF]  DEFAULT (getdate()) FOR [updated_at]
GO

