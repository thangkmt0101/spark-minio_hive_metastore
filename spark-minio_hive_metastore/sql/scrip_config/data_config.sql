INSERT INTO etl_job
(id, job_type, schema_name, table_name, sql_path, batch_size,
 delete_column, delete_condition, description, is_active, created_at, updated_at)
VALUES
(1,'1','dbo','transaction_summary','sql/baocao_tonghop_doanhthu/get_transaction_summary.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-04 09:42:59.603','2025-12-04 09:42:59.603'),

(6,'1','dbo','boo_comparison','sql/baocao_tonghop_doanhthu/get_boo_comparison.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:09:03.697','2025-12-09 11:09:03.697'),

(8,'1','dbo','fare_sales_monitoring','sql/baocao_tonghop_doanhthu/get_fare_sales_monitoring.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:13:23.413','2025-12-09 11:13:23.413'),

(9,'1','dbo','frontend_backend_comparison','sql/baocao_tonghop_doanhthu/get_frontend_backend_reconciliation.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:17:08.907','2025-12-09 11:17:08.907'),

(10,'1','dbo','lane_revenue','sql/baocao_tonghop_doanhthu/get_lane_revenue_summary_day.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:18:01.590','2025-12-09 11:18:01.590'),

(11,'3','dbo','lane_revenue_month','sql/baocao_tonghop_doanhthu/get_lane_revenue_summary_month.sql',50000,'month_id','month',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:18:15.690','2025-12-09 11:18:15.690'),

(12,'4','dbo','lane_revenue_quarter','sql/baocao_tonghop_doanhthu/get_lane_revenue_summary_quarter.sql',50000,'quarter_id','quarter',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:18:52.067','2025-12-09 11:18:52.067'),

(13,'5','dbo','lane_revenue_year','sql/baocao_tonghop_doanhthu/get_lane_revenue_summary_year.sql',50000,'year_id','year',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:19:05.703','2025-12-09 11:19:05.703');
INSERT INTO etl_job
(id, job_type, schema_name, table_name, sql_path, batch_size,
 delete_column, delete_condition, description, is_active, created_at, updated_at)
VALUES
(16,'4','dbo','plaza_revenue_quarter','sql/baocao_tonghop_doanhthu/get_plaza_revenue_summary_quarter.sql',50000,'quarter_id','quarter',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:19:54.420','2025-12-09 11:19:54.420'),

(17,'5','dbo','plaza_revenue_year','sql/baocao_tonghop_doanhthu/get_plaza_revenue_summary_year.sql',50000,'year_id','year',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:20:04.473','2025-12-09 11:20:04.473'),

(18,'1','dbo','plaza_route_revenue','sql/baocao_tonghop_doanhthu/get_plaza_route_share_day.sql',50000,'date','day',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:20:22.590','2025-12-09 11:20:22.590'),

(19,'3','dbo','plaza_route_revenue_month','sql/baocao_tonghop_doanhthu/get_plaza_route_share_month.sql',50000,'month_id','month',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:21:10.803','2025-12-09 11:21:10.803'),

(20,'4','dbo','plaza_route_revenue_quarter','sql/baocao_tonghop_doanhthu/get_plaza_route_share_quarter.sql',50000,'quarter_id','quarter',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:21:29.637','2025-12-09 11:21:29.637'),

(21,'5','dbo','plaza_route_revenue_year','sql/baocao_tonghop_doanhthu/get_plaza_route_share_year.sql',50000,'year_id','year',
 'job_type = 1 --> Daly, 2--> week, 3 -->month, 4-->quarter, 5-->year',
 true,'2025-12-09 11:21:36.883','2025-12-09 11:21:36.883');
