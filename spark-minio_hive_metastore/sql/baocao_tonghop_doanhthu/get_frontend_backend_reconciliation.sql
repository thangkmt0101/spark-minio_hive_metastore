-- bảng MEDIATION_OWNER"."MED_OTDR"  lưu dữ liệu đối soát từ trạm đẩy lên(đẩy lên qua file)
 
SELECT 

FE_TRANS_ID as transaction_code,
A.CHECKOUT_COMMIT_DATETIME as date,
C.CYCLE_NAME AS route, -- tuyến
D.TOLL_NAME AS plaza_name, -- tên trạm
SUM(A.TOTAL_AMOUNT) AS frontend_amount, 
SUM(A.TOTAL_AMOUNT ) AS backend_amount, --tạm fix do chưa biết cách tính
SUM(A.TOTAL_AMOUNT) AS difference_amount, --tạm fix do chưa biết cách tính
1 as processing_status
FROM ice.gold.fact_transport_transaction_stage A
INNER JOIN ice.gold.dim_toll_cycle B on A.CHECKOUT_TOLL_ID = B.TOLL_ID -- LẤY THÔNG TIN TRẠM
INNER JOIN ice.gold.dim_closed_cycle C on B.CYCLE_ID = C.CYCLE_ID -- LẤY THÔNG TIN TUYẾN
INNER JOIN ice.gold.dim_toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.FE_TRANS_ID, A.CHECKOUT_COMMIT_DATETIME, C.CYCLE_NAME, D.TOLL_NAME;
