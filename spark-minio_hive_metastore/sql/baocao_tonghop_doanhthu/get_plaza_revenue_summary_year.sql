-- Tính doanh thu trên các giao dịch - Tổng hợp doanh thu theo trạm


SELECT 
A.year as year_id,
C.CYCLE_NAME AS route, -- tuyến
D.TOLL_NAME AS plaza_name, -- tên trạm
A.VEHICLE_TYPE AS revenue_group,  -- loại phương tiện
COUNT(*) AS transaction_count,
SUM(A.TOTAL_AMOUNT) AS gross_revenue,
SUM(A.VOUCHER_USED_AMOUNT ) AS discount_amount,
NULL AS tax_amount, --tạm fix do chưa biết cách tính
NULL AS investment_amount, --tạm fix do chưa biết cách tính
NULL AS net_revenue --tạm fix do chưa biết cách tính
FROM silver.silver.TRANSPORT_TRANSACTION_STAGE A
INNER JOIN silver.silver.toll_cycle B on A.CHECKOUT_TOLL_ID = B.TOLL_ID -- LẤY THÔNG TIN TRẠM
INNER JOIN silver.silver.closed_cycle C on B.CYCLE_ID = C.CYCLE_ID -- LẤY THÔNG TIN TUYẾN
INNER JOIN silver.silver.toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
WHERE A.year  = '{{ year }}' 
GROUP BY A.year, A.CHECKOUT_TOLL_ID, A.VEHICLE_TYPE, C.CYCLE_NAME, D.TOLL_NAME;