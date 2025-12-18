-- Tính doanh thu trên các giao dịch - Tổng hợp doanh thu theo trạm


SELECT 
    date, plaza_name,
    SUM(data_a_tickets_sold) as data_a_tickets_sold,
    SUM(data_a_revenue) as data_a_revenue,
    SUM(data_a_traffic) as data_a_traffic,
    SUM(data_b_tickets_sold) as data_b_tickets_sold,
    SUM(data_b_revenue) as data_b_revenue,
    SUM(data_b_traffic) as data_b_traffic,
    SUM(data_a_tickets_sold) - SUM(data_b_tickets_sold) as total_diff_tickets_sold,
    SUM(data_a_revenue) - SUM(data_b_revenue) as total_diff_revenue,
    SUM(data_a_traffic) - SUM(data_b_traffic) as total_diff_traffic,
    VEHICLE_TYPE_PROFILE
FROM (
SELECT 
A.CHECKOUT_COMMIT_DATETIME as date,
D.TOLL_NAME AS plaza_name, -- tên trạm
A.VEHICLE_TYPE AS VEHICLE_TYPE_PROFILE,  -- loại phương tiện
COUNT(*) AS data_a_tickets_sold,
SUM(A.TOTAL_AMOUNT) AS data_a_revenue,
NULL AS data_a_traffic,

NULL AS data_b_tickets_sold,
NULL AS data_b_revenue,
NULL AS data_b_traffic

FROM silver.silver.TRANSPORT_TRANSACTION_STAGE A
INNER JOIN silver.silver.toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME, D.TOLL_NAME, A.VEHICLE_TYPE)A 
GROUP BY date, plaza_name, VEHICLE_TYPE_PROFILE;