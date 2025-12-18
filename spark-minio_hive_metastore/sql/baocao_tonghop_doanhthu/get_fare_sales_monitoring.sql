-- Tính doanh thu trên các giao dịch - Tổng hợp doanh thu theo trạm


SELECT 
C.TOLL_NAME AS plaza_name, -- tuyến
D.PRICE_TYPE AS ticket_name,
A.VEHICLE_TYPE AS ticket_category,
A.CHECKOUT_COMMIT_DATETIME as date,
MAX(D.PRICE_AMOUNT) AS face_value,
COUNT(*) AS ticket_quantity,
SUM(A.TOTAL_AMOUNT) AS total_amount
FROM silver.silver.TRANSPORT_TRANSACTION_STAGE A
INNER JOIN silver.silver.toll C on A.CHECKOUT_TOLL_ID = C.TOLL_ID -- LẤY THÔNG TIN TRẠM
INNER JOIN silver.silver.price D ON A.CHECKOUT_TOLL_ID = D.TOLL_ID and A.VEHICLE_TYPE = D.VEHICLE_TYPE -- lay thong tin ve

WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY C.TOLL_NAME, D.PRICE_TYPE, A.VEHICLE_TYPE, A.CHECKOUT_COMMIT_DATETIME;