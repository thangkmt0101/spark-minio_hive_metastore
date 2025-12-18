
SELECT 

FE_TRANS_ID as transaction_code,
D.TOLL_NAME AS plaza_name, -- tên trạm
A.CHECKOUT_DATETIME AS pass_time,
A.CHECKOUT_COMMIT_DATETIME AS date,
A.PLATE AS license_plate,
'ETC' AS transaction_type,
A.VEHICLE_TYPE AS collected_vehicle_type,
SUM(A.TOTAL_AMOUNT) AS collected_amount, 
A.VEHICLE_TYPE AS reconciled_vehicle_type,
SUM(A.TOTAL_AMOUNT) AS reconciled_amount,
NULL as surcharge_reason,
NULL as amount_difference -- SUM(A.TOTAL_AMOUNT) - SUM(A.TOTAL_AMOUNT)
FROM silver.silver.TRANSPORT_TRANSACTION_STAGE A
INNER JOIN silver.silver.toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY FE_TRANS_ID, CHECKOUT_DATETIME, CHECKOUT_COMMIT_DATETIME, PLATE, VEHICLE_TYPE, TOLL_NAME;
