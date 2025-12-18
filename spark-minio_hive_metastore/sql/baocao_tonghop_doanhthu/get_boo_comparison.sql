SELECT  
A.CHECKOUT_COMMIT_DATETIME as date,
BOO_ETAG as boo1_name,
SUM(TOTAL_AMOUNT) as boo1_revenue,
NULL as boo2_name,
NULL as boo2_revenue,
NULL as difference_amount,
NULL as  processing_status--SUM(CASE WHEN SUM(TOTAL_AMOUNT) - SUM(TOTAL_AMOUNT) >0 THEN 0 ELSE 1 END) 

FROM silver.silver.boo_transport_trans_stage A
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME, BOO_ETAG;