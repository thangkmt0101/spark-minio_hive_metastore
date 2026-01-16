-- Tính doanh thu trên các giao dịch - Tổng hợp doanh thu theo tuyến

 

SELECT 
'{{year_utc_7}}-{{month_utc_7}}' as month_id,
split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS route,
CASE 
        WHEN A.VEHICLE_TYPE = '1' THEN 'Xe < 12 chỗ, xe tải < 2 tấn; xe buýt công cộng'
        WHEN A.VEHICLE_TYPE = '2' THEN 'Xe 12-30 chỗ; xe tải 2 đến <4 tấn'
        WHEN A.VEHICLE_TYPE = '3' THEN 'Xe >= 31 chỗ; xe tải 4 đến <10 tấn'
        WHEN A.VEHICLE_TYPE = '4' THEN 'Xe tải 10 đến <18 tấn; xe Container 20 fit'
        WHEN A.VEHICLE_TYPE = '5' THEN 'Xe tải >= 18 tấn; xe Container 40 fit'
        ELSE 'Lỗi, chưa có mô tả'
END AS vehicle_transaction_type, -- loại phương tiện
B.VEHICLE_GROUP AS revenue_group,
COUNT(*) AS transaction_count,
SUM(A.TOTAL_AMOUNT) AS gross_revenue,
SUM(A.VOUCHER_USED_AMOUNT ) AS discount_amount,
NULL AS tax_amount, --tạm fix do chưa biết cách tính
NULL AS investment_amount, --tạm fix do chưa biết cách tính
NULL AS net_revenue, --tạm fix do chưa biết cách tính
'Loại phương tiện' AS type,
'Đầu tư công' as cycle_type
FROM ice.gold.fact_transport_transaction_stage A
inner join ice.gold.dim_vehicle B on A.vehicle_id = B.vehicle_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
GROUP BY A.VEHICLE_TYPE, B.VEHICLE_GROUP,split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1]
UNION ALL
SELECT 
'{{year_utc_7}}-{{month_utc_7}}' as month_id,
split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS route,
'ETC' AS vehicle_transaction_type, -- loại giao dịch
B.VEHICLE_GROUP AS revenue_group,
COUNT(*) AS transaction_count,
SUM(A.TOTAL_AMOUNT) AS gross_revenue,
SUM(A.VOUCHER_USED_AMOUNT ) AS discount_amount,
NULL AS tax_amount, --tạm fix do chưa biết cách tính
NULL AS investment_amount, --tạm fix do chưa biết cách tính
NULL AS net_revenue, --tạm fix do chưa biết cách tính
'Loại giao dịch' AS type,
'Đầu tư công' as cycle_type
FROM ice.gold.fact_transport_transaction_stage A
inner join ice.gold.dim_vehicle B on A.vehicle_id = B.vehicle_id

WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
GROUP BY A.checkout_channel, B.VEHICLE_GROUP,split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1];