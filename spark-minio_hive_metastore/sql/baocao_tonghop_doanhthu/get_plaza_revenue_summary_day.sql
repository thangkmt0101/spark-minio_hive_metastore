-- Tính doanh thu trên các giao dịch - Tổng hợp doanh thu theo trạm


SELECT 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
cycle_name AS route,
d.name AS vehicle_transaction_type, -- loại phương tiện 
A.CHECKOUT_TOLL_NAME AS plaza_name, -- tên trạm
d.VEHICLE_GROUP AS revenue_group,  -- Nhóm phương tiện
COUNT(distinct transport_trans_id) AS transaction_count,
SUM(A.PRICE_AMOUNT) AS gross_revenue,
NULL AS discount_amount,
NULL AS tax_amount, --tạm fix do chưa biết cách tính
NULL AS investment_amount, --tạm fix do chưa biết cách tính
NULL AS net_revenue, --tạm fix do chưa biết cách tính
'Loại phương tiện' AS type,
'Đầu tư công' as cycle_type
FROM ice.gold.fact_transport_trans_stage_detail a
INNER JOIN ice.gold.view_dim_toll_stage_closed b on a.stage_id = b.stage_id
INNER JOIN ice.gold.dim_ap_domain c on c.type = 'VEHICLE_TYPE' and c.code = d.VEHICLE_TYPE
INNER JOIN ice.gold.dim_price d on a.price_id = d.price_id
INNER JOIN ice.gold.dim_vehicle e on a.vehicle_id = e.vehicle_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
GROUP BY cycle_name,d.name,A.CHECKOUT_TOLL_NAME,d.VEHICLE_GROUP
UNION ALL
SELECT 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
cycle_name AS route,
'ETC' AS vehicle_transaction_type, -- loại giao dịch
A.CHECKOUT_TOLL_NAME AS plaza_name, -- tên trạm
d.VEHICLE_GROUP AS revenue_group,  -- Nhóm phương tiện
COUNT(distinct transport_trans_id) AS transaction_count,
SUM(A.PRICE_AMOUNT) AS gross_revenue,
NULL AS discount_amount,
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
GROUP BY cycle_name,A.CHECKOUT_TOLL_NAME,d.VEHICLE_GROUP
