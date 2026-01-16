
SELECT 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
stage_name as toll_name, -- tam lay theo đoạn tuyến 
split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS cycle_name,
'Đầu tư công' as cycle_type,
D.VEHICLE_GROUP AS revenue_group,  -- Nhóm phương tiện
a.PRICE_TYPE as ticket_type,
COUNT(*) AS transaction_count,
SUM(A.TOTAL_AMOUNT) AS gross_revenue,
SUM(A.VOUCHER_USED_AMOUNT ) AS discount_amount,
NULL AS tax_amount, --tạm fix do chưa biết cách tính
NULL AS investment_amount, --tạm fix do chưa biết cách tính
NULL AS net_revenue, --tạm fix do chưa biết cách tính
'Loại phương tiện' AS type,

FROM ice.gold.fact_transport_trans_stage_detail A
INNER JOIN ice.gold.fact_transport_transaction_stage B 
on a.transport_trans_id = b.transport_trans_id and a.year = b.year AND a.month = b.month AND a.day = b.DAY AND a.hour = b.hour
INNER JOIN ice.gold.dim_toll_stage c
on a.stage_id = c.stage_id
INNER JOIN ice.gold.dim_vehicle D
on b.vehicle_id = d.vehicle_id

WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
GROUP BY A.VEHICLE_TYPE, A.CHECKOUT_TOLL_NAME, B.VEHICLE_GROUP