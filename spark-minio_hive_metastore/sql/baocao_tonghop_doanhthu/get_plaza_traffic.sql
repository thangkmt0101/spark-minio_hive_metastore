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
'{{year_utc_7}}-{{month_utc_7}}' as date,
stage_name AS plaza_name, -- tên trạm
A.VEHICLE_TYPE AS VEHICLE_TYPE_PROFILE,  -- loại phương tiện
COUNT(*) AS data_a_tickets_sold,
SUM(A.price_amount) AS data_a_revenue,
COUNT(*) AS data_a_traffic,
0 AS data_b_tickets_sold,
0 AS data_b_revenue,
0 AS data_b_traffic
FROM ice.gold.fact_transport_trans_stage_detail A
INNER JOIN ice.gold.fact_transport_transaction_stage B 
on a.transport_trans_id = b.transport_trans_id
INNER JOIN ice.gold.dim_toll_stage c
on a.stage_id = c.stage_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}' 
     AND A.month = '{{ day_range.month }}' 
     AND A.day = '{{ day_range.day }}' 
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  ) and a.price_type = 'L'

GROUP BY D.TOLL_NAME, A.VEHICLE_TYPE)A 
GROUP BY date, plaza_name, VEHICLE_TYPE_PROFILE;