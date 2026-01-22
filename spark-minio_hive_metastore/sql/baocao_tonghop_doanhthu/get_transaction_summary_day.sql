--Báo cáo tổng hợp

SELECT 
datetime_id,date, VEHICLE_TYPE, PRICE_TICKET_TYPE AS fare_category, 
 -- Vé lượt
    SUM(CASE WHEN PRICE_TYPE = 'L' THEN QUANTITY END)        AS single_pass_count,
    SUM(CASE WHEN PRICE_TYPE = 'L' THEN TOTAL_AMOUNT END)    AS single_pass_revenue,

    -- Vé tháng
    SUM(CASE WHEN PRICE_TYPE = 'T' THEN QUANTITY END)       AS monthly_pass_count,
    SUM(CASE WHEN PRICE_TYPE = 'T' THEN TOTAL_AMOUNT END)   AS monthly_pass_revenue,

    -- Vé quý
    SUM(CASE WHEN PRICE_TYPE = 'Q' THEN QUANTITY END)         AS quarterly_pass_count,
    SUM(CASE WHEN PRICE_TYPE = 'Q' THEN TOTAL_AMOUNT END)     AS quarterly_pass_revenue,
      -- Vé nam
    SUM(CASE WHEN PRICE_TYPE = 'N' THEN QUANTITY END)         AS annual_pass_count,
    SUM(CASE WHEN PRICE_TYPE = 'N' THEN TOTAL_AMOUNT END)     AS annual_pass_revenue,
    -- Tổng
    SUM(QUANTITY) as total_pass_count,
    SUM(TOTAL_AMOUNT) as total_revenue,
    CYCLE_NAME AS route,
    ENTRY_PLAZA,
    EXIT_PLAZA,
    TOLL_CHANNEL,
    ENTRY_LANE,
    EXIT_LANE
FROM (
SELECT
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
d.VEHICLE_TYPE,
c.name as PRICE_TICKET_TYPE,
a.PRICE_TYPE,
b.CYCLE_NAME AS CYCLE_NAME,
b.checkin_toll_name AS ENTRY_PLAZA,
b.checkout_toll_name AS EXIT_PLAZA,
b.checkin_lane_name AS ENTRY_LANE,
b.checkout_lane_name AS EXIT_LANE,
'ETC' AS TOLL_CHANNEL,
COUNT(distinct transport_trans_id ) AS QUANTITY,
SUM(A.PRICE_AMOUNT) as TOTAL_AMOUNT
FROM ice.gold.fact_transport_trans_stage_detail a
INNER JOIN ice.gold.view_dim_toll_stage_closed b on a.stage_id = b.stage_id
INNER JOIN ice.gold.dim_ap_domain c on c.type = 'PRICE_TICKET_TYPE' and c.code = a.price_ticket_type
INNER JOIN ice.gold.dim_price d on a.price_id = d.price_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  ) and a.price_type = 'L'
group by d.VEHICLE_TYPE, c.name
,A.PRICE_TYPE, b.CYCLE_NAME , b.CHECKIN_TOLL_NAME, b.CHECKOUT_TOLL_NAME
,b.CHECKIN_LANE_NAME,b.CHECKOUT_LANE_NAME
) AS subquery
GROUP BY datetime_id, date, VEHICLE_TYPE, PRICE_TICKET_TYPE, CYCLE_NAME, ENTRY_PLAZA
, EXIT_PLAZA, TOLL_CHANNEL, ENTRY_LANE, EXIT_LANE;