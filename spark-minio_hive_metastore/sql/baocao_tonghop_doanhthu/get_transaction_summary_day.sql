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
B.VEHICLE_TYPE,
A.PRICE_TICKET_TYPE,
A.PRICE_TYPE,
C.CYCLE_NAME AS CYCLE_NAME,
B.toll_name AS ENTRY_PLAZA,
C.toll_name AS EXIT_PLAZA,
C.checkin_lane_id AS ENTRY_LANE,
C.checkout_lane_id AS EXIT_LANE,
'ETC' AS TOLL_CHANNEL,
COUNT(*) AS QUANTITY,
SUM(A.price_amount) as TOTAL_AMOUNT
from ice.gold.fact_transport_transaction_stage  A
INNER JOIN ice.gold.view_dim_toll_cycle B on B.toll_id = A.checkin_toll_id
INNER JOIN ice.gold.view_dim_toll_cycle C on C.toll_id = A.checkout_toll_id
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  ) and a.price_type = 'L'
group by B.VEHICLE_TYPE, A.PRICE_TICKET_TYPE
,A.PRICE_TYPE, C.CYCLE_NAME , C.CHECKIN_TOLL_NAME, C.CHECKOUT_TOLL_NAME
,C.CHECKIN_LANE_NAME,C.CHECKOUT_LANE_NAME
) AS subquery
GROUP BY datetime_id, date, VEHICLE_TYPE, PRICE_TICKET_TYPE, CYCLE_NAME, ENTRY_PLAZA
, EXIT_PLAZA, TOLL_CHANNEL, ENTRY_LANE, EXIT_LANE;