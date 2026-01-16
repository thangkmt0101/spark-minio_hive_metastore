--Báo cáo tổng hợp

SELECT 
year_id, VEHICLE_TYPE, PRICE_TICKET_TYPE AS fare_category, 
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
'{{year_utc_7}}' as year_id,
A.VEHICLE_TYPE,
B.PRICE_TICKET_TYPE,
B.PRICE_TYPE,
split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS CYCLE_NAME,
A.CHECKIN_TOLL_NAME AS ENTRY_PLAZA,
A.CHECKOUT_TOLL_NAME AS EXIT_PLAZA,
A.CHECKIN_LANE_NAME AS ENTRY_LANE,
A.CHECKOUT_LANE_NAME AS EXIT_LANE,
'ETC' AS TOLL_CHANNEL,
COUNT(*) AS QUANTITY,
SUM(A.TOTAL_AMOUNT) as TOTAL_AMOUNT
from ice.gold.fact_transport_transaction_stage A
inner join ice.gold.fact_transport_trans_stage_detail B on CAST(A.TRANSPORT_TRANS_ID AS BIGINT)= B.TRANSPORT_TRANS_ID
WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
group by A.VEHICLE_TYPE, B.PRICE_TICKET_TYPE
,B.PRICE_TYPE,split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1], A.CHECKIN_TOLL_NAME, A.CHECKOUT_TOLL_NAME
,A.CHECKIN_LANE_NAME,A.CHECKOUT_LANE_NAME
) AS subquery
GROUP BY year_id, VEHICLE_TYPE, PRICE_TICKET_TYPE, CYCLE_NAME, ENTRY_PLAZA
, EXIT_PLAZA, TOLL_CHANNEL, ENTRY_LANE, EXIT_LANE;