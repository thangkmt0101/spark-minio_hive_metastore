--Báo cáo tổng hợp

SELECT 
date, VEHICLE_TYPE, PRICE_TICKET_TYPE AS fare_category, 
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
    TOLL_CHANNEL
FROM (
SELECT 
A.CHECKOUT_COMMIT_DATETIME as date,
A.VEHICLE_TYPE,
B.PRICE_TICKET_TYPE,
B.PRICE_TYPE,
F.CYCLE_NAME,
C.TOLL_ID AS ENTRY_PLAZA,
d.TOLL_ID AS EXIT_PLAZA,
'ETC' AS TOLL_CHANNEL,
COUNT(*) AS QUANTITY,
SUM(A.TOTAL_AMOUNT) as TOTAL_AMOUNT
FROM ice.gold.fact_transport_transaction_stage A
inner join ice.gold.dim_price B on A.CHECKOUT_TOLL_ID = B.TOLL_ID and A.VEHICLE_TYPE = B.VEHICLE_TYPE
left join ice.gold.dim_toll C on A.CHECKIN_TOLL_ID = C.TOLL_ID  -- LẤY THÔNG TIN TRẠM in
left join ice.gold.dim_toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
inner join ice.gold.dim_toll_cycle E on A.CHECKOUT_TOLL_ID = E.TOLL_ID -- LẤY THÔNG TIN TRẠM để có thông tin tuyến
inner join ice.gold.dim_closed_cycle F on E.CYCLE_ID = F.CYCLE_ID -- LẤY THÔNG TIN TUYẾN


WHERE (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
GROUP BY A.VEHICLE_TYPE, B.PRICE_TICKET_TYPE, B.PRICE_TYPE, F.CYCLE_NAME, C.TOLL_ID, d.TOLL_ID
) AS subquery
GROUP BY date, VEHICLE_TYPE, PRICE_TICKET_TYPE, CYCLE_NAME, ENTRY_PLAZA, EXIT_PLAZA, TOLL_CHANNEL;