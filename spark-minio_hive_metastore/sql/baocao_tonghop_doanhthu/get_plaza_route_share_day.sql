-- Tính toán doanh thu
WITH revenue_union AS (


    --1. Vé lượt (price_type = 'L')
    SELECT
        split(A.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS route,       -- tuyến
        D.TOLL_NAME         AS plaza_name,   -- trạm
        'Đầu tư tư nhân'   AS cycle_type,
        SUM(A.price_amount) AS gross_revenue
    FROM ice.gold.fact_transport_trans_stage_detail  A
    join ice.gold.fact_transport_transaction_stage tts 
    on A.transport_trans_id  = cast(tts.transport_trans_id as bigint)
    LEFT JOIN ice.gold.dim_toll_cycle B1
        ON tts.checkin_toll_id  = B1.toll_id 
    LEFT join ice.gold.dim_toll_cycle B2
    	  ON tts.checkout_toll_id  = B2.toll_id 
    LEFT JOIN ice.gold.dim_closed_cycle C
        ON B1.CYCLE_ID = C.CYCLE_ID
    JOIN ice.gold.dim_toll D
        ON tts.CHECKOUT_TOLL_ID = D.TOLL_ID
    WHERE (
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
         AND A.month = '{{ day_range.month }}'
         AND A.day = '{{ day_range.day }}'
         AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
      )
    GROUP BY
        C.CYCLE_NAME,
        D.TOLL_NAME

    UNION ALL
      --2. Vé dài hạn (tính tại thời điểm mua)
    SELECT
        C.CYCLE_NAME        AS route,
        D.TOLL_NAME         AS plaza_name, -- tuyến
        'Đầu tư tư nhân'   AS cycle_type, -- trạm
        SUM(A.price_amount) AS gross_revenue
    FROM ice.gold.fact_boo_subscription_history A
    JOIN ice.gold.dim_toll_cycle B
        ON A.toll_id = B.TOLL_ID
    JOIN ice.gold.dim_closed_cycle C
        ON B.CYCLE_ID = C.CYCLE_ID
    JOIN ice.gold.dim_toll D
        ON A.toll_in_id  = D.toll_id 
    JOIN ice.gold.dim_toll F
    	  ON A.toll_out_id  = F.toll_id 
    WHERE A.price_type = 'L'
      AND (
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
         AND A.month = '{{ day_range.month }}'
         AND A.day = '{{ day_range.day }}'
         AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
      )
    GROUP BY
        C.CYCLE_NAME,
        D.TOLL_NAME
)

SELECT
    '{{year_utc_7}}{{month_utc_7}}{{day_utc_7}}00' AS datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' AS date,
    route,
    plaza_name,
    cycle_type,
    SUM(gross_revenue)      AS gross_revenue,

    NULL AS investment_amount,
    NULL AS discount_amount,
    NULL AS tax_amount,
    NULL AS pre_allocation_revenue,
    NULL AS bot1_revenue,
    NULL AS bot2_revenue,
    NULL AS boo1_revenue, -- tính 2 cách : nhân tỉ lệ % hoặc chia cố định vd 1000đ/1 xe
    NULL AS nsnn_revenue,
    NULL AS dbvn_revenue,
    NULL AS dvvh_revenue
FROM revenue_union
GROUP BY
    route,
    plaza_name,
    cycle_type;
