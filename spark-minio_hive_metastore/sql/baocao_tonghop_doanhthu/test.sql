WITH revenue_union AS (

    /* ======================================================
       1. VÉ LƯỢT
       ====================================================== */
    SELECT
        C.CYCLE_NAME              AS route,
        D.TOLL_NAME               AS plaza_name,
        'Đầu tư tư nhân'          AS cycle_type,

        SUM(A.price_amount)       AS gross_revenue,
        SUM(
            CASE
                WHEN S.distribution_type = 'PERCENT'
                    THEN A.price_amount * COALESCE(S.boo_split_value, 0) / 100
                WHEN S.distribution_type = 'FIXED'
                    THEN COALESCE(S.boo_split_value, 0)
                ELSE 0
            END
        ) AS boo_revenue

    FROM ice.gold.fact_transport_trans_stage_detail A
    JOIN ice.gold.fact_transport_transaction_stage tts
        ON A.transport_trans_id = CAST(tts.transport_trans_id AS BIGINT) -- đổi kiểu dữ liệu về 1

    LEFT JOIN ice.gold.dim_toll_cycle B
        ON tts.checkout_toll_id = B.toll_id
    LEFT JOIN ice.gold.dim_closed_cycle C
        ON B.CYCLE_ID = C.CYCLE_ID
    JOIN ice.gold.dim_toll D
        ON tts.checkout_toll_id = D.toll_id

    -- MAP TỶ LỆ BOO
    LEFT JOIN ice.gold.dim_share S
        ON A.vehicle_type = S.vehicle_type
        AND A.price_type  = S.price_type
        AND tts.boo = S.boo 
        -- ở đây nên map 1 cái thành dim boo
        AND DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'
            BETWEEN S.effective_date AND S.expiry_date

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


    UNION ALL


    /* ======================================================
       2. VÉ DÀI HẠN
       ====================================================== */
    SELECT
        C.CYCLE_NAME              AS route,
        D.TOLL_NAME               AS plaza_name,
        'Đầu tư tư nhân'          AS cycle_type,

        SUM(A.price_amount)       AS gross_revenue,
        SUM(
            CASE
                WHEN S.distribution_type = 'PERCENT'
                    THEN A.price_amount * COALESCE(S.boo_split_value, 0) / 100
                WHEN S.distribution_type = 'FIXED'
                    THEN COALESCE(S.boo_split_value, 0)
                ELSE 0
            END
        ) AS boo_revenue

    FROM ice.gold.fact_boo_subscription_history A
    JOIN ice.gold.dim_toll_cycle B
        ON A.toll_id = B.TOLL_ID
    JOIN ice.gold.dim_closed_cycle C
        ON B.CYCLE_ID = C.CYCLE_ID
    JOIN ice.gold.dim_toll D
        ON A.toll_id = D.toll_id

    -- MAP TỶ LỆ BOO
    LEFT JOIN ice.gold.dim_share S
        ON A.vehicle_type = S.vehicle_type
        AND A.price_type  = S.price_type
         AND A.boo = S.boo
         -- ở đây nên map 1 cái thành dim boo
        AND DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'
            BETWEEN S.effective_date AND S.expiry_date

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
)
SELECT
    '{{year_utc_7}}-{{month_utc_7}}' AS month_id,
    route,
    plaza_name,
    cycle_type,

    SUM(gross_revenue)                       AS gross_revenue,
    NULL                                    AS investment_amount,
    NULL                                    AS discount_amount,
    NULL                                    AS tax_amount,

    SUM(gross_revenue)                      AS pre_allocation_revenue,
    NULL                                    AS bot1_revenue,
    NULL                                    AS bot2_revenue,

    SUM(boo_revenue)                        AS boo1_revenue,

    NULL                                    AS nsnn_revenue,
    NULL                                    AS dbvn_revenue,
    NULL                                    AS dvvh_revenue

FROM revenue_union
GROUP BY
    route,
    plaza_name,
    cycle_type;
