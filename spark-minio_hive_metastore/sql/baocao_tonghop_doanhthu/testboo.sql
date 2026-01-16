WITH boo_revenue_raw AS (

-- tính vé lượt
    SELECT
        DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' AS date,
        tts.boo AS boo,

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
        ON A.transport_trans_id = CAST(tts.transport_trans_id AS BIGINT)

    LEFT JOIN ice.gold.dim_share S
        ON A.vehicle_type = S.vehicle_type
       AND A.price_type  = S.price_type
       AND tts.boo       = S.boo
       AND DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'
           BETWEEN S.effective_date AND S.expiry_date

    WHERE A.price_type = 'L'
      AND (
        {% for day_range in day_ranges %}
        (A.year  = '{{ day_range.year }}'
         AND A.month = '{{ day_range.month }}'
         AND A.day   = '{{ day_range.day }}'
         AND CAST(A.hour AS BIGINT)
             BETWEEN '{{ day_range.hour_start }}'
                 AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
      )

    GROUP BY
        tts.boo


    UNION ALL


-- tính vé dài hạn
    SELECT
        DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' AS date,
        A.boo AS boo,

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

    LEFT JOIN ice.gold.dim_share S
        ON A.vehicle_type = S.vehicle_type
       AND A.price_type  = S.price_type
       AND A.boo         = S.boo
       AND DATE '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'
           BETWEEN S.effective_date AND S.expiry_date

    WHERE (
        {% for day_range in day_ranges %}
        (A.year  = '{{ day_range.year }}'
         AND A.month = '{{ day_range.month }}'
         AND A.day   = '{{ day_range.day }}'
         AND CAST(A.hour AS BIGINT)
             BETWEEN '{{ day_range.hour_start }}'
                 AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
      )

    GROUP BY
        A.boo
),

-- tính tổng doanh thu giữa lượt và dài hạn
boo_daily_revenue AS (
    SELECT
        date,
        boo,
        SUM(boo_revenue) AS boo_revenue
    FROM boo_revenue_raw
    GROUP BY
        date,
        boo
)

-- selfjoin để ra cặp boo
SELECT
    '{{year_utc_7}}{{month_utc_7}}{{day_utc_7}}00' AS datetime,

    a.boo         AS boo1,
    a.boo_revenue AS boo1_revenue,

    b.boo         AS boo2,
    b.boo_revenue AS boo2_revenue,

    ABS(a.boo_revenue - b.boo_revenue) AS difference

FROM boo_daily_revenue a
JOIN boo_daily_revenue b
    ON a.date = b.date
   AND a.boo < b.boo

ORDER BY
    boo1,
    boo2;
