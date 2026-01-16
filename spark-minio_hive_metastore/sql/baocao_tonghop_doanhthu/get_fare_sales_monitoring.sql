WITH ticket_union AS (
    -- vé lượt
    SELECT
        'Vé lượt'                    AS price_name,
        tts.vehicle_type               AS vehicle_type,
        A.price_amount               AS price_amount,
		TTS.CHECKOUT_TOLL_NAME AS plaza_name, -- tên trạm
        COUNT(DISTINCT A.transport_trans_id) AS ticket_count
    FROM ice.gold.fact_transport_trans_stage_detail A
    JOIN ice.gold.fact_transport_transaction_stage tts
        ON A.transport_trans_id = CAST(tts.transport_trans_id AS BIGINT)
    WHERE(
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
        AND A.month = '{{ day_range.month }}'
        AND A.day = '{{ day_range.day }}'
        AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
        )
        AND A.price_type = 'L'
    

    GROUP BY
        tts.vehicle_type,
        A.price_amount,
        TTS.CHECKOUT_TOLL_NAME


    UNION ALL

    SELECT
        CASE
            WHEN A.price_type = 'M' THEN 'Vé tháng'
            WHEN A.price_type = 'Q' THEN 'Vé quý'
            WHEN A.price_type = 'N' THEN 'Vé Năm'
            ELSE then 'Lỗi , không tìm thấy loại vé'
        END                           AS price_name,
        A.vehicle_type               AS vehicle_type,
        A.price_amount               AS price_amount,
        B.toll_name 				as plaza_name,
        COUNT(A.subscription_hist_id ) AS ticket_count
    FROM ice.gold.fact_boo_subscription_history A
	join ice.gold.dim_toll B on A.toll_out_id = B.toll_id 
    WHERE(
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
        AND A.month = '{{ day_range.month }}'
        AND A.day = '{{ day_range.day }}'
        AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    )
    GROUP BY
        A.price_type,
        A.vehicle_type,
        A.price_amount,
        B.toll_name
)

SELECT
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
    price_name,
    vehicle_type,
    price_amount,
    plaza_name,
    SUM(ticket_count)                     AS ticket_count,
    SUM(ticket_count * price_amount)      AS revenue
FROM ticket_union
GROUP BY
    price_name,
    vehicle_type,
    price_amount,
    plaza_name
ORDER BY
    price_name,
    vehicle_type;
