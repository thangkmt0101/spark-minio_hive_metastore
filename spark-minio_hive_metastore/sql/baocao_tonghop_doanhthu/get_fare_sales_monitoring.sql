WITH ticket_union AS (
    -- vé lượt
    SELECT
        'Vé lượt' AS price_name,
        c.name  AS vehicle_type,
        a.price_amount  AS price_amount,
		b.checkout_toll_name AS plaza_name, -- tên trạm
        COUNT(DISTINCT A.transport_trans_id) AS ticket_count,
        SUM(A.PRICE_AMOUNT) AS PRICE_AMOUNT

    FROM ice.gold.fact_transport_trans_stage_detail a
    INNER JOIN ice.gold.view_dim_toll_stage_closed b on a.stage_id = b.stage_id
    INNER JOIN ice.gold.dim_ap_domain c on c.type = 'VEHICLE_TYPE' and c.code = d.VEHICLE_TYPE
    INNER JOIN ice.gold.dim_price d on a.price_id = d.price_id
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
        c.name,
        a.price_amount,
        b.checkout_toll_name

 
)

SELECT
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
    price_name,
    vehicle_type,
    price_amount,
    plaza_name,
    ticket_count AS ticket_count,
    PRICE_AMOUNT AS revenue
FROM ticket_union
 
