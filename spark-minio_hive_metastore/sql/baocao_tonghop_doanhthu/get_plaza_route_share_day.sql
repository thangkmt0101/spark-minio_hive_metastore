-- Tính toán doanh thu - Tỷ lệ %
WITH plaza_route_revenue AS (
    --1. Vé lượt (price_type = 'L') -- đầu tư tư nhân
    SELECT
        '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id
        ,'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
        ,cycle_name AS route       -- tuyến
        ,A.CHECKOUT_TOLL_NAME AS plaza_name   -- trạm
        ,'Đầu tư tư nhân'   AS cycle_type
        ,SUM(A.price_amount) AS gross_revenue -- Tổng doanh thu
        ,NULL AS investment_amount -- Tổng đối trừ
        ,NULL AS discount_amount -- Tổng giảm giá
        ,NULL AS tax_amount -- Tổng thuế
        ,NULL AS pre_allocation_revenue -- Tổng doanh thu trc chia
        ,NULL AS bot1_revenue -- Tổng doanh thu bot1
        ,NULL AS boo1_revenue -- Tổng doanh thu boo1
        ,NULL AS nsnn_revenue -- Phần NSNN
        ,NULL AS dbvn_revenue -- Phần ĐBVN
        ,NULL AS dvvh_revenue -- Phần ĐVVH
    FROM ice.gold.fact_transport_trans_stage_detail a
    INNER JOIN ice.gold.view_dim_toll_stage_closed b on a.stage_id = b.stage_id
    WHERE (
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
        AND A.month = '{{ day_range.month }}'
        AND A.day = '{{ day_range.day }}'
        AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    ) and a.price_type = 'L'
    GROUP BY cycle_name, A.CHECKOUT_TOLL_NAME
    UNION ALL
     --1. Vé lượt (price_type = 'L') -- đầu tư công
    SELECT
        '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id
        ,'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date
        ,cycle_name AS route       -- tuyến
        ,A.CHECKOUT_TOLL_NAME AS plaza_name   -- trạm
        ,'Đầu tư công'   AS cycle_type
        ,SUM(A.price_amount) AS gross_revenue -- Tổng doanh thu
        ,NULL AS investment_amount -- Tổng đối trừ
        ,NULL AS discount_amount -- Tổng giảm giá
        ,NULL AS tax_amount -- Tổng thuế
        ,NULL AS pre_allocation_revenue -- Tổng doanh thu trc chia
        ,NULL AS bot1_revenue -- Tổng doanh thu bot1
        ,NULL AS boo1_revenue -- Tổng doanh thu boo1
        ,NULL AS nsnn_revenue -- Phần NSNN
        ,NULL AS dbvn_revenue -- Phần ĐBVN
        ,NULL AS dvvh_revenue -- Phần ĐVVH
    FROM ice.gold.fact_transport_trans_stage_detail a
    INNER JOIN ice.gold.view_dim_toll_stage_closed b on a.stage_id = b.stage_id
    WHERE (
        {% for day_range in day_ranges %}
        (A.year = '{{ day_range.year }}'
        AND A.month = '{{ day_range.month }}'
        AND A.day = '{{ day_range.day }}'
        AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    ) and a.price_type = 'L'
    GROUP BY cycle_name, A.CHECKOUT_TOLL_NAME
)

SELECT datetime_id,date,route,plaza_name,cycle_type,gross_revenue,investment_amount
      ,discount_amount,tax_amount,pre_allocation_revenue,bot1_revenue,boo1_revenue
      ,nsnn_revenue,dbvn_revenue,dvvh_revenue
FROM plaza_route_revenue

