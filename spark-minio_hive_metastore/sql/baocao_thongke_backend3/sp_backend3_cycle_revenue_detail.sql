SELECT 
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}'|| '00' as datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
    c.cycle_name,
    dap.name AS vehicle_type,
    d.price_type,
    COUNT(distinct transport_trans_id) quantity,
    sum(price_amount) amount
 FROM ice.gold.fact_transport_trans_stage_detail a
 INNER JOIN ice.gold.fact_transport_transaction_stage b ON a.TRANSPORT_TRANS_ID  = b.TRANSPORT_TRANS_ID
 INNER JOIN ice.gold.view_dim_toll_stage_closed c on a.stage_id = c.stage_id
 INNER JOIN ice.gold.dim_price d on a.price_id = d.price_id
 INNER JOIN ice.gold.dim_ap_domain dap on dap.type = 'VEHICLE_TYPE' and dap.code = d.VEHICLE_TYPE
 WHERE 
 (
    {% for day_range in day_ranges %}
    (A.year = '{{ day_range.year }}'
     AND A.month = '{{ day_range.month }}'
     AND A.day = '{{ day_range.day }}'
     AND A.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
  )
  AND B.CHARGE_IN_STATUS in ('Hủy', 'Thất bại') and a.price_type = 'L'

GROUP BY c.cycle_name, dap.name, d.price_type