SELECT 
TTS.transport_trans_id  as transaction_code, 
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' ||  '00' as datetime_id,
'{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' as date,
split(TTS.CHECKOUT_TOLL_NAME, '\\s*-\\s*')[1] AS route,
TTS.CHECKOUT_TOLL_NAME AS plaza_name, -- tên trạm
TTS.etag_number ,
TTS.checkout_plate ,
mo.plate,
TTS.TOTAL_AMOUNT as frontend_amount,
mo.price as backend_amount, 
ABS(TTS.TOTAL_AMOUNT - mo.price ) as difference_amount
FROM ice.gold.fact_transport_transaction_stage TTS
JOIN ice.gold.fact_med_etdr  mo
ON TTS.CHECKOUT_TOLL_NAME = mo.CHECKOUT_TOLL_NAME
-- còn trạng thái hậu kiểm và trạng thái đối soát thì chưa thấy define

WHERE (
        {% for day_range in day_ranges %}
        (TTS.year = '{{ day_range.year }}'
         AND TTS.month = '{{ day_range.month }}'
         AND TTS.day = '{{ day_range.day }}'
         AND TTS.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    )
GROUP BY TTS.transport_trans_id