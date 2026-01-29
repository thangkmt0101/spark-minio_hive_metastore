
SELECT
    '{{year_utc_7}}{{month_utc_7}}{{day_utc_7}}00' AS datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' AS date,
    tts.CHECKOUT_DATETIME as pass_time,
    vtt.transport_trans_id                                  AS transaction_code,
    dt.toll_name                                             AS plaza_name,
    tts.CHECKOUT_CHANNEL                                             AS transaction_type,
    tts.PLATE                                  AS license_plate,

    tts.vehicle_type                                       AS collected_vehicle_type,
    tts.total_amount                                       AS collected_amount,

    -- dữ liệu đối soát
    vtt.vehicle_type                                       AS reconciled_vehicle_type,
    vtt.price_amount                                       AS reconciled_collected_amount,

    -- reason
    null                                   AS surcharge_reason,     


    -- differ
    ABS(tts.total_amount - vtt.price_amount)                  AS amount_difference

FROM vio_transport_transaction vtt
JOIN transport_transaction_stage tts ON vtt.transport_trans_id = tts.transport_trans_id
JOIN ice.gold.dim_toll dt ON tts.CHECKOUT_TOLL_ID = dt.TOLL_ID
WHERE vtt.status = 'CẦN TRUY THU'
AND (
        {% for day_range in day_ranges %}
        (TTS.year = '{{ day_range.year }}'
         AND TTS.month = '{{ day_range.month }}'
         AND TTS.day = '{{ day_range.day }}'
         AND TTS.hour BETWEEN '{{ day_range.hour_start }}' AND '{{ day_range.hour_end }}')
        {% if not loop.last %} OR {% endif %}
        {% endfor %}
    );
