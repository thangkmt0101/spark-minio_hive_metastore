
SELECT
    '{{year_utc_7}}{{month_utc_7}}{{day_utc_7}}00' AS datetime_id,
    '{{year_utc_7}}-{{month_utc_7}}-{{day_utc_7}}' AS date,
    vtt.checkout_commit_datetime as pass_time,
    vtt.transport_trans_id                                  AS transaction_code,
    E.toll_name                                             AS plaza_name,
    vtt.channel                                             AS transaction_type,
    vtt.plate_from_toll                                    AS license_plate,

    tts.vehicle_type                                       AS collected_vehicle_type,
    tts.total_amount                                       AS collected_amount,

    -- dữ liệu đối soát
    vtt.vehicle_type                                       AS reconciled_vehicle_type,
    vtt.price_amount                                       AS reconciled_collected_amount,

    -- reason
    D.irregular_rule_name                                   AS surcharge_reason,     


    -- differ
    ABS(tts.total_amount - vtt.price_amount)                  AS amount_difference

FROM vio_transport_transaction vtt
JOIN transport_transaction_stage tts
    ON vtt.transport_trans_id = tts.transport_trans_id
LEFT JOIN fact_irregular_rule D
    ON vtt.irregular_rule_id = D.irregular_rule_id
JOIN ice.gold.dim_toll E
    ON vtt.toll_id = E.toll_id
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
