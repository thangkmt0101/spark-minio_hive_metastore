SELECT transport_trans_id, etag_id, vehicle_id,
checkin_toll_id, checkin_lane_id
,{{year_utc_7}} year
,{{month_utc_7}} month
,{{day_utc_7}} day
FROM ice.gold.fact_transport_transaction_stage
where transport_trans_id in (1964262538,1964262538,1964262740);