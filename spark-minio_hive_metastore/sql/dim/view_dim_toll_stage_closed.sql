CREATE OR REPLACE VIEW ice.gold.view_dim_toll_stage_closed AS
select  /*+ BROADCAST(d) */
s.stage_id, stage_code, stage_name, cycle_id, 
s.toll_a,ta.toll_name as checkin_toll_name, s.toll_b,
tb.toll_name as checkout_toll_name, start_date, end_date, s.status, s.status_commercial, 
s.latch_hour, subscription_paid_for, bot_a, bot_b, s.boo, s.scd_valid_from,
s.scd_valid_to, s.is_active, s.processing_timestamp
FROM ice.gold.dim_toll_stage s
LEFT JOIN ice.gold.dim_toll ta ON ta.toll_id = s.toll_a
LEFT JOIN ice.gold.dim_toll tb ON tb.toll_id = s.toll_b;
