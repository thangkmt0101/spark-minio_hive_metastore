SELECT * 
FROM ice.gold.fact_transport_trans_stage_detail a
INNER JOIN ice.gold.fact_transport_transaction_stage b 
ON a.transport_trans_id  = b.transport_trans_id
INNER JOIN ice.gold.view_dim_toll_stage_closed c
on a.stage_id = c.stage_id