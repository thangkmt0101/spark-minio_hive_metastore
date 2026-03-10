SELECT 
A.CHECKOUT_COMMIT_DATETIME as date, --Ngày giờ giao dịch
TollType ,
ExitTollId ,
ClosedCycleId ,
WbTypeId ,
TransactionCode ,
PlateNumber ,
Etag ,
VehicleType ,
WbTypeName,
ExitTollName ,
ClosedCycleName ,
TimeCheckIn ,
TimeCheckOut ,
TolTall
 

FROM ice.gold.fact_transport_transaction_stage A
left join ice.gold.dim_toll_lane C on A.CHECKIN_LANE_ID = C.TOLL_LANE_ID  -- LẤY THÔNG TIN TRẠM in
left join ice.gold.dim_toll_lane d on A.CHECKOUT_LANE_ID = d.TOLL_LANE_ID -- LẤY THÔNG TIN TRẠM out

WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME,TOLL_NAME, TOLL_TYPE, C.LANE_NAME, d.LANE_NAME, CHARGE_STATUS, BOO;
