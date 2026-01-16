SELECT 
A.CHECKOUT_COMMIT_DATETIME as date, --Ngày giờ giao dịch
TollTypeId, --Loại trạm
StationId, --Chu trình
TollInId, --Trạm vào
TollOutId, --Trạm ra
BotId, --Bot
TicketTypeId, --Loại vé  
TransactionCode , --Mã giao dịch
TollInDate,  --Thời gian vào trạm
TollOutDate, --Thời gian ra trạm
PlateNumber, --Biển số
Etag,
VehicleType, --Loại phương tiện
TollName, --Tên trạm
TollType, -- Loại trạm
TicketType, --Loại vé  
TicketTypePrice, -- Loại giá vé  
TracfficAcount, -- Số tài khoản giao thông 
Status

FROM ice.gold.fact_transport_transaction_stage A
inner join ice.gold.dim_price B on A.CHECKOUT_TOLL_ID = B.TOLL_ID and A.VEHICLE_TYPE = B.VEHICLE_TYPE --Lấy thông tin vé
left join ice.gold.dim_toll C on A.CHECKIN_TOLL_ID = C.TOLL_ID  -- LẤY THÔNG TIN TRẠM in
left join ice.gold.dim_toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
inner join ice.gold.dim_toll_cycle E on A.CHECKOUT_TOLL_ID = E.TOLL_ID -- LẤY THÔNG TIN TRẠM để có thông tin tuyến
inner join ice.gold.dim_closed_cycle F on E.CYCLE_ID = F.CYCLE_ID -- LẤY THÔNG TIN TUYẾN
INNER JOIN ice.gold.ACCOUNT G on A.ACCOUNT_ID = G.ACCOUNT_ID
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME, A.VEHICLE_TYPE, B.PRICE_TICKET_TYPE, B.PRICE_TYPE, F.CYCLE_NAME, C.TOLL_ID, d.TOLL_ID;