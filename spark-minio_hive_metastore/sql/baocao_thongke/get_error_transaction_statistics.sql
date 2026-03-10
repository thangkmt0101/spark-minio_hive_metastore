SELECT 
A.CHECKOUT_COMMIT_DATETIME as date, --Ngày giờ giao dịch
C.TOLL_ID AS EntryTollId, --Trạm vào 
d.TOLL_ID AS ExitTollId , --Trạm ra 
CHARGE_STATUS AS TransactionTypeId , --Loại giao dịch (PENDING: Chờ thanh toán, SUCCESS,IMMEDIATE: Thành công, FAILED: Thất bại, ROLL: Bị hủy)
NULL AS TransactionError , --Lỗi giao dịch
ACCOUNT_NUMBER AS AccountCode , --Số tài khoản
NULL AS ServiceId , --Loại dịch vụ 
NULL AS Code , --Mã giao dịch
NULL AS TransactionType   , --Loại giao dịch
NULL AS TransactionTypeName ,
NULL AS TransactionErrorType  , --Loại giao dịch LỖI
NULL AS EntryTollName,
NULL AS ExitTollName,
NULL AS ServiceName 



FROM ice.gold.fact_transport_transaction_stage A
inner join ice.gold.dim_price B on A.CHECKOUT_TOLL_ID = B.TOLL_ID and A.VEHICLE_TYPE = B.VEHICLE_TYPE --Lấy thông tin vé
left join ice.gold.dim_toll C on A.CHECKIN_TOLL_ID = C.TOLL_ID  -- LẤY THÔNG TIN TRẠM in
left join ice.gold.dim_toll d on A.CHECKOUT_TOLL_ID = d.TOLL_ID -- LẤY THÔNG TIN TRẠM out
inner join ice.gold.dim_toll_cycle E on A.CHECKOUT_TOLL_ID = E.TOLL_ID -- LẤY THÔNG TIN TRẠM để có thông tin tuyến
inner join ice.gold.dim_closed_cycle F on E.CYCLE_ID = F.CYCLE_ID -- LẤY THÔNG TIN TUYẾN
INNER JOIN ice.gold.ACCOUNT G on A.ACCOUNT_ID = G.ACCOUNT_ID
WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME, A.VEHICLE_TYPE, B.PRICE_TICKET_TYPE, B.PRICE_TYPE, F.CYCLE_NAME, C.TOLL_ID, d.TOLL_ID;