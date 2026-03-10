SELECT 
A.CHECKOUT_COMMIT_DATETIME as date, --Ngày giờ giao dịch
TOLL_NAME AS TollId, --Tên trạm
TOLL_TYPE AS TollType, --Loại trạm
C.LANE_NAME AS EntryLaneId, --LÀN vào 
d.LANE_NAME AS ExitLaneId, --lÀN ra 
CHARGE_STATUS AS TransactionTypeId, --Loại giao dịch (PENDING: Chờ thanh toán, SUCCESS,IMMEDIATE: Thành công, FAILED: Thất bại, ROLL: Bị hủy)
NULL AS TransactionSource, --Nguồn giao dịch
NULL AS ReconciliationRequestSource, --Nguồn yêu cầu đối soát
NULL AS Status,
BOO AS BooId,
NULL AS StationId,
NULL AS StationName,
NULL AS TransactionSourceName,
NULL AS ReconciliationRequestSourceName,
COUNT(*) AS Quantity,
NULL AS Total
FROM ice.gold.fact_transport_transaction_stage A
left join ice.gold.dim_toll_lane C on A.CHECKIN_LANE_ID = C.TOLL_LANE_ID  -- LẤY THÔNG TIN TRẠM in
left join ice.gold.dim_toll_lane d on A.CHECKOUT_LANE_ID = d.TOLL_LANE_ID -- LẤY THÔNG TIN TRẠM out

WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME,TOLL_NAME, TOLL_TYPE, C.LANE_NAME, d.LANE_NAME, CHARGE_STATUS, BOO;
