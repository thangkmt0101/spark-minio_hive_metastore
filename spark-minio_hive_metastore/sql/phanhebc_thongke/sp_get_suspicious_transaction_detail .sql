SELECT 
A.CHECKOUT_COMMIT_DATETIME as date, --Ngày giờ giao dịch
TOLL_NAME AS TollId, --Tên trạm
TOLL_TYPE AS TollType, --Loại trạm
C.LANE_NAME AS EntryLaneId, --LÀN vào 
d.LANE_NAME AS ExitLaneId, --lÀN ra 
CHARGE_STATUS AS UnusualTransactionType, --Loại giao dịch bất thường(PENDING: Chờ thanh toán, SUCCESS,IMMEDIATE: Thành công, FAILED: Thất bại, ROLL: Bị hủy)
NULL AS TransactionSource, --Nguồn giao dịch
NULL AS ReconciliationRequestSource, --Nguồn yêu cầu đối soát
BOO AS BooId,
NULL AS PostAuditStatus, --Trạng thái hậu kiểm
NULL AS ExplanationStatus, --Trạng thái HS giải trình 
NULL AS AttributionSource, --Nguồn đánh dấu 
NULL AS ReconciliationStatus , --Trạng thái đối soát


NULL AS StationId,
NULL AS StationName,
NULL AS TransactionSourceId, -- Nguồn giao dịch  
NULL AS TransactionSourceName, -- Tên nguồn giao dịch
NULL AS ReconciliationRequestSourceId , -- Nguồn yêu cầu đối soát
NULL AS ReconciliationRequestSourceName, -- Tên nguồn yêu cầu đối soát
NULL AS TransactionTypeId, -- Loại giao dịch
NULL AS TransactionTypeName, -- Tên loại giao dịch

NULL AS RFID,  
PLATE AS LicensePlate,
VEHICLE_TYPE AS VehicleType,
PRICE AS Price,
NULL AS Staus,
NULL AS StatusName,
NULL AS DeductionCount,
NULL AS BooName,
NULL AS Requester,
NULL AS RequestDate,
NULL AS Processor


FROM silver.silver.TRANSPORT_TRANSACTION_STAGE A
left join silver.silver.toll_lane C on A.CHECKIN_LANE_ID = C.TOLL_LANE_ID  -- LẤY THÔNG TIN TRẠM in
left join silver.silver.toll_lane d on A.CHECKOUT_LANE_ID = d.TOLL_LANE_ID -- LẤY THÔNG TIN TRẠM out

WHERE A.year  = '{{ year }}' AND A.month = '{{ month }}' AND A.day   = '{{ day }}'
GROUP BY A.CHECKOUT_COMMIT_DATETIME,TOLL_NAME, TOLL_TYPE, C.LANE_NAME, d.LANE_NAME, CHARGE_STATUS, BOO;
