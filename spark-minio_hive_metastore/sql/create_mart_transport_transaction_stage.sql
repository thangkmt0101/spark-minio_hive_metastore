CREATE TABLE IF NOT EXISTS ice.mart.transport_transaction_stage (
    -- Business columns
    transport_trans_id    BIGINT,
    etag_id               BIGINT,
    vehicle_id            BIGINT,
    checkin_toll_id       BIGINT,
    checkin_lane_id       BIGINT,
    
    -- Partition columns
    year                  VARCHAR,
    month                 VARCHAR,
    day                   VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month', 'day'],
    location = 's3a://csdlgt/mart/transport_transaction_stage'
);