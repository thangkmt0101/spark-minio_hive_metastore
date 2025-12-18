CREATE TABLE IF NOT EXISTS silver.silver.TRANSPORT_TRANSACTION_STAGE (
    -- Business columns
    TRANSPORT_TRANS_ID        BIGINT,
    ETAG_ID                   BIGINT,
    VEHICLE_ID                BIGINT,
    CHECKIN_TOLL_ID           BIGINT,
    CHECKIN_LANE_ID           BIGINT,
    CHECKIN_COMMIT_DATETIME   TIMESTAMP,
    CHECKIN_CHANNEL           BIGINT,
    CHECKIN_PASS              STRING,
    CHECKIN_PASS_REASON_ID    STRING,
    CHECKOUT_TOLL_ID          BIGINT,
    CHECKOUT_LANE_ID          BIGINT,
    CHECKOUT_COMMIT_DATETIME  TIMESTAMP,
    CHECKOUT_CHANNEL          BIGINT,
    CHECKOUT_PASS             STRING,
    CHECKOUT_PASS_REASON_ID   STRING,
    CHARGE_STATUS             STRING,
    CHARGE_TYPE               STRING,
    TOTAL_AMOUNT              BIGINT,
    ACCOUNT_ID                BIGINT,
    ACCOUNT_TRANS_ID          BIGINT,
    CHECKIN_DATETIME          TIMESTAMP,
    CHECKOUT_DATETIME         TIMESTAMP,
    CHECKIN_STATUS            STRING,
    ETAG_NUMBER               STRING,
    REQUEST_ID                BIGINT,
    CHECKIN_PLATE             STRING,
    CHECKIN_PLATE_STATUS      STRING,
    CHECKOUT_PLATE            STRING,
    CHECKOUT_PLATE_STATUS     STRING,
    PLATE_FROM_TOLL           STRING,
    IMG_COUNT                 INT,
    CHECKIN_IMG_COUNT         INT,
    CHECKOUT_IMG_COUNT        INT,
    STATUS                    STRING,
    LAST_UPDATE               TIMESTAMP,
    NOTIF_SCAN                INT,
    TOLL_TYPE                 STRING,
    RATING_TYPE               STRING,
    INSERT_DATETIME           TIMESTAMP,
    INSUFF_AMOUNT             DECIMAL(12,2),
    CHECKOUT_STATUS           STRING,
    BITWISE_SCAN              BIGINT,
    PLATE                     STRING,
    VEHICLE_TYPE              STRING,
    FE_VEHICLE_LENGTH         BIGINT,
    FE_COMMIT_AMOUNT          BIGINT,
    FE_WEIGHT                 BIGINT,
    FE_REASON_ID              BIGINT,
    VEHICLE_TYPE_PROFILE      STRING,
    BOO                       DOUBLE,
    BOO_TRANSPORT_TRANS_ID    BIGINT,
    SUBSCRIPTION_IDS          STRING,
    CHECKIN_SHIFT             STRING,
    CHECKOUT_SHIFT            STRING,
    TURNING_CODE              STRING,
    CHECKIN_TID               STRING,
    CHECKOUT_TID              STRING,
    CHARGE_IN                 STRING,
    CHARGE_TRANS_ID           STRING,
    BALANCE                   DECIMAL(20,2),
    CHARGE_IN_STATUS          STRING,
    CHARGE_DATETIME           TIMESTAMP,
    CHARGE_IN_104             STRING,
    FE_ONLINE_STATUS          STRING,
    MDH_ID                    BIGINT,
    CHD_TYPE                  STRING,
    CHD_REF_ID                BIGINT,
    CHD_REASON                STRING,
    FE_TRANS_ID               STRING,
    TRANSITION_CLOSE          STRING,
    VOUCHER_CODE              STRING,
    VOUCHER_USED_AMOUNT       BIGINT,
    VOUCHER_AMOUNT            BIGINT,
    TRANSPORT_SYNC_ID         BIGINT,
    TICKET_IN_ID              BIGINT,
    HUB_ID                    BIGINT,
    TICKET_ETAG_ID            BIGINT,
    TICKET_OUT_ID             BIGINT,
    TRANSPORT_TRANS_ID_EX     STRING,

    -- Partition columns (physical folders on MinIO)
    year                      STRING,
    month                     STRING,
    day                       STRING,
    hour                      STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour);



CREATE TABLE IF NOT EXISTS silver.silver.etag (
    ETAG_ID               DOUBLE NOT NULL,
    ETAG_TYPE             STRING NOT NULL,
    SERIAL                STRING NOT NULL,
    STATUS                DOUBLE NOT NULL,
    ETAG_NUMBER           STRING,
    SHOP_ID               DOUBLE NOT NULL,
    GOODS_GROUP_ID        DOUBLE NOT NULL,
    CREATE_DATETIME       TIMESTAMP NOT NULL,

    -- Partition Columns
    year                  STRING,
    month                 STRING,
    day                   STRING,
    hour                  STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour);



CREATE TABLE IF NOT EXISTS silver.silver.bot (
    bot_id DOUBLE NOT NULL,
    bot_name STRING,
    bank_id DOUBLE,
    bank_acount STRING,
    status STRING,
    bot_code STRING,
    bank_account_name STRING,

    -- Partition columns (generated on write)
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)


CREATE TABLE IF NOT EXISTS silver.silver.closed_cycle (
    cycle_id BIGINT NOT NULL,
    cycle_code STRING,
    cycle_name STRING,
    status STRING,
    boo STRING,
    close_time_code STRING,
    transition_close STRING,
    parrent_cycle_id BIGINT,
    pi_close STRING,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.price (
    price_id BIGINT,
    price_type STRING,
    vehicle_type STRING,
    toll_id BIGINT,
    create_datetime TIMESTAMP,
    effect_datetime TIMESTAMP,
    end_datetime TIMESTAMP,
    modify_datetime TIMESTAMP,
    status BIGINT,
    price_amount BIGINT,
    note STRING,
    stage_id BIGINT,
    staff_id BIGINT,
    price_ticket_type STRING,
    modify_staff BIGINT,
    vehicle_type_profile STRING,
    turning_code STRING,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.toll (
    toll_id BIGINT NOT NULL,
    toll_name STRING,
    address STRING,
    province STRING,
    district STRING,
    province_new STRING,
    precinct_new STRING,
    precinct STRING,
    toll_type STRING,
    tell_number STRING,
    fax STRING,
    status STRING,
    toll_code STRING,
    toll_name_search STRING,
    status_commercial STRING,
    tcoc_version_id BIGINT,
    allow_same_inout STRING,
    allow_same_inout_time BIGINT,
    latch_hour TIMESTAMP,
    boo STRING,
    vehicle_type_profile STRING,
    calc_type STRING,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.toll_cycle (
    toll_id BIGINT,
    cycle_id BIGINT,
    status STRING,
    order_id INT,
    toll_cycle_id BIGINT NOT NULL,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.toll_lane (
    toll_id BIGINT,
    lane_code BIGINT,
    lane_type STRING,
    status STRING,
    toll_lane_id BIGINT NOT NULL,
    lane_name STRING,
    free_lanes STRING,
    free_lanes_limit INT,
    free_lanes_second STRING,
    free_toll_id BIGINT,
    dup_filter STRING,
    free_limit_time STRING,
    free_allow_loop STRING,
    transit_lane STRING,
    freeflow_lane STRING,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.toll_stage (
    stage_id BIGINT NOT NULL,
    stage_code STRING,
    stage_name STRING,
    cycle_id BIGINT,
    toll_a BIGINT,
    toll_b BIGINT,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    status STRING,
    status_commercial STRING,
    latch_hour TIMESTAMP,
    subscription_paid_for BIGINT,
    bot_a BIGINT,
    bot_b BIGINT,
    boo STRING,

    -- Partition columns
    year  STRING,
    month STRING,
    day   STRING,
    hour  STRING
)
USING iceberg
PARTITIONED BY (
    year,
    month,
    day,
    hour
)

CREATE TABLE IF NOT EXISTS silver.silver.boo_transport_trans_stage (
    transport_trans_id      BIGINT,
    subscriber_id           BIGINT,
    etag_id                 BIGINT,
    vehicle_id              BIGINT,
    checkin_toll_id         BIGINT,
    checkin_lane_id         BIGINT,
    checkin_commit_datetime TIMESTAMP,
    checkin_channel         BIGINT,
    checkin_pass            STRING,
    checkin_pass_reason_id  STRING,
    checkout_toll_id        BIGINT,
    checkout_lane_id        BIGINT,
    checkout_commit_datetime TIMESTAMP,
    checkout_channel        BIGINT,
    checkout_pass           STRING,
    checkout_pass_reason_id STRING,
    charge_status           STRING,
    charge_type             STRING,
    total_amount            BIGINT,
    account_id              BIGINT,
    account_trans_id        BIGINT,
    checkin_datetime        TIMESTAMP,
    checkout_datetime       TIMESTAMP,
    checkin_status          STRING,
    etag_number             STRING,
    request_id              BIGINT,
    checkin_plate           STRING,
    checkin_plate_status    STRING,
    checkout_plate          STRING,
    checkout_plate_status   STRING,
    plate_from_toll         STRING,
    img_count               INT,
    checkin_img_count       INT,
    checkout_img_count      INT,
    status                  STRING,
    last_update             TIMESTAMP,
    notif_scan              INT,
    toll_type               STRING,
    rating_type             STRING,
    insert_datetime         TIMESTAMP,
    insuff_amount           DECIMAL(12,2),
    checkout_status         STRING,
    plate                   STRING,
    bitwise_scan            BIGINT,
    vehicle_type            STRING,
    fe_weight               BIGINT,
    fe_reason_id            BIGINT,
    vehicle_type_profile    STRING,
    boo                     BIGINT,
    boo_vehicle_type        STRING,
    boo_register_vehicle_type STRING,
    boo_seat                BIGINT,
    boo_weight_goods        BIGINT,
    boo_weight_all          BIGINT,
    boo_ticket_id           BIGINT,
    checkin_shift           STRING,
    checkout_shift          STRING,
    turning_code            STRING,
    checkin_tid             STRING,
    checkout_tid            STRING,
    mdh_id                  BIGINT,
    chd_type                STRING,
    chd_ref_id              BIGINT,
    chd_reason              STRING,
    charge_in_status        STRING,
    charge_datetime         TIMESTAMP,
    charge_in_104           STRING,
    fe_online_status        STRING,
    fe_trans_id             STRING,
    transition_close        STRING,
    transport_sync_id       BIGINT,
    ticket_in_id            BIGINT,
    ticket_out_id           BIGINT,
    hub_id                  BIGINT,
    boo_etag                STRING,
    ticket_etag_id          BIGINT,
    map_checkin_toll_id     DOUBLE,
    map_checkin_lane_id     DOUBLE,
    boo_type                STRING,

    -- Partition columns
    year                    STRING,
    month                   STRING,
    day                     STRING,
    hour                    STRING
)
USING iceberg
PARTITIONED BY (year, month, day, hour);

CREATE TABLE IF NOT EXISTS silver.silver.transport_trans_stage_detail (
    transport_trans_id          BIGINT NOT NULL,
    stage_id                    BIGINT NOT NULL,
    price_id                    BIGINT,
    subscription_his_id         BIGINT,
    price_type                  STRING,
    price_amount                BIGINT,
    checkin_datetime            TIMESTAMP,
    price_ticket_type            STRING,
    transport_stage_detail_id   BIGINT,
    mdh_id                      BIGINT,

    -- Partition columns
    year                        INT,
    month                       INT,
    day                         INT,
    hour                        INT
)
USING iceberg
PARTITIONED BY (year, month, day, hour);


CREATE TABLE IF NOT EXISTS silver.silver.dim_date (
    date_id              INT NOT NULL,
    date_name            DATE NOT NULL,

    day                  INT NOT NULL,
    day_of_week          INT NOT NULL,
    day_of_year          INT NOT NULL,

    week_id              STRING NOT NULL,
    week_of_year         INT NOT NULL,
    week_start_date      DATE NOT NULL,
    week_end_date        DATE NOT NULL,

    month_id             STRING NOT NULL,
    month                INT NOT NULL,
    month_name           STRING NOT NULL,
    month_short_name     STRING NOT NULL,
    month_start_date     DATE NOT NULL,
    month_end_date       DATE NOT NULL,

    quarter_id           STRING NOT NULL,
    quarter              INT NOT NULL,
    quarter_name         STRING NOT NULL,
    quarter_start_date   DATE NOT NULL,
    quarter_end_date     DATE NOT NULL,

    year_id              STRING NOT NULL,
    year                 INT NOT NULL
)
USING iceberg