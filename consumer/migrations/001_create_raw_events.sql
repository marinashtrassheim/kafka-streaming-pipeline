CREATE TABLE IF NOT EXISTS cart_events_raw (
    event_id String,
    user_id UInt64,
    session_id String,
    event_type LowCardinality(String),
    product_id UInt32,
    product_name String,
    quantity UInt16,
    price Decimal(10, 2),
    timestamp DateTime64(3),
    processed_at DateTime64(3),
    processing_lag UInt32,
    is_late UInt8,
    hour_of_day UInt8,
    day_of_week UInt8,
    cart_total_before Decimal(10, 2),
    cart_total_after Decimal(10, 2)
)
ENGINE = MergeTree
ORDER BY (event_type, toStartOfMinute(timestamp), user_id)
TTL toDateTime(processed_at) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

ALTER TABLE cart_events_raw
    ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter GRANULARITY 4;

ALTER TABLE cart_events_raw
    ADD INDEX IF NOT EXISTS idx_timestamp timestamp TYPE minmax GRANULARITY 2;
