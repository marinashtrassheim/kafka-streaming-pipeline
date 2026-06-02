-- ClickHouse migration: Materialized views for real-time aggregations
-- Version: 002
-- Depends on: 001_create_raw_events.sql

DROP VIEW IF EXISTS cart_events_1min;

CREATE MATERIALIZED VIEW cart_events_1min
ENGINE = SummingMergeTree
ORDER BY minute
AS SELECT
    toStartOfMinute(timestamp) AS minute,
    countIf(event_type = 'add_to_cart') AS add_to_cart_count,
    countIf(event_type = 'remove_item') AS remove_item_count,
    countIf(event_type = 'update_quantity') AS update_quantity_count,
    countIf(event_type = 'checkout') AS checkout_count,
    count() AS total_events,
    sumIf(cart_total_after, event_type = 'checkout') AS revenue_total,
    avg(cart_total_after) AS avg_cart_value,
    max(cart_total_after) AS max_cart_value,
    uniq(user_id) AS unique_users,
    uniqIf(user_id, event_type = 'checkout') AS unique_checkout_users,
    sum(is_late) AS late_events_count
FROM cart_events_raw
GROUP BY minute;

DROP VIEW IF EXISTS user_hourly_activity;

CREATE MATERIALIZED VIEW user_hourly_activity
ENGINE = SummingMergeTree
ORDER BY (hour, user_id)
AS SELECT
    toStartOfHour(timestamp) AS hour,
    user_id,
    count() AS total_actions,
    countIf(event_type = 'add_to_cart') AS adds,
    countIf(event_type = 'remove_item') AS removes,
    countIf(event_type = 'checkout') AS checkouts,
    sum(cart_total_after - cart_total_before) AS cart_value_change,
    uniq(session_id) AS sessions_count,
    avg(processing_lag) AS avg_processing_lag_ms
FROM cart_events_raw
GROUP BY hour, user_id;

DROP VIEW IF EXISTS product_popularity_5min;

CREATE MATERIALIZED VIEW product_popularity_5min
ENGINE = SummingMergeTree
ORDER BY (window_start, product_id)
AS SELECT
    toStartOfFiveMinute(timestamp) AS window_start,
    product_id,
    product_name,
    countIf(event_type = 'add_to_cart') AS add_count,
    countIf(event_type = 'remove_item') AS remove_count,
    sumIf(quantity, event_type = 'add_to_cart') AS total_added_quantity,
    sumIf(quantity, event_type = 'remove_item') AS total_removed_quantity,
    sumIf(price * quantity, event_type = 'add_to_cart') AS potential_revenue,
    uniqIf(user_id, event_type IN ('add_to_cart', 'remove_item')) AS unique_users
FROM cart_events_raw
WHERE product_id > 0
GROUP BY window_start, product_id, product_name;

DROP VIEW IF EXISTS active_carts;

CREATE VIEW active_carts AS
SELECT
    user_id,
    max(timestamp) AS last_activity,
    countIf(event_type = 'add_to_cart') - countIf(event_type = 'remove_item') AS net_adds,
    max(cart_total_after) AS current_cart_value,
    max(session_id) AS current_session
FROM cart_events_raw
WHERE timestamp > now() - INTERVAL 1 HOUR
  AND event_type != 'checkout'
GROUP BY user_id
HAVING net_adds > 0
ORDER BY last_activity DESC;
