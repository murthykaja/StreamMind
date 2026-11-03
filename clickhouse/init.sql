CREATE DATABASE IF NOT EXISTS streammind;

CREATE TABLE IF NOT EXISTS streammind.window_metrics
(
    service String,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    requests UInt32,
    errors UInt32,
    avg_latency_ms Float32,
    p95_latency_ms Float32
)
ENGINE = MergeTree
ORDER BY (service, window_start)
TTL toDateTime(window_start) + INTERVAL 6 HOUR;

CREATE TABLE IF NOT EXISTS streammind.logs
(
    ts DateTime64(3, 'UTC'),
    service String,
    level LowCardinality(String),
    message String,
    version String
)
ENGINE = MergeTree
ORDER BY (service, ts)
TTL toDateTime(ts) + INTERVAL 6 HOUR;

CREATE TABLE IF NOT EXISTS streammind.spans
(
    ts DateTime64(3, 'UTC'),
    trace_id String,
    service String,
    peer String,
    duration_ms Float32,
    error UInt8
)
ENGINE = MergeTree
ORDER BY (service, peer, ts)
TTL toDateTime(ts) + INTERVAL 6 HOUR;

CREATE TABLE IF NOT EXISTS streammind.deployments
(
    ts DateTime64(3, 'UTC'),
    service String,
    version String,
    previous_version String,
    author String,
    change_summary String
)
ENGINE = MergeTree
ORDER BY (service, ts)
TTL toDateTime(ts) + INTERVAL 24 HOUR;
