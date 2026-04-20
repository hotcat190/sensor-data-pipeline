-- Create grafana user
CREATE USER IF NOT EXISTS grafana IDENTIFIED BY '123';

-- Grant read access
GRANT SELECT ON *.* TO grafana;

-- Allow safe settings (needed for Grafana / clickhouse-go)
ALTER USER grafana SETTINGS
    readonly = 2,
    allow_ddl = 0;

-- Kafka table
CREATE TABLE IF NOT EXISTS kafka_env_sensor_metrics
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'env_sensor_metrics',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

-- Main table
CREATE TABLE IF NOT EXISTS environmental_data
(
    timestamp        DateTime,
    date             Date MATERIALIZED toDate(timestamp),

    country          LowCardinality(String),
    city             LowCardinality(String),

    lat              Float32,
    lon              Float32,

    weather_main     LowCardinality(String),
    weather_desc     String,
    weather_icon     LowCardinality(String),

    temp             Float32,
    feels_like       Float32,
    temp_min         Float32,
    temp_max         Float32,

    pressure         UInt16,
    humidity         UInt8,
    sea_level        UInt16,
    grnd_level       UInt16,

    wind_speed       Float32,
    wind_deg         UInt16,
    wind_gust        Float32,

    rain_1h          Float32,
    snow_1h          Float32,

    clouds_all       UInt8,
    visibility       UInt32,

    sunrise          DateTime,
    sunset           DateTime,

    station_id       UInt32,
    timezone         Int32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (country, city, timestamp);

-- Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_env_sensor_metrics
TO environmental_data
AS
SELECT
    toDateTime(JSONExtractInt(raw, 'timestamp')) AS timestamp,
    JSONExtractString(raw, 'country') AS country,
    JSONExtractString(raw, 'city') AS city,
    JSONExtractFloat(raw, 'lat') AS lat,
    JSONExtractFloat(raw, 'lon') AS lon,
    JSONExtractString(raw, 'weather_main') AS weather_main,
    JSONExtractString(raw, 'weather_desc') AS weather_desc,
    JSONExtractString(raw, 'weather_icon') AS weather_icon,
    JSONExtractFloat(raw, 'temp') AS temp,
    JSONExtractFloat(raw, 'feels_like') AS feels_like,
    JSONExtractFloat(raw, 'temp_min') AS temp_min,
    JSONExtractFloat(raw, 'temp_max') AS temp_max,
    JSONExtractUInt(raw, 'pressure') AS pressure,
    JSONExtractUInt(raw, 'humidity') AS humidity,
    JSONExtractUInt(raw, 'sea_level') AS sea_level,
    JSONExtractUInt(raw, 'grnd_level') AS grnd_level,
    JSONExtractFloat(raw, 'wind_speed') AS wind_speed,
    JSONExtractUInt(raw, 'wind_deg') AS wind_deg,
    JSONExtractFloat(raw, 'wind_gust') AS wind_gust,
    JSONExtractFloat(raw, 'rain_1h') AS rain_1h,
    JSONExtractFloat(raw, 'snow_1h') AS snow_1h,
    JSONExtractUInt(raw, 'clouds_all') AS clouds_all,
    JSONExtractUInt(raw, 'visibility') AS visibility,
    toDateTime(JSONExtractInt(raw, 'sunrise')) AS sunrise,
    toDateTime(JSONExtractInt(raw, 'sunset')) AS sunset,
    JSONExtractUInt(raw, 'station_id') AS station_id,
    JSONExtractInt(raw, 'timezone') AS timezone
FROM kafka_env_sensor_metrics;

-- BME Performance Test Schema
CREATE DATABASE IF NOT EXISTS sensor_storage;

CREATE TABLE IF NOT EXISTS sensor_storage.bme280_data
(
    sensor_id UInt32,
    sensor_type LowCardinality(String),
    location UInt32,
    lat Float32,
    lon Float32,
    timestamp DateTime,
    pressure Float32 CODEC(DoubleDelta, LZ4),
    altitude Nullable(Float32),
    pressure_sealevel Nullable(Float32),
    temperature Float32 CODEC(DoubleDelta, LZ4),
    humidity Float32 CODEC(DoubleDelta, LZ4)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (sensor_id, timestamp);