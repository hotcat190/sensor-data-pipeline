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
CREATE MATERIALIZED VIEW mv_env_sensor_metrics
TO environmental_data
AS
SELECT
    toDateTime(JSONExtractInt(raw, 'dt')) AS timestamp,

    JSONExtractString(JSONExtractRaw(raw, 'sys'), 'country') AS country,
    JSONExtractString(raw, 'name') AS city,

    JSONExtractFloat(JSONExtractRaw(raw, 'coord'), 'lat') AS lat,
    JSONExtractFloat(JSONExtractRaw(raw, 'coord'), 'lon') AS lon,

    JSONExtractString(JSONExtractArrayRaw(raw, 'weather')[1], 'main') AS weather_main,
    JSONExtractString(JSONExtractArrayRaw(raw, 'weather')[1], 'description') AS weather_desc,
    JSONExtractString(JSONExtractArrayRaw(raw, 'weather')[1], 'icon') AS weather_icon,

    JSONExtractFloat(JSONExtractRaw(raw, 'main'), 'temp') - 273.15 AS temp,
    JSONExtractFloat(JSONExtractRaw(raw, 'main'), 'feels_like') - 273.15 AS feels_like,
    JSONExtractFloat(JSONExtractRaw(raw, 'main'), 'temp_min') - 273.15 AS temp_min,
    JSONExtractFloat(JSONExtractRaw(raw, 'main'), 'temp_max') - 273.15 AS temp_max,

    JSONExtractUInt(JSONExtractRaw(raw, 'main'), 'pressure') AS pressure,
    JSONExtractUInt(JSONExtractRaw(raw, 'main'), 'humidity') AS humidity,
    JSONExtractUInt(JSONExtractRaw(raw, 'main'), 'sea_level') AS sea_level,
    JSONExtractUInt(JSONExtractRaw(raw, 'main'), 'grnd_level') AS grnd_level,

    JSONExtractFloat(JSONExtractRaw(raw, 'wind'), 'speed') AS wind_speed,
    JSONExtractUInt(JSONExtractRaw(raw, 'wind'), 'deg') AS wind_deg,
    ifNull(JSONExtractFloat(JSONExtractRaw(raw, 'wind'), 'gust'), 0.0) AS wind_gust,

    ifNull(JSONExtractFloat(JSONExtractRaw(raw, 'rain'), '1h'), 0.0) AS rain_1h,
    ifNull(JSONExtractFloat(JSONExtractRaw(raw, 'snow'), '1h'), 0.0) AS snow_1h,

    JSONExtractUInt(JSONExtractRaw(raw, 'clouds'), 'all') AS clouds_all,
    ifNull(JSONExtractUInt(raw, 'visibility'), 10000) AS visibility,

    toDateTime(JSONExtractInt(JSONExtractRaw(raw, 'sys'), 'sunrise')) AS sunrise,
    toDateTime(JSONExtractInt(JSONExtractRaw(raw, 'sys'), 'sunset')) AS sunset,

    JSONExtractUInt(raw, 'id') AS station_id,
    JSONExtractInt(raw, 'timezone') AS timezone
FROM kafka_env_sensor_metrics;