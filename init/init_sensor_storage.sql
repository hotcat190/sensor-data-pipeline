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

CREATE TABLE IF NOT EXISTS sensor_storage.sds011_data
(
    sensor_id UInt32,
    sensor_type LowCardinality(String),
    location UInt32,
    lat Float32,
    lon Float32,
    timestamp DateTime,
    P1 Float32 CODEC(Gorilla, LZ4),
    durP1 Nullable(Float32),
    ratioP1 Nullable(Float32),
    P2 Float32 CODEC(Gorilla, LZ4),
    durP2 Nullable(Float32),
    ratioP2 Nullable(Float32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (sensor_id, timestamp);

USE sensor_storage;

CREATE TABLE IF NOT EXISTS sensor_storage.kafka_bme280_raw
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'sensor_bme280_topic',
    kafka_group_name = 'ch_bme280_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS sensor_storage.kafka_sds011_raw
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'sensor_sds011_topic',
    kafka_group_name = 'ch_sds011_consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_storage.mv_bme280_data
TO sensor_storage.bme280_data
AS
SELECT
    JSONExtractUInt(raw, 'sensor_id') AS sensor_id,
    JSONExtractString(raw, 'sensor_type') AS sensor_type,
    JSONExtractUInt(raw, 'location') AS location,
    JSONExtractFloat(raw, 'lat') AS lat,
    JSONExtractFloat(raw, 'lon') AS lon,
    toDateTime(JSONExtractString(raw, 'timestamp')) AS timestamp,
    JSONExtractFloat(raw, 'pressure') AS pressure,
    if(JSONHas(raw, 'altitude'), JSONExtractFloat(raw, 'altitude'), NULL) AS altitude,
    if(JSONHas(raw, 'pressure_sealevel'), JSONExtractFloat(raw, 'pressure_sealevel'), NULL) AS pressure_sealevel,
    JSONExtractFloat(raw, 'temperature') AS temperature,
    JSONExtractFloat(raw, 'humidity') AS humidity
FROM sensor_storage.kafka_bme280_raw;

CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_storage.mv_sds011_data
TO sensor_storage.sds011_data
AS
SELECT
    JSONExtractUInt(raw, 'sensor_id') AS sensor_id,
    JSONExtractString(raw, 'sensor_type') AS sensor_type,
    JSONExtractUInt(raw, 'location') AS location,
    JSONExtractFloat(raw, 'lat') AS lat,
    JSONExtractFloat(raw, 'lon') AS lon,
    toDateTime(JSONExtractString(raw, 'timestamp')) AS timestamp,
    JSONExtractFloat(raw, 'P1') AS P1,
    if(JSONHas(raw, 'durP1'), JSONExtractFloat(raw, 'durP1'), NULL) AS durP1,
    if(JSONHas(raw, 'ratioP1'), JSONExtractFloat(raw, 'ratioP1'), NULL) AS ratioP1,
    JSONExtractFloat(raw, 'P2') AS P2,
    if(JSONHas(raw, 'durP2'), JSONExtractFloat(raw, 'durP2'), NULL) AS durP2,
    if(JSONHas(raw, 'ratioP2'), JSONExtractFloat(raw, 'ratioP2'), NULL) AS ratioP2
FROM sensor_storage.kafka_sds011_raw;
