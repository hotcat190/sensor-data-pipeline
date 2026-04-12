DROP TABLE IF EXISTS input_test;

CREATE TABLE IF NOT EXISTS input_test
(
    raw String
)
ENGINE = MergeTree
ORDER BY tuple();

DROP VIEW IF EXISTS mv_env_sensor_metrics;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_env_sensor_metrics
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
FROM input_test;

INSERT INTO input_test VALUES ('{
  "coord": { "lon": 105.8412, "lat": 21.0245 },
  "weather": [
    {
      "id": 501,
      "main": "Rain",
      "description": "moderate rain",
      "icon": "10d"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 300.15,
    "feels_like": 303.15,
    "temp_min": 299.15,
    "temp_max": 301.15,
    "pressure": 1005,
    "humidity": 85,
    "sea_level": 1005,
    "grnd_level": 1000
  },
  "visibility": 8000,
  "wind": {
    "speed": 5.5,
    "deg": 180,
    "gust": 7.2
  },
  "rain": {
    "1h": 3.5
  },
  "clouds": {
    "all": 90
  },
  "dt": 1712900000,
  "sys": {
    "type": 1,
    "id": 1234,
    "country": "VN",
    "sunrise": 1712870000,
    "sunset": 1712914000
  },
  "timezone": 25200,
  "id": 1581130,
  "name": "Hanoi",
  "cod": 200
}');

INSERT INTO input_test VALUES ('{
  "coord": { "lon": 139.6917, "lat": 35.6895 },
  "weather": [
    {
      "id": 600,
      "main": "Snow",
      "description": "light snow",
      "icon": "13n"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 273.15,
    "feels_like": 270.15,
    "temp_min": 272.15,
    "temp_max": 274.15,
    "pressure": 1012,
    "humidity": 70,
    "sea_level": 1012,
    "grnd_level": 1008
  },
  "visibility": 10000,
  "wind": {
    "speed": 3.2,
    "deg": 45
  },
  "snow": {
    "1h": 1.2
  },
  "clouds": {
    "all": 75
  },
  "dt": 1712900300,
  "sys": {
    "type": 1,
    "id": 5678,
    "country": "JP",
    "sunrise": 1712865000,
    "sunset": 1712912000
  },
  "timezone": 32400,
  "id": 1850147,
  "name": "Tokyo",
  "cod": 200
}');

DROP VIEW IF EXISTS mv_env_sensor_metrics;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_env_sensor_metrics
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