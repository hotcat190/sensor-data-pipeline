#!/bin/bash

# Configuration
CONTAINER_NAME="clickstack"
DATABASE="sensor_storage"
TABLE="bme280_data"
DATA_DIR="./clickhouse-bme-data"

# Find the first CSV file in the data directory
CSV_FILE=$(ls $DATA_DIR/*.csv 2>/dev/null | head -n 1)

if [ -z "$CSV_FILE" ]; then
    echo "Error: No CSV file found in $DATA_DIR"
    exit 1
fi

echo "Targeting Container: $CONTAINER_NAME"
echo "Loading data from: $CSV_FILE"
echo "Destination: $DATABASE.$TABLE"
echo "------------------------------------------------"

# Get start time (nanoseconds)
START_TIME=$(date +%s%N)

# Load data using docker exec and clickhouse-client
# We pipe the host file into the container's stdin
cat "$CSV_FILE" | docker exec -i "$CONTAINER_NAME" clickhouse-client \
    --query="INSERT INTO $DATABASE.$TABLE FORMAT CSVWithNames" \
    --format_csv_delimiter ';'

# Get end time
END_TIME=$(date +%s%N)

# Row count check
ROWS_AFTER=$(docker exec -i "$CONTAINER_NAME" clickhouse-client --query="SELECT count() FROM $DATABASE.$TABLE")

# Calculate duration and throughput
DURATION_NS=$((END_TIME - START_TIME))
DURATION_SEC=$(echo "scale=3; $DURATION_NS / 1000000000" | bc -l 2>/dev/null || awk "BEGIN {print $DURATION_NS / 1000000000}")
THROUGHPUT=$(echo "scale=2; $ROWS_AFTER / $DURATION_SEC" | bc -l 2>/dev/null || awk "BEGIN {print $ROWS_AFTER / $DURATION_SEC}")

echo "Load Summary:"
echo "  - Duration: $DURATION_SEC seconds"
echo "  - Total Rows in Table: $ROWS_AFTER"
echo "  - Approx Throughput: $THROUGHPUT rows/sec"
echo "------------------------------------------------"

echo "Detailed Query Metrics (from system.query_log):"
docker exec -i "$CONTAINER_NAME" clickhouse-client --query="
SELECT
    event_time,
    query_duration_ms,
    formatReadableSize(read_bytes) as read_bytes,
    written_rows,
    formatReadableSize(written_bytes) as written_bytes,
    formatReadableSize(memory_usage) as peak_memory
FROM system.query_log
WHERE query LIKE 'INSERT INTO $DATABASE.$TABLE%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1
FORMAT Vertical
"

echo "------------------------------------------------"
echo "Compression Metrics (from system.parts):"
docker exec -i "$CONTAINER_NAME" clickhouse-client --query="
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS storage_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS raw_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS compression_ratio,
    sum(rows) AS total_rows
FROM system.parts
WHERE database = '$DATABASE' AND table = '$TABLE' AND active
GROUP BY table
FORMAT Vertical
"
