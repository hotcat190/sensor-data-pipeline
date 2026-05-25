#!/bin/bash

# Configuration
CONTAINER_NAME="clickstack"
DATABASE="sensor_storage"
# Calculate absolute paths relative to the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"
BME_DIR="$PROJECT_ROOT/clickhouse-bme-data"
SDS_DIR="$PROJECT_ROOT/clickhouse-sds-data"

load_combined_data() {
    local dir=$1
    local filename=$2
    local table=$3
    local label=$4

    CSV_FILE="$dir/$filename"

    if [ ! -f "$CSV_FILE" ]; then
        echo "Warning: Combined file not found: $CSV_FILE"
        echo "Please run 'python combine_data.py' first."
        return
    fi

    echo "Loading combined $label data natively from: $CSV_FILE"
    
    START_TIME=$(date +%s%N)
    
    # 1. Copy file to container's temp directory for native access
    TEMP_FILE="/tmp/$filename"
    docker cp "$CSV_FILE" "$CONTAINER_NAME:$TEMP_FILE"

    # 2. Use ClickHouse native FROM INFILE with error tolerance
    docker exec "$CONTAINER_NAME" clickhouse-client \
        --query="INSERT INTO $DATABASE.$table FROM INFILE '$TEMP_FILE' FORMAT CSVWithNames" \
        --input_format_allow_errors_num=100000 \
        --input_format_allow_errors_ratio=0.1 \
        --format_csv_delimiter ';'

    # 3. Cleanup temp file in container
    docker exec "$CONTAINER_NAME" rm "$TEMP_FILE"
    
    END_TIME=$(date +%s%N)
    DURATION_NS=$((END_TIME - START_TIME))
    DURATION_SEC=$(echo "scale=3; $DURATION_NS / 1000000000" | bc -l 2>/dev/null || awk "BEGIN {print $DURATION_NS / 1000000000}")
    
    echo "  - Done in $DURATION_SEC seconds"
}

echo "Targeting Container: $CONTAINER_NAME"
echo "------------------------------------------------"

# Load combined BME data
load_combined_data "$BME_DIR" "combined_bme.csv" "bme280_data" "BME280"

# Load combined SDS data
load_combined_data "$SDS_DIR" "combined_sds.csv" "sds011_data" "SDS011"

echo "------------------------------------------------"
echo "Recent Load Metrics (system.query_log):"
docker exec -i "$CONTAINER_NAME" clickhouse-client --query="
SELECT
    event_time,
    query_duration_ms,
    written_rows,
    formatReadableSize(written_bytes) as written_bytes,
    formatReadableSize(memory_usage) as mem,
    substring(query, 1, 50) as query_short
FROM system.query_log
WHERE query LIKE 'INSERT INTO sensor_storage.%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 5
SETTINGS use_query_cache = 0
FORMAT Vertical
"
