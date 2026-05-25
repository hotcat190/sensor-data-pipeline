# Configuration
$ContainerName = "clickstack"
$Database = "sensor_storage"
# Paths are relative to the script location in data-loader/
$BmeDir = "$PSScriptRoot/../clickhouse-bme-data"
$SdsDir = "$PSScriptRoot/../clickhouse-sds-data"

function Load-CombinedSensorData {
    param($Dir, $Filename, $Table, $Label)

    $CsvFile = Join-Path $Dir $Filename

    if (-not (Test-Path $CsvFile)) {
        Write-Warning "Combined file not found: $CsvFile"
        Write-Warning "Please run 'python combine_data.py' first."
        return
    }

    Write-Host "Loading combined $Label data natively from: $CsvFile"
    $StartTime = Get-Date

    # 1. Copy file to container's temp directory for native access
    $TempFileName = "/tmp/$Filename"
    docker cp $CsvFile "${ContainerName}:$TempFileName"

    # 2. Use ClickHouse native FROM INFILE with error tolerance
    docker exec $ContainerName clickhouse-client `
        --query="INSERT INTO $Database.$Table FROM INFILE '$TempFileName' FORMAT CSVWithNames" `
        --input_format_allow_errors_num=100000 `
        --input_format_allow_errors_ratio=0.1 `
        --format_csv_delimiter ';'

    # 3. Cleanup temp file in container
    docker exec $ContainerName rm $TempFileName

    $EndTime = Get-Date
    $DurationSec = [math]::Round(($EndTime - $StartTime).TotalSeconds, 3)
    Write-Host "  - Done in $DurationSec seconds"
}

Write-Host "Targeting Container: $ContainerName"
Write-Host "------------------------------------------------"

# Load combined BME data
Load-CombinedSensorData -Dir $BmeDir -Filename "combined_bme.csv" -Table "bme280_data" -Label "BME280"

# Load combined SDS data
Load-CombinedSensorData -Dir $SdsDir -Filename "combined_sds.csv" -Table "sds011_data" -Label "SDS011"

Write-Host "------------------------------------------------"
Write-Host "Recent Load Metrics (system.query_log):"
docker exec -i $ContainerName clickhouse-client --query="
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
