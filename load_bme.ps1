# Configuration
$ContainerName = "clickstack"
$Database = "sensor_storage"
$Table = "bme280_data"
$DataDir = "./clickhouse-bme-data"

# Find the first CSV file in the data directory
$CsvFile = Get-ChildItem -Path $DataDir -Filter "*.csv" | Select-Object -First 1

if (-not $CsvFile) {
    Write-Error "No CSV file found in $DataDir"
    exit 1
}

Write-Host "Targeting Container: $ContainerName"
Write-Host "Loading data from: $($CsvFile.FullName)"
Write-Host "Destination: $Database.$Table"
Write-Host "------------------------------------------------"

# Get start time
$StartTime = Get-Date

# Load data using docker exec and clickhouse-client
# We pipe the file content into the container's stdin (removed -Raw for better line parsing)
Get-Content -Path $CsvFile.FullName | docker exec -i $ContainerName clickhouse-client `
    --query="INSERT INTO $Database.$Table FORMAT CSVWithNames" `
    --format_csv_delimiter ';'

# Get end time
$EndTime = Get-Date
$DurationSec = ($EndTime - $StartTime).TotalSeconds

# Row count check
$RowsAfter = (docker exec -i $ContainerName clickhouse-client --query="SELECT count() FROM $Database.$Table").Trim()

# Calculate throughput
$Throughput = [math]::Round($RowsAfter / ($DurationSec + 0.001), 2)

Write-Host "Load Summary:"
Write-Host "  - Duration: $DurationSec seconds"
Write-Host "  - Total Rows in Table: $RowsAfter"
Write-Host "  - Approx Throughput: $Throughput rows/sec"
Write-Host "------------------------------------------------"

Write-Host "Detailed Query Metrics (from system.query_log):"
docker exec -i $ContainerName clickhouse-client --query="
SELECT
    event_time,
    query_duration_ms,
    formatReadableSize(read_bytes) as read_bytes,
    written_rows,
    formatReadableSize(written_bytes) as written_bytes,
    formatReadableSize(memory_usage) as peak_memory
FROM system.query_log
WHERE query LIKE 'INSERT INTO $Database.$Table%'
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 1
FORMAT Vertical
"

Write-Host "------------------------------------------------"
Write-Host "Compression Metrics (from system.parts):"
docker exec -i $ContainerName clickhouse-client --query="
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS storage_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS raw_size,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS compression_ratio,
    sum(rows) AS total_rows
FROM system.parts
WHERE database = '$Database' AND table = '$Table' AND active
GROUP BY table
FORMAT Vertical
"
