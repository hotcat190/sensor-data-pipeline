-- Create grafana user
CREATE USER IF NOT EXISTS grafana IDENTIFIED BY '123';

-- Grant read access
GRANT SELECT ON *.* TO grafana;

-- Allow safe settings (needed for Grafana / clickhouse-go)
ALTER USER grafana SETTINGS
    readonly = 2,
    allow_ddl = 0;