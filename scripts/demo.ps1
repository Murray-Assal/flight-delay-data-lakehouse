[CmdletBinding()]
param(
    [switch]$SkipPlatformCheck,
    [switch]$StopAfterDemo
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$taskRunner = Join-Path $PSScriptRoot "lakehouse.ps1"

Set-Location $projectRoot

# Isolate a clean-checkout run from any existing local lakehouse volumes.
$demoProject = "flight-delay-lakehouse-demo-$PID"
$env:COMPOSE_PROJECT_NAME = $demoProject
Write-Host "Using isolated Docker Compose project: $demoProject"

function Invoke-LakehouseTask {
    param([Parameter(Mandatory = $true)][string]$Task)

    Write-Host "`n==> $Task"
    # A PowerShell-only task such as ``configure`` does not set LASTEXITCODE.
    # Reset it so a prior native command cannot make a successful task look failed.
    $global:LASTEXITCODE = 0
    & $taskRunner $Task
    if ($LASTEXITCODE -ne 0) {
        throw "Lakehouse task failed: $Task"
    }
}

function Invoke-TrinoQuery {
    param([Parameter(Mandatory = $true)][string]$Sql)

    & docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "Trino query failed."
    }
}

# Fixture-only demo: safe for repeat runs and does not consume Aviationstack quota.
if (-not (Test-Path ".env")) {
    Invoke-LakehouseTask "configure"
}

Invoke-LakehouseTask "up"
if (-not $SkipPlatformCheck) {
    Invoke-LakehouseTask "platform-check"
}

# Baseline Bronze → Silver.
Invoke-LakehouseTask "bronze-fixture"
Invoke-LakehouseTask "silver"

# Additive schema evolution.
Invoke-LakehouseTask "bronze-v2-fixture"
Invoke-LakehouseTask "silver"
$snapshotLine = Invoke-TrinoQuery 'SELECT snapshot_id FROM iceberg.silver."silver_flights$snapshots" ORDER BY committed_at DESC LIMIT 1' |
    Select-Object -Last 1
$snapshotId = $snapshotLine.Trim().Trim('"')
if (-not $snapshotId -or $snapshotId -notmatch '^\d+$') {
    throw "Could not capture the Iceberg snapshot before the late correction."
}

# Late-arriving update and current-state merge.
Invoke-LakehouseTask "bronze-late-fixture"
Invoke-LakehouseTask "silver"

Write-Host "`n==> Current versus historical Silver state"
Invoke-TrinoQuery "SELECT flight_iata, delay_minutes, aircraft_type FROM iceberg.silver.silver_flights"
Invoke-TrinoQuery "SELECT flight_iata, delay_minutes, aircraft_type FROM iceberg.silver.silver_flights FOR VERSION AS OF $snapshotId"

# Gold facts, aggregates, and SCD Type 2 dimensions.
Invoke-LakehouseTask "gold"
Invoke-LakehouseTask "bronze-dimension-fixture"
Invoke-LakehouseTask "silver"
Invoke-LakehouseTask "gold"
Invoke-LakehouseTask "test"

Write-Host "`n==> Gold analytics summary"
Invoke-TrinoQuery "SELECT airline_iata, total_flights, delayed_flights, delay_rate, average_delay_minutes FROM iceberg.gold.agg_delay_by_airline_hour ORDER BY airline_iata, hour_start_at"
Write-Host "`nDemo complete. Open Metabase at http://localhost:3000 to inspect the dashboard."

if ($StopAfterDemo) {
    Invoke-LakehouseTask "down"
}