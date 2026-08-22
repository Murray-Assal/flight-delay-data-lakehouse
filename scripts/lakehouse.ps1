param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("configure", "up", "down", "logs", "platform-check", "test")]
    [string]$Task
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$sparkPackages = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.postgresql:postgresql:42.7.5"

Set-Location $projectRoot

switch ($Task) {
    "configure" {
        if (-not (Test-Path ".env")) {
            Copy-Item ".env.example" ".env"
            Write-Host "Created .env. Replace its placeholder values before starting the stack."
        }
        else {
            Write-Host ".env already exists; no changes made."
        }
    }
    "up" {
        docker compose up -d postgres minio minio-init trino metabase
    }
    "down" {
        docker compose down
    }
    "logs" {
        docker compose logs -f --tail=100
    }
    "platform-check" {
        docker compose run --rm spark --packages $sparkPackages jobs/verify_lakehouse.py
    }
    "test" {
        python -m pytest
    }
}
