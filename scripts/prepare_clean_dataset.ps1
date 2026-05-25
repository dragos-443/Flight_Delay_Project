param(
    [string]$InputPath = $null,
    [string]$ParquetOutputPath = $null,
    [string]$CsvOutputPath = $null
)

$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$Arguments)

    docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Comando Docker fallito: docker $($Arguments -join ' ')"
    }
}

function Read-DotEnvValue {
    param(
        [string]$Path,
        [string]$Key
    )

    if (-not (Test-Path $Path)) {
        return $null
    }

    $line = Get-Content $Path | Where-Object { $_ -match "^$Key=" } | Select-Object -First 1
    if (-not $line) {
        return $null
    }

    return ($line -replace "^$Key=", "").Trim()
}

$hdfsNameNode = Read-DotEnvValue ".env" "HDFS_NAMENODE"
$hdfsRawDir = Read-DotEnvValue ".env" "HDFS_RAW_DIR"
$hdfsProcessedDir = Read-DotEnvValue ".env" "HDFS_PROCESSED_DIR"
$sparkMasterUrl = Read-DotEnvValue ".env" "SPARK_MASTER_URL"

if (-not $hdfsNameNode) { $hdfsNameNode = "hdfs://namenode:8020" }
if (-not $hdfsRawDir) { $hdfsRawDir = "/data/raw" }
if (-not $hdfsProcessedDir) { $hdfsProcessedDir = "/data/processed" }
if (-not $sparkMasterUrl) { $sparkMasterUrl = "spark://spark-master:7077" }

if (-not $InputPath) {
    $InputPath = "$hdfsNameNode$hdfsRawDir/flight_data_2024.csv"
}

if (-not $ParquetOutputPath) {
    $ParquetOutputPath = "$hdfsNameNode$hdfsProcessedDir/flights_2024_clean.parquet"
}

if (-not $CsvOutputPath) {
    $CsvOutputPath = "$hdfsNameNode$hdfsProcessedDir/flights_2024_clean_csv"
}

Write-Host "Preparo dataset pulito da: $InputPath"
Write-Host "Output Parquet: $ParquetOutputPath"
Write-Host "Output CSV: $CsvOutputPath"

Invoke-Docker @(
    "compose", "--env-file", ".env",
    "exec", "-T", "spark-master",
    "/opt/spark/bin/spark-submit",
    "--master", $sparkMasterUrl,
    "--conf", "spark.hadoop.fs.defaultFS=$hdfsNameNode",
    "/opt/project/src/prepare_clean_dataset.py",
    "--input", $InputPath,
    "--parquet-output", $ParquetOutputPath,
    "--csv-output", $CsvOutputPath
)
