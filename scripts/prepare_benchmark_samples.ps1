$ErrorActionPreference = "Stop"

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

function Invoke-DockerChecked {
    param([string[]]$Arguments)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = docker @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference

    $output | ForEach-Object { Write-Host $_ }

    if ($exitCode -ne 0) {
        throw "Comando Docker fallito: docker $($Arguments -join ' ')"
    }

    return $output
}

$hdfsNameNode = Read-DotEnvValue ".env" "HDFS_NAMENODE"
$hdfsProcessedDir = Read-DotEnvValue ".env" "HDFS_PROCESSED_DIR"
$hdfsSamplesDir = Read-DotEnvValue ".env" "HDFS_SAMPLES_DIR"
$sparkMasterUrl = Read-DotEnvValue ".env" "SPARK_MASTER_URL"

if (-not $hdfsNameNode) { $hdfsNameNode = "hdfs://namenode:8020" }
if (-not $hdfsProcessedDir) { $hdfsProcessedDir = "/data/processed" }
if (-not $hdfsSamplesDir) { $hdfsSamplesDir = "/data/samples" }
if (-not $sparkMasterUrl) { $sparkMasterUrl = "spark://spark-master:7077" }

$inputPath = "$hdfsNameNode$hdfsProcessedDir/flights_2024_clean.parquet"
$outputRoot = "$hdfsNameNode$hdfsSamplesDir"

Write-Host "Preparo sample benchmark deterministici"
Write-Host "Input: $inputPath"
Write-Host "Output root: $outputRoot"

Invoke-DockerChecked @(
    "compose", "--env-file", ".env",
    "exec", "-T", "spark-master",
    "/opt/spark/bin/spark-submit",
    "--master", $sparkMasterUrl,
    "--conf", "spark.hadoop.fs.defaultFS=$hdfsNameNode",
    "/opt/project/src/prepare_benchmark_samples.py",
    "--input", $inputPath,
    "--output-root", $outputRoot
) | Out-Null

Write-Host "Sample benchmark creati in $outputRoot"
