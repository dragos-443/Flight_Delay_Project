param(
    [string]$LocalPath = $null
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

if (-not $LocalPath) {
    $LocalPath = Read-DotEnvValue ".env" "DATASET_LOCAL_PATH"
}

if (-not $LocalPath) {
    $LocalPath = "./data/raw/flight_data_2024.csv"
}

if (-not (Test-Path $LocalPath) -and (Test-Path "./flight_data_2024.csv")) {
    $LocalPath = "./flight_data_2024.csv"
}

if (-not (Test-Path $LocalPath)) {
    throw "Dataset non trovato. Inserisci il CSV in data/raw/flight_data_2024.csv oppure passa -LocalPath."
}

$hdfsRawDir = Read-DotEnvValue ".env" "HDFS_RAW_DIR"
$hdfsProcessedDir = Read-DotEnvValue ".env" "HDFS_PROCESSED_DIR"
$hdfsOutputsDir = Read-DotEnvValue ".env" "HDFS_OUTPUTS_DIR"

if (-not $hdfsRawDir) { $hdfsRawDir = "/data/raw" }
if (-not $hdfsProcessedDir) { $hdfsProcessedDir = "/data/processed" }
if (-not $hdfsOutputsDir) { $hdfsOutputsDir = "/outputs" }

$fileName = Split-Path $LocalPath -Leaf
$containerTmpPath = "/tmp/$fileName"
$hdfsFilePath = "$hdfsRawDir/$fileName"

Write-Host "Creo directory HDFS..."
Invoke-Docker @("compose", "--env-file", ".env", "exec", "-T", "namenode", "hdfs", "dfs", "-mkdir", "-p", $hdfsRawDir, $hdfsProcessedDir, $hdfsOutputsDir)

Write-Host "Copio il dataset nel container namenode..."
Invoke-Docker @("compose", "--env-file", ".env", "cp", $LocalPath, "namenode:$containerTmpPath")

Write-Host "Carico il dataset in HDFS: $hdfsFilePath"
Invoke-Docker @("compose", "--env-file", ".env", "exec", "-T", "namenode", "hdfs", "dfs", "-put", "-f", $containerTmpPath, $hdfsFilePath)

Write-Host "Contenuto di ${hdfsRawDir}:"
Invoke-Docker @("compose", "--env-file", ".env", "exec", "-T", "namenode", "hdfs", "dfs", "-ls", "-h", $hdfsRawDir)
