param(
    [ValidateSet("100k", "500k", "half", "full", "all")]
    [string]$RunSize = "full",
    [string]$InputPath = $null,
    [string]$OutputRoot = $null,
    [string]$BenchmarkRoot = $null,
    [switch]$CompareWithSparkSql
)

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

function Invoke-BeelineChecked {
    param([string]$Sql)

    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = $Sql | docker compose --env-file .env exec -T hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true --showHeader=true --outputformat=tsv2 -f /dev/stdin 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference

    $output | ForEach-Object { Write-Host $_ }

    if ($exitCode -ne 0) {
        throw "Comando Hive fallito."
    }
}

function Convert-ToCsvValue {
    param([string]$Value)

    if ($null -eq $Value) {
        return '""'
    }

    return '"' + ($Value -replace '"', '""') + '"'
}

function Save-TimingRow {
    param(
        [string]$TimingPath,
        [string]$Analysis,
        [string]$Technology,
        [string]$CurrentRunSize,
        [string]$CurrentInputPath,
        [string]$CurrentOutputPath,
        [double]$ExecutionTimeSeconds,
        [int]$OutputRows,
        [string]$RunTimestamp
    )

    $localDir = Join-Path "outputs" "benchmarks\analysis_3_1\hive"
    New-Item -ItemType Directory -Force -Path $localDir | Out-Null
    $localTimingPath = Join-Path $localDir "timings.csv"

    $header = "analysis,technology,run_size,input_path,output_path,execution_time_seconds,output_rows,run_timestamp"
    if (-not (Test-Path $localTimingPath)) {
        Set-Content -Path $localTimingPath -Value $header
    }

    $line = @(
        Convert-ToCsvValue $Analysis
        Convert-ToCsvValue $Technology
        Convert-ToCsvValue $CurrentRunSize
        Convert-ToCsvValue $CurrentInputPath
        Convert-ToCsvValue $CurrentOutputPath
        $ExecutionTimeSeconds.ToString("0.000", [System.Globalization.CultureInfo]::InvariantCulture)
        $OutputRows
        Convert-ToCsvValue $RunTimestamp
    ) -join ","

    Add-Content -Path $localTimingPath -Value $line

    $hdfsTimingDir = $TimingPath -replace "/[^/]+$", ""
    $containerTimingPath = "/tmp/analysis_3_1_hive_timings.csv"

    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "namenode",
        "hdfs", "dfs", "-mkdir", "-p", $hdfsTimingDir
    ) | Out-Null

    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "cp", $localTimingPath, "namenode:$containerTimingPath"
    ) | Out-Null

    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "namenode",
        "hdfs", "dfs", "-put", "-f",
        $containerTimingPath,
        $TimingPath
    ) | Out-Null
}

function Get-HiveCount {
    param([string]$TableName)

    $countSql = "USE flight_delay; SELECT COUNT(*) AS row_count FROM $TableName;"
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = $countSql | docker compose --env-file .env exec -T hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true --showHeader=false --outputformat=tsv2 -f /dev/stdin 2>&1
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference

    if ($exitCode -ne 0) {
        throw "Impossibile leggere il conteggio da Hive."
    }

    $countLine = $output | Where-Object { $_ -match "^\d+$" } | Select-Object -Last 1
    if (-not $countLine) {
        throw "Conteggio Hive non trovato nell'output."
    }

    return [int]$countLine
}

$hdfsNameNode = Read-DotEnvValue ".env" "HDFS_NAMENODE"
$hdfsProcessedDir = Read-DotEnvValue ".env" "HDFS_PROCESSED_DIR"
$hdfsSamplesDir = Read-DotEnvValue ".env" "HDFS_SAMPLES_DIR"
$sparkMasterUrl = Read-DotEnvValue ".env" "SPARK_MASTER_URL"

if (-not $hdfsNameNode) { $hdfsNameNode = "hdfs://namenode:8020" }
if (-not $hdfsProcessedDir) { $hdfsProcessedDir = "/data/processed" }
if (-not $hdfsSamplesDir) { $hdfsSamplesDir = "/data/samples" }
if (-not $sparkMasterUrl) { $sparkMasterUrl = "spark://spark-master:7077" }

function Resolve-InputPath {
    param([string]$CurrentRunSize)

    if ($InputPath) {
        return $InputPath
    }

    return "$hdfsSamplesDir/flights_clean_$CurrentRunSize.parquet"
}

if (-not $OutputRoot) {
    $OutputRoot = "/outputs/analysis_3_1/hive"
}

if (-not $BenchmarkRoot) {
    $BenchmarkRoot = "/outputs/benchmarks/analysis_3_1/hive"
}

$runSizes = @($RunSize)
if ($RunSize -eq "all") {
    $runSizes = @("100k", "500k", "half", "full")
    $localTimingPath = Join-Path "outputs" "benchmarks\analysis_3_1\hive\timings.csv"
    Remove-Item -LiteralPath $localTimingPath -ErrorAction SilentlyContinue
    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "namenode",
        "hdfs", "dfs", "-rm", "-f",
        "$BenchmarkRoot/timings.csv"
    ) | Out-Null
}

$setupSql = @"
CREATE DATABASE IF NOT EXISTS flight_delay;
"@
Invoke-BeelineChecked $setupSql

foreach ($currentRunSize in $runSizes) {
    $currentInputPath = Resolve-InputPath $currentRunSize
    $parquetOutputPath = "$OutputRoot/$currentRunSize/parquet"
    $csvOutputPath = "$OutputRoot/$currentRunSize/csv"
    $qualifiedParquetOutputPath = "$hdfsNameNode$parquetOutputPath"
    $runTimestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $outputTable = "analysis_3_1_hive_$($currentRunSize -replace '[^A-Za-z0-9_]', '_')"

    Write-Host "Eseguo analisi 3.1 Hive ($currentRunSize)"
    Write-Host "Input: $currentInputPath"
    Write-Host "Output Parquet: $parquetOutputPath"
    Write-Host "Output CSV: $csvOutputPath"

    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "namenode",
        "hdfs", "dfs", "-rm", "-r", "-f",
        $parquetOutputPath,
        $csvOutputPath
    ) | Out-Null

    $analysisSql = @"
USE flight_delay;
DROP TABLE IF EXISTS flights_clean;
CREATE EXTERNAL TABLE flights_clean (
  year INT,
  month INT,
  day_of_month INT,
  day_of_week INT,
  fl_date DATE,
  op_unique_carrier STRING,
  op_carrier_fl_num INT,
  origin STRING,
  origin_city_name STRING,
  origin_state_nm STRING,
  dest STRING,
  dest_city_name STRING,
  dest_state_nm STRING,
  route STRING,
  dep_delay DOUBLE,
  arr_delay DOUBLE,
  cancelled INT,
  is_cancelled INT,
  cancellation_code STRING,
  distance DOUBLE,
  carrier_delay DOUBLE,
  weather_delay DOUBLE,
  nas_delay DOUBLE,
  security_delay DOUBLE,
  late_aircraft_delay DOUBLE,
  primary_disruption_cause STRING,
  departure_delay_band STRING
)
STORED AS PARQUET
LOCATION '$currentInputPath';

DROP TABLE IF EXISTS $outputTable;
CREATE TABLE $outputTable
STORED AS PARQUET
LOCATION '$parquetOutputPath'
AS
WITH analysis_input AS (
  SELECT *
  FROM flights_clean
)
SELECT
  op_unique_carrier,
  route,
  COUNT(*) AS flight_count,
  MIN(arr_delay) AS min_arr_delay,
  MAX(arr_delay) AS max_arr_delay,
  ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
  ROUND(AVG(is_cancelled), 4) AS cancellation_rate,
  regexp_replace(
    concat_ws(',', sort_array(collect_set(lpad(CAST(month AS STRING), 2, '0')))),
    '(^|,)0',
    '`$1'
  ) AS operating_months
FROM analysis_input
GROUP BY op_unique_carrier, route;

INSERT OVERWRITE DIRECTORY '$csvOutputPath'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
  op_unique_carrier,
  route,
  flight_count,
  min_arr_delay,
  max_arr_delay,
  avg_arr_delay,
  cancellation_rate,
  operating_months
FROM $outputTable
ORDER BY op_unique_carrier, route;

SELECT COUNT(*) AS output_rows FROM $outputTable;
SELECT *
FROM $outputTable
ORDER BY op_unique_carrier, route
LIMIT 10;
"@

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    Invoke-BeelineChecked $analysisSql
    $stopwatch.Stop()

    $outputRows = Get-HiveCount $outputTable
    $elapsedSeconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
    $timingPath = "$BenchmarkRoot/timings.csv"

    Save-TimingRow `
        -TimingPath $timingPath `
        -Analysis "analysis_3_1" `
        -Technology "hive" `
        -CurrentRunSize $currentRunSize `
        -CurrentInputPath $currentInputPath `
        -CurrentOutputPath "$OutputRoot/$currentRunSize" `
        -ExecutionTimeSeconds $elapsedSeconds `
        -OutputRows $outputRows `
        -RunTimestamp $runTimestamp

    if ($CompareWithSparkSql) {
        $sparkSqlParquetPath = "$hdfsNameNode/outputs/analysis_3_1/spark_sql/$currentRunSize/parquet"
        Invoke-DockerChecked @(
            "compose", "--env-file", ".env",
            "exec", "-T", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", $sparkMasterUrl,
            "--conf", "spark.hadoop.fs.defaultFS=$hdfsNameNode",
            "/opt/project/src/compare_analysis_outputs.py",
            "--left", $qualifiedParquetOutputPath,
            "--right", $sparkSqlParquetPath,
            "--left-label", "hive",
            "--right-label", "spark_sql"
        ) | Out-Null
    }

    Write-Host "Run completata: $currentRunSize, secondi=$elapsedSeconds, output_rows=$outputRows"
    Write-Host "Timing CSV HDFS: $timingPath"
}
