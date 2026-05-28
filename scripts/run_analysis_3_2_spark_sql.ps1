param(
    [ValidateSet("100k", "500k", "half", "full", "all")]
    [string]$RunSize = "full",
    [string]$InputPath = $null,
    [string]$OutputRoot = $null,
    [string]$BenchmarkRoot = $null
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

    $localDir = Join-Path "outputs" "benchmarks\analysis_3_2\spark_sql"
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
    $containerTimingPath = "/tmp/analysis_3_2_spark_sql_timings.csv"

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

$hdfsNameNode = Read-DotEnvValue ".env" "HDFS_NAMENODE"
$hdfsProcessedDir = Read-DotEnvValue ".env" "HDFS_PROCESSED_DIR"
$sparkMasterUrl = Read-DotEnvValue ".env" "SPARK_MASTER_URL"

if (-not $hdfsNameNode) { $hdfsNameNode = "hdfs://namenode:8020" }
if (-not $hdfsProcessedDir) { $hdfsProcessedDir = "/data/processed" }
if (-not $sparkMasterUrl) { $sparkMasterUrl = "spark://spark-master:7077" }

if (-not $InputPath) {
    $InputPath = "$hdfsNameNode$hdfsProcessedDir/flights_2024_clean.parquet"
}

if (-not $OutputRoot) {
    $OutputRoot = "$hdfsNameNode/outputs/analysis_3_2/spark_sql"
}

if (-not $BenchmarkRoot) {
    $BenchmarkRoot = "/outputs/benchmarks/analysis_3_2/spark_sql"
}

$runSizes = @($RunSize)
if ($RunSize -eq "all") {
    $runSizes = @("100k", "500k", "half", "full")
    $localTimingPath = Join-Path "outputs" "benchmarks\analysis_3_2\spark_sql\timings.csv"
    Remove-Item -LiteralPath $localTimingPath -ErrorAction SilentlyContinue
    Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "namenode",
        "hdfs", "dfs", "-rm", "-f",
        "$BenchmarkRoot/timings.csv"
    ) | Out-Null
}

foreach ($currentRunSize in $runSizes) {
    $parquetOutputPath = "$OutputRoot/$currentRunSize/parquet"
    $csvOutputPath = "$OutputRoot/$currentRunSize/csv"
    $runTimestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    Write-Host "Eseguo analisi 3.2 Spark SQL ($currentRunSize)"
    Write-Host "Input: $InputPath"
    Write-Host "Output Parquet: $parquetOutputPath"
    Write-Host "Output CSV: $csvOutputPath"

    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $sparkOutput = Invoke-DockerChecked @(
        "compose", "--env-file", ".env",
        "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", $sparkMasterUrl,
        "--conf", "spark.hadoop.fs.defaultFS=$hdfsNameNode",
        "/opt/project/src/analysis_3_2_spark_sql.py",
        "--input", $InputPath,
        "--parquet-output", $parquetOutputPath,
        "--csv-output", $csvOutputPath,
        "--run-size", $currentRunSize
    )
    $stopwatch.Stop()

    $outputRowsLine = $sparkOutput | Where-Object { $_ -match "^output_rows=" } | Select-Object -Last 1
    if (-not $outputRowsLine) {
        throw "Impossibile leggere output_rows dall'output Spark."
    }

    $outputRows = [int]($outputRowsLine -replace "^output_rows=", "")
    $elapsedSeconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 3)
    $timingPath = "$BenchmarkRoot/timings.csv"

    Save-TimingRow `
        -TimingPath $timingPath `
        -Analysis "analysis_3_2" `
        -Technology "spark_sql" `
        -CurrentRunSize $currentRunSize `
        -CurrentInputPath $InputPath `
        -CurrentOutputPath "$OutputRoot/$currentRunSize" `
        -ExecutionTimeSeconds $elapsedSeconds `
        -OutputRows $outputRows `
        -RunTimestamp $runTimestamp

    Write-Host "Run completata: $currentRunSize, secondi=$elapsedSeconds, output_rows=$outputRows"
    Write-Host "Timing CSV HDFS: $timingPath"
}
