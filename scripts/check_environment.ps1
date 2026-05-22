$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$Arguments)

    docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Comando Docker fallito: docker $($Arguments -join ' ')"
    }
}

Write-Host "Container attivi:"
Invoke-Docker @("compose", "--env-file", ".env", "ps")

Write-Host ""
Write-Host "Verifica HDFS:"
Invoke-Docker @("compose", "--env-file", ".env", "exec", "-T", "namenode", "hdfs", "dfs", "-ls", "/")

Write-Host ""
Write-Host "Verifica Spark:"
Invoke-Docker @("compose", "--env-file", ".env", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit", "--version")

Write-Host ""
Write-Host "Verifica Hive:"
$hiveReady = $false
for ($attempt = 1; $attempt -le 12; $attempt++) {
    docker compose --env-file .env exec -T hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES;"
    if ($LASTEXITCODE -eq 0) {
        $hiveReady = $true
        break
    }

    Write-Host "Hive non ancora pronto, nuovo tentativo tra 10 secondi ($attempt/12)..."
    Start-Sleep -Seconds 10
}

if (-not $hiveReady) {
    throw "Hive non disponibile dopo 12 tentativi."
}
