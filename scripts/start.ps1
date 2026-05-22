param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$Arguments)

    docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Comando Docker fallito: docker $($Arguments -join ' ')"
    }
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Creato .env a partire da .env.example"
}

$args = @("compose", "--env-file", ".env", "up", "-d")
if ($Build) {
    $args += "--build"
}

Invoke-Docker $args
