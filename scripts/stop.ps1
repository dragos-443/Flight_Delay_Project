param(
    [switch]$Volumes
)

$ErrorActionPreference = "Stop"

function Invoke-Docker {
    param([string[]]$Arguments)

    docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Comando Docker fallito: docker $($Arguments -join ' ')"
    }
}

$args = @("compose", "--env-file", ".env", "down")
if ($Volumes) {
    $args += "-v"
}

Invoke-Docker $args
