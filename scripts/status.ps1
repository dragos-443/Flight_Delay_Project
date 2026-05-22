$ErrorActionPreference = "Stop"

docker compose --env-file .env ps
if ($LASTEXITCODE -ne 0) {
    throw "Comando Docker fallito: docker compose --env-file .env ps"
}
