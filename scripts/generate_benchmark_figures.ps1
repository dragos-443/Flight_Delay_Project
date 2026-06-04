$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $projectRoot "src\generate_benchmark_figures.py"

Write-Host "Genero benchmark summary e grafici SVG dai timing esistenti..."
Write-Host "Se sono presenti timing di scalabilita, genero anche scalability_summary.csv e figure dedicate."
python $scriptPath
Write-Host "Generazione completata."
