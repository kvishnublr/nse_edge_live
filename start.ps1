# NSE EDGE v5 — Windows launcher (matches frontend localhost:8765)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Set-Location backend
if (-not $env:NSE_EDGE_PORT) { $env:NSE_EDGE_PORT = "8765" }
Write-Host "Starting backend on port $env:NSE_EDGE_PORT — open frontend\index.html"
python main.py
