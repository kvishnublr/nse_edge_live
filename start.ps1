# NSE EDGE v5 — Windows launcher (UI: http://127.0.0.1:8000/ when PORT=8000)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Set-Location backend
if (-not $env:PORT) { $env:PORT = "8000" }
Write-Host ""
Write-Host "  NSE EDGE — starting backend (PORT=$env:PORT)"
Write-Host "  Open in browser:  http://127.0.0.1:$env:PORT/"
Write-Host ""
python main.py
