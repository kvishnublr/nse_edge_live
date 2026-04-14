# STOCKR.IN v5 - Windows launcher (UI at 127.0.0.1:8000 when PORT=8000)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
Set-Location backend
if (-not $env:PORT) { $env:PORT = "8000" }
Write-Host ""
Write-Host ('  STOCKR.IN - starting backend (PORT=' + $env:PORT + ')')
Write-Host ('  Open in browser:  http://127.0.0.1:' + $env:PORT + '/')
Write-Host ""
Write-Host "  Tip: After pulling new code, stop this window (Ctrl+C) and run start.ps1 again."
Write-Host "       PLAYBOOK: boot log should show 'Mounted playbook-design API'."
Write-Host "       Quick test: http://127.0.0.1:$($env:PORT)/api/playbook-design/ping  -> {\"ok\":true,\"playbook_routes\":true}"
Write-Host "       If ping is 404, another program is on this port or an old server is still running."
Write-Host ""
python main.py
