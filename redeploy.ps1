# NSE EDGE — redeploy backend (Fly.io). GitHub Pages UI targets https://nse-edge-backend.fly.dev
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Get-FlyAppFromToml {
  $ft = Join-Path $PSScriptRoot "fly.toml"
  if (-not (Test-Path $ft)) { return $null }
  foreach ($ln in Get-Content $ft) {
    if ($ln -match '^\s*app\s*=\s*"([^"]+)"') { return $Matches[1] }
  }
  return $null
}

$app = if ($env:FLY_APP) { $env:FLY_APP } else { Get-FlyAppFromToml }
if (-not $app) {
  Write-Error "No app name: set FLY_APP or add app = `"name`" to fly.toml"
  exit 1
}

$exe = $null
if (Get-Command flyctl -ErrorAction SilentlyContinue) { $exe = "flyctl" }
elseif (Get-Command fly -ErrorAction SilentlyContinue) { $exe = "fly" }
else {
  Write-Host "Install Fly CLI: winget install Fly.io.flyctl"
  exit 1
}

Write-Host "Deploying Fly app: $app ($exe deploy)"
& $exe deploy --app $app --ha=false
if ($LASTEXITCODE -ne 0) {
  Write-Host ""
  Write-Host "If 'app not found': run '$exe apps list', then set FLY_APP=<name> or edit fly.toml."
  exit $LASTEXITCODE
}

Write-Host "OK — open https://$app.fly.dev/api/intra-index/health"
