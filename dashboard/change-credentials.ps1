# BlockDAG credential rotation
# Updates NODE_RPC_USER, NODE_RPC_PASS, POSTGRES_USER, POSTGRES_PASSWORD
# in .env, updates the dashboard HTML defaults, then restarts the stack.
#
# Run from C:\blockdag\dashboard:
#   powershell -ExecutionPolicy Bypass -File C:\blockdag\dashboard\change-credentials.ps1

$ErrorActionPreference = "Stop"
$blockdagDir  = "C:\blockdag"
$envFile      = Join-Path $blockdagDir ".env"
$dashFile     = "C:\blockdag\dashboard\blockdag-dashboard.html"

if (-not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile"; exit 1
}

# ── Prompt for new credentials ────────────────────────────────────────────────
Write-Host ""
Write-Host "BlockDAG credential rotation"
Write-Host "────────────────────────────"
Write-Host "Press Enter to keep the current value shown in brackets."
Write-Host ""

$envContent = Get-Content $envFile -Raw

function GetCurrent($key) {
    if ($envContent -match "(?m)^$key=(.+)$") { return $Matches[1].Trim() }
    return ""
}

$curRpcUser  = GetCurrent "NODE_RPC_USER"
$curRpcPass  = GetCurrent "NODE_RPC_PASS"
$curPgUser   = GetCurrent "POSTGRES_USER"
$curPgPass   = GetCurrent "POSTGRES_PASSWORD"

$newRpcUser = Read-Host "RPC username   [$curRpcUser]"
$newRpcPass = Read-Host "RPC password   [$curRpcPass]"
$newPgUser  = Read-Host "Postgres user  [$curPgUser]"
$newPgPass  = Read-Host "Postgres pass  [$curPgPass]"

# Fall back to current if left blank
if (-not $newRpcUser) { $newRpcUser = $curRpcUser }
if (-not $newRpcPass) { $newRpcPass = $curRpcPass }
if (-not $newPgUser)  { $newPgUser  = $curPgUser  }
if (-not $newPgPass)  { $newPgPass  = $curPgPass  }

Write-Host ""
Write-Host "Applying:"
Write-Host "  RPC  : $newRpcUser / $newRpcPass"
Write-Host "  Postgres: $newPgUser / $newPgPass"
Write-Host ""

# ── Update .env ───────────────────────────────────────────────────────────────
$env2 = $envContent
$env2 = $env2 -replace "(?m)^NODE_RPC_USER=.*$",     "NODE_RPC_USER=$newRpcUser"
$env2 = $env2 -replace "(?m)^NODE_RPC_PASS=.*$",     "NODE_RPC_PASS=$newRpcPass"
$env2 = $env2 -replace "(?m)^POSTGRES_USER=.*$",     "POSTGRES_USER=$newPgUser"
$env2 = $env2 -replace "(?m)^POSTGRES_PASSWORD=.*$", "POSTGRES_PASSWORD=$newPgPass"
$env2 = $env2 -replace "(?m)^PG_URL=.*$",            "PG_URL=postgres://${newPgUser}:${newPgPass}@pool-db:5432/pool"
Set-Content -Path $envFile -Value $env2 -NoNewline
Write-Host ".env updated."

# ── Update dashboard HTML defaults ────────────────────────────────────────────
if (Test-Path $dashFile) {
    $html = Get-Content $dashFile -Raw
    $html = $html -replace "(?<=id=`"cfg-user`" value=`")[^`"]*(?=`")", $newRpcUser
    $html = $html -replace "(?<=id=`"cfg-pass`" value=`")[^`"]*(?=`")", $newRpcPass
    Set-Content -Path $dashFile -Value $html -NoNewline
    Write-Host "Dashboard HTML defaults updated."
}

# ── Restart the stack ─────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Restarting Docker stack to apply new credentials..."
Set-Location $blockdagDir
docker compose up -d
Write-Host ""
Write-Host "Done. If the dashboard server is running, restart it too:"
Write-Host "  Ctrl+C the running python blockdag-dashboard-server.py, then restart it."
