# BlockDAG Windows Task Scheduler setup
# Registers two tasks:
#   1. BlockDAG-Backup    - hourly chain data backup
#   2. BlockDAG-Dashboard - start dashboard server at login
#
# Run once from an Administrator PowerShell:
#   powershell -ExecutionPolicy Bypass -File "C:\blockdag node\dashboard\setup-tasks.ps1"

$ErrorActionPreference = "Stop"

$blockdagDir  = "C:\blockdag node\dashboard"
$backupScript = "C:\blockdag node\dashboard\blockdag-backup.ps1"
$serverScript = "C:\blockdag node\dashboard\blockdag-dashboard-server.py"
$psExe        = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

# Locate python.exe (avoid Windows Store stub)
# Try known install paths first, then fall back to PATH detection
$pythonExe = $null
$knownPaths = @(
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "C:\Python314\python.exe",
    "C:\Python313\python.exe",
    "C:\Python312\python.exe"
)
foreach ($p in $knownPaths) {
    if (Test-Path $p) { $pythonExe = $p; break }
}
if (-not $pythonExe) {
    try {
        $found = (Get-Command python -ErrorAction Stop).Source
        if ($found -notlike "*WindowsApps*") {
            $pythonExe = $found
        }
    } catch {}
}

$skipDashboard = (-not $pythonExe -or -not (Test-Path $pythonExe))
if ($skipDashboard) {
    Write-Warning "Could not locate python.exe - dashboard task will be skipped."
}

# ── Task 1: Hourly backup ─────────────────────────────────────────────────────
Write-Host "Registering BlockDAG-Backup task..."

$backup1Arg = '-NonInteractive -ExecutionPolicy Bypass -File "' + $backupScript + '" -Schedule 1'
$backup2Arg = '-NonInteractive -ExecutionPolicy Bypass -File "' + $backupScript + '" -Schedule 2'

$backupAction = New-ScheduledTaskAction `
    -Execute $psExe `
    -Argument $backup1Arg `
    -WorkingDirectory $blockdagDir

$backup2Action = New-ScheduledTaskAction `
    -Execute $psExe `
    -Argument $backup2Arg `
    -WorkingDirectory $blockdagDir

$backupTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval ([TimeSpan]::FromHours(1)) `
    -RepetitionDuration ([TimeSpan]::MaxValue)

$backupSettings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::FromHours(2)) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName "BlockDAG-Backup" `
    -Action $backupAction `
    -Trigger $backupTrigger `
    -Settings $backupSettings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "  BlockDAG-Backup registered - runs every hour."

# ── Task 2: Second backup (weekly default, adjustable from dashboard) ─────────
Write-Host "Registering BlockDAG-Backup-2 task..."

$backup2Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval ([TimeSpan]::FromDays(7)) `
    -RepetitionDuration ([TimeSpan]::MaxValue)

$backup2Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::FromHours(2)) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName "BlockDAG-Backup-2" `
    -Action $backup2Action `
    -Trigger $backup2Trigger `
    -Settings $backup2Settings `
    -RunLevel Highest `
    -Force | Out-Null

Write-Host "  BlockDAG-Backup-2 registered - runs every 7 days (adjustable from dashboard)."

# ── Task 3: Dashboard server at login ─────────────────────────────────────────
if (-not $skipDashboard) {
    Write-Host "Registering BlockDAG-Dashboard task..."

    # Launch via PowerShell so we can set PYTHONIOENCODING before starting Python
    $dashArg = '-NonInteractive -Command "$env:PYTHONIOENCODING = ''utf-8''; ' +
               '& ''' + $pythonExe + ''' ''' + $serverScript + '''"'

    $dashAction = New-ScheduledTaskAction `
        -Execute $psExe `
        -Argument $dashArg `
        -WorkingDirectory $blockdagDir

    $dashTrigger = New-ScheduledTaskTrigger -AtLogOn
    $dashTrigger.Delay = "PT30S"

    $dashSettings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit 0 `
        -RestartCount 5 `
        -RestartInterval ([TimeSpan]::FromMinutes(2)) `
        -MultipleInstances IgnoreNew

    Register-ScheduledTask `
        -TaskName "BlockDAG-Dashboard" `
        -Action $dashAction `
        -Trigger $dashTrigger `
        -Settings $dashSettings `
        -RunLevel Highest `
        -Force | Out-Null

    Write-Host "  BlockDAG-Dashboard registered - starts 30s after login."
    Write-Host "  Dashboard will be at http://localhost:8088"
}

Write-Host ""
Write-Host "Done. To verify:"
Write-Host "  Get-ScheduledTask -TaskName BlockDAG-Backup"
Write-Host "  Get-ScheduledTask -TaskName BlockDAG-Backup-2"
Write-Host "  Get-ScheduledTask -TaskName BlockDAG-Dashboard"
Write-Host ""
Write-Host "To run a backup immediately:"
Write-Host "  Start-ScheduledTask -TaskName BlockDAG-Backup"
Write-Host "  Start-ScheduledTask -TaskName BlockDAG-Backup-2"
