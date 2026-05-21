# BlockDAG chain data backup
# Stops node1 (HAProxy backup node) for a clean copy, warm-copies node2.
# Keeps the last $RETAIN backups and prunes older ones.
# Run manually or via Task Scheduler (setup-tasks.ps1).
#
# Parameters:
#   -Schedule  1 or 2 (default 1) — selects which retention count to use

param([int]$Schedule = 1)

$NODE1_DATA   = "C:\blockdag\data\node1"
$NODE2_DATA   = "C:\blockdag\data\node2"
$BACKUP_DIR   = "C:\blockdag\data-restore\backups"
$LOG_FILE     = "C:\blockdag\data-restore\backup.log"
$CONFIG_FILE  = "C:\blockdag\dashboard\backup-config.json"

$retainKey = if ($Schedule -eq 2) { "retain_copies_2" } else { "retain_copies_1" }
$RETAIN = 12
if (Test-Path $CONFIG_FILE) {
    try {
        $cfg = Get-Content $CONFIG_FILE -Raw | ConvertFrom-Json
        $val = $cfg.$retainKey
        if ($val) { $RETAIN = [int]$val }
    } catch {}
}

function Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line
}

New-Item -ItemType Directory -Path $BACKUP_DIR -Force | Out-Null

$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
$dst = Join-Path $BACKUP_DIR "blockdag-backup-$ts"
New-Item -ItemType Directory -Path $dst -Force | Out-Null

Log "Starting backup → $dst"

# ── node1: stop for a consistent snapshot ────────────────────────────────────
Log "Stopping bdag-miner-node-1..."
docker stop bdag-miner-node-1 | Out-Null

Log "Copying node1 data..."
robocopy $NODE1_DATA "$dst\node1" /E /MT:8 /R:2 /W:3 /NP /NFL /NDL `
    /LOG+:$LOG_FILE | Out-Null

Log "Restarting bdag-miner-node-1..."
docker start bdag-miner-node-1 | Out-Null

# ── node2: warm copy (stays running, node1 handles RPC during copy) ───────────
Log "Copying node2 data (warm)..."
robocopy $NODE2_DATA "$dst\node2" /E /MT:8 /R:2 /W:3 /NP /NFL /NDL `
    /LOG+:$LOG_FILE | Out-Null

Log "Backup complete: $dst"

# ── Prune old backups ─────────────────────────────────────────────────────────
$all = Get-ChildItem $BACKUP_DIR -Directory | Sort-Object Name
if ($all.Count -gt $RETAIN) {
    $toDelete = $all | Select-Object -First ($all.Count - $RETAIN)
    foreach ($d in $toDelete) {
        Log "Pruning old backup: $($d.Name)"
        Remove-Item $d.FullName -Recurse -Force
    }
}

Log "Done. Backups retained: $(([Math]::Min($all.Count, $RETAIN)))"

# robocopy exits 1 for "files copied successfully"; Task Scheduler treats non-zero as failure
exit 0
