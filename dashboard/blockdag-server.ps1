# BlockDAG Dashboard Server (PowerShell — no installs required)
# Run:  powershell -ExecutionPolicy Bypass -File C:\blockdag\dashboard\blockdag-server.ps1
# Open: http://localhost:8088

$PORT       = 8088
$RPC_URL    = "http://localhost:38131"
$NODE1_DATA = "C:\blockdag\data\node1"
$NODE2_DATA = "C:\blockdag\data\node2"
$BACKUP_DIR = "C:\blockdag\data-restore\backups"
$DASH_FILE  = Join-Path $PSScriptRoot "blockdag-dashboard.html"
$CONTAINERS = @("bdag-miner-node-1","bdag-miner-node-2","asic-pool","rpc-failover","pool-db")

# ── Helpers ───────────────────────────────────────────────────────────────────
function Write-Json($ctx, $obj, [int]$code = 200) {
    $json  = $obj | ConvertTo-Json -Depth 5 -Compress
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $ctx.Response.StatusCode  = $code
    $ctx.Response.ContentType = "application/json"
    $ctx.Response.Headers.Add("Access-Control-Allow-Origin", "*")
    $ctx.Response.OutputStream.Write($bytes, 0, $bytes.Length)
    $ctx.Response.Close()
}

function Read-Body($req) {
    if ($req.ContentLength64 -le 0) { return [byte[]]@() }
    $buf = New-Object byte[] $req.ContentLength64
    $req.InputStream.Read($buf, 0, $buf.Length) | Out-Null
    return $buf
}

# ── Request handler ───────────────────────────────────────────────────────────
function Handle($ctx) {
    $req    = $ctx.Request
    $resp   = $ctx.Response
    $method = $req.HttpMethod
    $path   = $req.Url.AbsolutePath

    # CORS preflight
    if ($method -eq "OPTIONS") {
        $resp.Headers.Add("Access-Control-Allow-Origin",  "*")
        $resp.Headers.Add("Access-Control-Allow-Headers", "Content-Type,Authorization")
        $resp.Headers.Add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        $resp.StatusCode = 200; $resp.Close(); return
    }

    # GET / — serve dashboard HTML
    if ($method -eq "GET" -and $path -in "/","/index.html") {
        if (Test-Path $DASH_FILE) {
            $bytes = [System.IO.File]::ReadAllBytes($DASH_FILE)
            $resp.StatusCode  = 200
            $resp.ContentType = "text/html; charset=utf-8"
            $resp.OutputStream.Write($bytes, 0, $bytes.Length)
        } else {
            $resp.StatusCode = 404
        }
        $resp.Close(); return
    }

    # GET /containers — live container states via docker inspect
    if ($method -eq "GET" -and $path -eq "/containers") {
        $result = @{}
        foreach ($c in $CONTAINERS) {
            $state = docker inspect --format "{{.State.Status}}" $c 2>$null
            $result[$c] = if ($LASTEXITCODE -eq 0) { $state.Trim() } else { "not found" }
        }
        Write-Json $ctx $result; return
    }

    # POST /rpc — proxy to BlockDAG RPC (fixes CORS when opening as file://)
    if ($method -eq "POST" -and $path -eq "/rpc") {
        $body    = Read-Body $req
        $authHdr = $req.Headers["Authorization"]
        try {
            $webReq = [System.Net.WebRequest]::Create($RPC_URL)
            $webReq.Method      = "POST"
            $webReq.ContentType = "application/json"
            $webReq.Timeout     = 15000
            if ($authHdr) { $webReq.Headers["Authorization"] = $authHdr }
            $s = $webReq.GetRequestStream()
            $s.Write($body, 0, $body.Length); $s.Close()
            $webResp   = $webReq.GetResponse()
            $reader    = New-Object System.IO.StreamReader $webResp.GetResponseStream()
            $respBody  = $reader.ReadToEnd(); $reader.Close()
            $bytes     = [System.Text.Encoding]::UTF8.GetBytes($respBody)
            $resp.StatusCode  = 200
            $resp.ContentType = "application/json"
            $resp.Headers.Add("Access-Control-Allow-Origin", "*")
            $resp.OutputStream.Write($bytes, 0, $bytes.Length)
            $resp.Close()
        } catch {
            Write-Json $ctx @{ error = $_.Exception.Message } 502
        }
        return
    }

    # POST /docker/start|stop|restart
    if ($method -eq "POST" -and $path -match "^/docker/(start|stop|restart)$") {
        $action  = $Matches[1]
        $results = @{}
        foreach ($c in $CONTAINERS) {
            $out = & docker $action $c 2>&1
            $results[$c] = if ($LASTEXITCODE -eq 0) { "ok" } else { "$out" }
        }
        Write-Json $ctx @{ ok = $true; results = $results }; return
    }

    # POST /backup — stop node1 (HAProxy backup), copy both nodes, restart
    if ($method -eq "POST" -and $path -eq "/backup") {
        $ts  = Get-Date -Format "yyyyMMdd_HHmmss"
        $dst = Join-Path $BACKUP_DIR "blockdag-backup-$ts"
        try {
            New-Item -ItemType Directory -Path $dst -Force | Out-Null
            docker stop bdag-miner-node-1 2>&1 | Out-Null
            Copy-Item -Path $NODE1_DATA -Destination (Join-Path $dst "node1") -Recurse -Force
            docker start bdag-miner-node-1 2>&1 | Out-Null
            # node2 stays up (RPC primary) — warm copy
            Copy-Item -Path $NODE2_DATA -Destination (Join-Path $dst "node2") -Recurse -Force
            Write-Json $ctx @{ ok = $true; path = $dst }
        } catch {
            Write-Json $ctx @{ ok = $false; error = $_.Exception.Message } 500
        }
        return
    }

    $resp.StatusCode = 404; $resp.Close()
}

# ── Start listener ────────────────────────────────────────────────────────────
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("http://localhost:$PORT/")
$listener.Start()

Write-Host "BlockDAG Dashboard  ->  http://localhost:$PORT" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop." -ForegroundColor Gray

try {
    while ($listener.IsListening) {
        $ctx = $listener.GetContext()
        Handle $ctx
    }
} finally {
    $listener.Stop()
}
