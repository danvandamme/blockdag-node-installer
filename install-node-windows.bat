@echo off
REM ============================================================================
REM BlockDAG Node Installer
REM Installs the full BlockDAG mining node stack via Docker.
REM
REM Bundled files (must be in the same folder as this .bat):
REM   docker-compose.yml        Full stack definition
REM   .env.example              Configuration template
REM   haproxy.cfg               RPC load-balancer config
REM   asic-pool\.env.example    Pool-specific config template
REM   asic-pool\schema.sql      Pool database schema
REM   bdag-release-asic-pool.tar   (optional) Pool Docker image
REM   bdag-release-node.tar        (optional) Node Docker image
REM
REM Usage: Double-click this file, or run from a command prompt.
REM ============================================================================
setlocal enabledelayedexpansion
cd /d "%~dp0"

set "VERSION=DVD-2026.0526.3"
set "INSTALL_DIR=C:\blockdag node"

set "POOL_IMAGE=bdag-release/asic-pool:local"
set "NODE_IMAGE=bdag-release/node:local"

echo.
echo   =====================================================
echo     BlockDAG Node Installer  v%VERSION%
echo     dagtech.network
echo   =====================================================
echo.

REM ============================================================================
REM 0. Administrator elevation
REM    Required for Defender exclusions and firewall rules.
REM    Re-launches this script as Administrator if not already elevated.
REM ============================================================================
net session >nul 2>&1
if errorlevel 1 (
    echo [Node] Administrator rights required - click Yes on the UAC prompt.
    echo [Node] The installer will continue in a new window.
    set "_SELF=%~f0"
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process cmd.exe -Verb RunAs -ArgumentList ('/d /k ' + [char]34 + $env:_SELF + [char]34)"
    exit /b
)

REM ============================================================================
REM 0b. Windows Defender exclusions
REM     Added before any file copies so nothing is flagged mid-install.
REM ============================================================================
echo [Node] Configuring Windows Defender exclusions...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Add-MpPreference -ExclusionPath '%INSTALL_DIR%' -ErrorAction SilentlyContinue; Add-MpPreference -ExclusionPath '%~dp0' -ErrorAction SilentlyContinue" >nul 2>&1
echo [Node] Defender exclusions set.

REM ============================================================================
REM 1. Verify all bundled files are present
REM ============================================================================
echo [Node] Checking bundled files...
set "MISSING=0"
for %%f in (
    "docker-compose.yml"
    ".env.example"
    "haproxy.cfg"
    "asic-pool\.env.example"
    "asic-pool\schema.sql"
) do (
    if not exist "%~dp0%%~f" (
        echo [Node] ERROR: Missing required file: %%~f
        set "MISSING=1"
    )
)
if "%MISSING%"=="1" (
    echo.
    echo   One or more required files are missing from the installer folder.
    echo   Please re-download the installer package and try again.
    pause & exit /b 1
)
echo [Node] All required files present.

REM ============================================================================
REM 1b. Disk space check - warn if C: has less than 100 GB free
REM ============================================================================
echo [Node] Checking available disk space...
for /f "tokens=*" %%s in ('powershell -NoProfile -Command "(Get-PSDrive C).Free" 2^>nul') do set "FREE_BYTES=%%s"
if defined FREE_BYTES (
    for /f "tokens=*" %%g in ('powershell -NoProfile -Command "[math]::Round(%FREE_BYTES% / 1GB, 1)" 2^>nul') do set "FREE_GB=%%g"
    echo [Node] Free space on C: !FREE_GB! GB
    for /f "tokens=*" %%c in ('powershell -NoProfile -Command "if (%FREE_BYTES% -lt 100GB) { 'LOW' } else { 'OK' }" 2^>nul') do set "SPACE_CHECK=%%c"
    if "!SPACE_CHECK!"=="LOW" (
        echo.
        echo   [WARN] Only !FREE_GB! GB free on C: -- 100 GB or more is recommended.
        echo          Chain data grows over time. Running low on space may cause
        echo          the node or database to fail unexpectedly.
        echo.
        echo   Press any key to continue anyway, or close this window to cancel.
        pause >nul
    )
) else (
    echo [Node] Could not determine free space - skipping check.
)

REM ============================================================================
REM 2. Check Docker - auto-start if not running
REM ============================================================================
echo [Node] Checking Docker...
docker info >nul 2>&1
if not errorlevel 1 goto :docker_ready

echo.
echo   Docker is not running. Attempting to start Docker Desktop...

REM -- Check standard install locations (system-wide, then per-user) --
set "DOCKER_EXE="
if exist "C:\Program Files\Docker\Docker\Docker Desktop.exe" set "DOCKER_EXE=C:\Program Files\Docker\Docker\Docker Desktop.exe"
if not defined DOCKER_EXE if exist "%LOCALAPPDATA%\Programs\Docker\Docker\Docker Desktop.exe" set "DOCKER_EXE=%LOCALAPPDATA%\Programs\Docker\Docker\Docker Desktop.exe"

if not defined DOCKER_EXE (
    echo.
    echo   [ERROR] Docker Desktop does not appear to be installed.
    echo   Download and install it from: https://www.docker.com/products/docker-desktop
    echo   Then re-run this installer.
    echo.
    pause & exit /b 1
)

echo   Starting: !DOCKER_EXE!
start "" "!DOCKER_EXE!"
echo.
echo   Waiting for Docker to become ready (up to 60 seconds^)...
set "DOCKER_READY=0"
for /l %%i in (1,1,30) do (
    if "!DOCKER_READY!"=="0" (
        docker info >nul 2>&1
        if not errorlevel 1 set "DOCKER_READY=1"
        if "!DOCKER_READY!"=="0" (
            powershell -NoProfile -Command "Start-Sleep -Seconds 2" >nul 2>&1
            <nul set /p "=."
        )
    )
)
echo.
if "!DOCKER_READY!"=="0" (
    echo.
    echo   [ERROR] Docker did not become ready in time.
    echo   Please wait for Docker Desktop to fully start (whale icon in system tray^)
    echo   then re-run this installer.
    echo.
    pause & exit /b 1
)
echo   Docker is ready.

:docker_ready
echo [Node] Docker is running.

REM ============================================================================
REM 2b. Check for existing containers - prompt to overwrite
REM ============================================================================
echo [Node] Checking for existing BlockDAG containers...
set "CONTAINERS_EXIST=0"
for %%c in (asic-pool bdag-miner-node-1 bdag-miner-node-2 pool-db rpc-failover) do (
    docker ps -a --filter "name=%%c" --format "{{.Names}}" 2>nul | findstr /i "%%c" >nul 2>&1
    if not errorlevel 1 set "CONTAINERS_EXIST=1"
)
if "!CONTAINERS_EXIST!"=="1" (
    echo [Node] Existing containers found - stopping them...
    if exist "%INSTALL_DIR%\docker-compose.yml" (
        cd /d "%INSTALL_DIR%"
        docker compose down >nul 2>&1
        cd /d "%~dp0"
    ) else (
        for %%c in (asic-pool bdag-miner-node-1 bdag-miner-node-2 pool-db rpc-failover) do (
            docker rm -f %%c >nul 2>&1
        )
    )
    echo [Node] Existing containers removed.
) else (
    echo [Node] No existing containers found.
)

REM ============================================================================
REM 3. Load Docker images
REM    - If .tar files are in this folder, load them automatically.
REM    - If images are already loaded, skip.
REM    - If neither, show a clear error.
REM ============================================================================
echo [Node] Checking BlockDAG Docker images...

REM -- Pool image --
docker image inspect %POOL_IMAGE% >nul 2>&1
if errorlevel 1 (
    if exist "%~dp0bdag-release-asic-pool.tar" (
        echo [Node] Loading pool image from bdag-release-asic-pool.tar...
        docker load -i "%~dp0bdag-release-asic-pool.tar"
        if errorlevel 1 ( echo [Node] ERROR: Failed to load pool image. & pause & exit /b 1 )
    ) else (
        echo.
        echo   [ERROR] Pool image not found: %POOL_IMAGE%
        echo   Place bdag-release-asic-pool.tar in the same folder as this installer.
        echo   Use save-images.bat on your existing node machine to create that file.
        echo.
        pause & exit /b 1
    )
) else (
    echo [Node] Pool image already loaded.
)

REM -- Node image --
docker image inspect %NODE_IMAGE% >nul 2>&1
if errorlevel 1 (
    if exist "%~dp0bdag-release-node.tar" (
        echo [Node] Loading node image from bdag-release-node.tar...
        docker load -i "%~dp0bdag-release-node.tar"
        if errorlevel 1 ( echo [Node] ERROR: Failed to load node image. & pause & exit /b 1 )
    ) else (
        echo.
        echo   [ERROR] Node image not found: %NODE_IMAGE%
        echo   Place bdag-release-node.tar in the same folder as this installer.
        echo   Use save-images.bat on your existing node machine to create that file.
        echo.
        pause & exit /b 1
    )
) else (
    echo [Node] Node image already loaded.
)
echo [Node] Images ready.

REM ============================================================================
REM 4. Configuration
REM ============================================================================
echo.
echo   ---- Configuration ----
echo.

REM -- Mining wallet address --
:wallet_prompt
set "MINING_ADDRESS="
set /p "MINING_ADDRESS=  Mining wallet address (0x...): "
if "%MINING_ADDRESS%"=="" ( echo   [WARN] Wallet address is required. & goto :wallet_prompt )
for /f "tokens=*" %%v in ('powershell -NoProfile -Command "if ('%MINING_ADDRESS%' -match '^0x[0-9a-fA-F]{40}$') { 'OK' } else { 'BAD' }" 2^>nul') do set "ADDR_CHECK=%%v"
if not "!ADDR_CHECK!"=="OK" (
    echo   [WARN] That does not look like a valid wallet address.
    echo          Expected format: 0x followed by 40 hex characters.
    echo          Example: 0xAbCd1234...
    goto :wallet_prompt
)

REM -- Pool defaults (fee, difficulty, stratum port, password)
REM    All can be changed after install via the dashboard Config window.
set "POOL_FEE=2.0"
set "POOL_DIFF=1.0"
set "POOL_PORT=3334"
set "POOL_PASSWORD="

echo.
echo   Wallet: %MINING_ADDRESS%
echo.

REM ============================================================================
REM 4b. Detect public IP - ask about port forwarding for P2P inbound connections
REM     If the user confirms port forwarding, NODE_EXTERNAL_IP is written to .env
REM     so the node advertises its public address to the BlockDAG P2P network.
REM ============================================================================
echo [Node] Detecting public IP address...
set "SERVER_IP="
set "WRITE_EXTERNAL_IP="

powershell -NoProfile -Command ^
    "try { $ip = (Invoke-WebRequest -Uri 'https://api.ipify.org' -UseBasicParsing -TimeoutSec 5).Content.Trim(); if ($ip -match '^\d+\.\d+\.\d+\.\d+$') { $ip } }" ^
    > "%TEMP%\bdag_ip.txt" 2>nul
if exist "%TEMP%\bdag_ip.txt" set /p SERVER_IP=<"%TEMP%\bdag_ip.txt"
del "%TEMP%\bdag_ip.txt" >nul 2>&1

if not "!SERVER_IP!"=="" (
    echo.
    echo   Detected public IP: !SERVER_IP!
    echo.
    echo   If you have ports 8151 and 8152 forwarded on your router to this PC,
    echo   your node will advertise its public address so the BlockDAG P2P network
    echo   can reach you directly ^(more peers, faster sync^).
    echo.
    call :prompt_port_forward
    echo.
) else (
    REM Fall back to LAN IP for stratum display; P2P external IP left blank
    powershell -NoProfile -Command ^
        "(Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -ne '127.0.0.1' -and $_.IPAddress -notlike '169.254.*' } | ForEach-Object { $a = Get-NetAdapter -InterfaceIndex $_.InterfaceIndex -ErrorAction SilentlyContinue; if ($a -and -not $a.Virtual) { $_ } } | Select-Object -First 1).IPAddress" ^
        > "%TEMP%\bdag_ip.txt" 2>nul
    if exist "%TEMP%\bdag_ip.txt" set /p SERVER_IP=<"%TEMP%\bdag_ip.txt"
    del "%TEMP%\bdag_ip.txt" >nul 2>&1
    echo [Node] Could not detect public IP - P2P external IP will be left blank.
)
if "!SERVER_IP!"=="" set "SERVER_IP=YOUR_SERVER_IP"
goto :port_forward_done

:prompt_port_forward
    set "PF_CHOICE="
    set /p "PF_CHOICE=  Are ports 8151/8152 forwarded on your router to this PC? [Y/N]: "
    if /i "!PF_CHOICE!"=="Y" ( set "WRITE_EXTERNAL_IP=!SERVER_IP!" & exit /b )
    if /i "!PF_CHOICE!"=="N" ( set "WRITE_EXTERNAL_IP=" & exit /b )
    echo   Please enter Y or N.
    goto :prompt_port_forward

:port_forward_done
echo [Node] Server IP: !SERVER_IP!
if not "!WRITE_EXTERNAL_IP!"=="" (
    echo [Node] P2P external IP: !WRITE_EXTERNAL_IP! ^(will be written to .env^)
) else (
    echo [Node] P2P external IP: ^(none - outbound connections only^)
)

REM ============================================================================
REM 5. Create install directory and copy all bundled files
REM ============================================================================
echo [Node] Creating install directory: %INSTALL_DIR%
if not exist "%INSTALL_DIR%"                  mkdir "%INSTALL_DIR%"
if not exist "%INSTALL_DIR%\asic-pool"        mkdir "%INSTALL_DIR%\asic-pool"
REM -- Prompt to keep or overwrite existing chain data --
set "KEEP_CHAIN_DATA=0"
set "CHAIN_DATA_EXISTS=0"
if exist "%INSTALL_DIR%\chain-data\node1\"    set "CHAIN_DATA_EXISTS=1"
if exist "%INSTALL_DIR%\chain-data\node2\"    set "CHAIN_DATA_EXISTS=1"
if exist "%INSTALL_DIR%\chain-data\postgres\" set "CHAIN_DATA_EXISTS=1"
if "!CHAIN_DATA_EXISTS!"=="1" (
    echo.
    echo   [!] Existing chain data found at: %INSTALL_DIR%\chain-data
    echo.
    echo       K = Keep existing data  ^(recommended - preserves your synced chain^)
    echo       O = Overwrite with bundled data  ^(fresh start - replaces current data^)
    echo.
    call :prompt_chain_data
    echo.
)
goto :chain_prompt_end

:prompt_chain_data
    set "CHAIN_CHOICE="
    set /p "CHAIN_CHOICE=  Your choice [K/O]: "
    if /i "!CHAIN_CHOICE!"=="K" ( set "KEEP_CHAIN_DATA=1" & exit /b )
    if /i "!CHAIN_CHOICE!"=="O" ( set "KEEP_CHAIN_DATA=0" & exit /b )
    echo   [WARN] Please enter K or O.
    goto :prompt_chain_data

:chain_prompt_end
if not exist "%INSTALL_DIR%\chain-data\node1"    mkdir "%INSTALL_DIR%\chain-data\node1"
if not exist "%INSTALL_DIR%\chain-data\node2"    mkdir "%INSTALL_DIR%\chain-data\node2"
if not exist "%INSTALL_DIR%\chain-data\postgres" mkdir "%INSTALL_DIR%\chain-data\postgres"
if "!KEEP_CHAIN_DATA!"=="0" (
    if exist "%~dp0chain-data\" (
        echo [Node] Copying bundled chain data...
        xcopy /e /i /y /q "%~dp0chain-data" "%INSTALL_DIR%\chain-data" >nul
    )
) else (
    echo [Node] Keeping existing chain data.
)
if not exist "%INSTALL_DIR%\dashboard"        mkdir "%INSTALL_DIR%\dashboard"
if not exist "%INSTALL_DIR%\ops"              mkdir "%INSTALL_DIR%\ops"
if not exist "%INSTALL_DIR%\src"              mkdir "%INSTALL_DIR%\src"
if not exist "%INSTALL_DIR%\tools"            mkdir "%INSTALL_DIR%\tools"

echo [Node] Copying config files...
copy /y "%~dp0docker-compose.yml"          "%INSTALL_DIR%\docker-compose.yml"
if errorlevel 1 ( echo [Node] ERROR: Failed to copy docker-compose.yml & pause & exit /b 1 )
copy /y "%~dp0haproxy.cfg"                 "%INSTALL_DIR%\haproxy.cfg"
if errorlevel 1 ( echo [Node] ERROR: Failed to copy haproxy.cfg & pause & exit /b 1 )
copy /y "%~dp0asic-pool\schema.sql"        "%INSTALL_DIR%\asic-pool\schema.sql"
if errorlevel 1 ( echo [Node] ERROR: Failed to copy asic-pool\schema.sql & pause & exit /b 1 )
copy /y "%~dp0asic-pool\.env.example"      "%INSTALL_DIR%\asic-pool\.env.example"
if errorlevel 1 ( echo [Node] ERROR: Failed to copy asic-pool\.env.example & pause & exit /b 1 )

echo [Node] Copying optional support files...
if exist "%~dp0MANIFEST.json"       copy /y "%~dp0MANIFEST.json"       "%INSTALL_DIR%\MANIFEST.json"
if exist "%~dp0README.html"         copy /y "%~dp0README.html"         "%INSTALL_DIR%\README.html"
if exist "%~dp0Common Commands.txt" copy /y "%~dp0Common Commands.txt" "%INSTALL_DIR%\Common Commands.txt"

echo [Node] Copying dashboard...
if exist "%~dp0dashboard\" (
    xcopy /e /i /y "%~dp0dashboard" "%INSTALL_DIR%\dashboard"
    if errorlevel 1 ( echo [Node] ERROR: Failed to copy dashboard & pause & exit /b 1 )
)

echo [Node] Copying ops tools...
if exist "%~dp0ops\" (
    xcopy /e /i /y "%~dp0ops" "%INSTALL_DIR%\ops"
    if errorlevel 1 ( echo [Node] ERROR: Failed to copy ops & pause & exit /b 1 )
)

echo [Node] Copying src files...
if exist "%~dp0src\" (
    xcopy /e /i /y "%~dp0src" "%INSTALL_DIR%\src"
    if errorlevel 1 ( echo [Node] ERROR: Failed to copy src & pause & exit /b 1 )
)

echo [Node] Copying tools...
if exist "%~dp0tools\" (
    xcopy /e /i /y "%~dp0tools" "%INSTALL_DIR%\tools"
    if errorlevel 1 ( echo [Node] ERROR: Failed to copy tools & pause & exit /b 1 )
)

REM ============================================================================
REM 6. Write .env files with user's values
REM    Uses PowerShell to substitute values into the .env.example template.
REM ============================================================================
echo [Node] Writing .env...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$src='%~dp0.env.example'; $dst='%INSTALL_DIR%\.env';" ^
    "$lines = Get-Content $src;" ^
    "$lines = $lines -replace '^MINING_ADDRESS=.*',   'MINING_ADDRESS=%MINING_ADDRESS%';" ^
    "$lines = $lines -replace '^POOL_PORT=.*',        'POOL_PORT=%POOL_PORT%';" ^
    "$lines = $lines -replace '^POOL_FEE_PERCENTAGE=.*', 'POOL_FEE_PERCENTAGE=%POOL_FEE%';" ^
    "$lines = $lines -replace '^POOL_STARTING_PDIFF=.*', 'POOL_STARTING_PDIFF=%POOL_DIFF%';" ^
    "$lines = $lines -replace '^BDAG_MINER_POOL_PASSWORD=.*', 'BDAG_MINER_POOL_PASSWORD=%POOL_PASSWORD%';" ^
    "$lines = $lines -replace '^PG_URL=.*', 'PG_URL=postgres://test:test@pool-db:5432/pool';" ^
    "$lines = $lines -replace '^NODE_EXTERNAL_IP=.*', 'NODE_EXTERNAL_IP=%WRITE_EXTERNAL_IP%';" ^
    "$lines = $lines -replace '^INSTALLER_VERSION=.*', 'INSTALLER_VERSION=%VERSION%';" ^
    "$lines | Set-Content $dst -Encoding UTF8"

if errorlevel 1 ( echo [Node] ERROR: Failed to write .env & pause & exit /b 1 )

REM  asic-pool\.env must be the same file (compose reads it for pool-db and pool)
copy /y "%INSTALL_DIR%\.env" "%INSTALL_DIR%\asic-pool\.env" >nul

REM  Substitute @@RPC_AUTH@@ in haproxy.cfg with base64(NODE_RPC_USER:NODE_RPC_PASS)
REM  so the IBD-aware health check can authenticate against the node RPC.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$env_content = Get-Content '%INSTALL_DIR%\.env';" ^
    "$user = ($env_content | Select-String '^NODE_RPC_USER=(.+)').Matches[0].Groups[1].Value;" ^
    "$pass = ($env_content | Select-String '^NODE_RPC_PASS=(.+)').Matches[0].Groups[1].Value;" ^
    "$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(\"${user}:${pass}\"));" ^
    "(Get-Content '%INSTALL_DIR%\haproxy.cfg' -Raw) -replace '@@RPC_AUTH@@', $auth | Set-Content '%INSTALL_DIR%\haproxy.cfg' -NoNewline"

if errorlevel 1 ( echo [Node] ERROR: Failed to patch haproxy.cfg auth token & pause & exit /b 1 )

echo [Node] Configuration files written.

REM ============================================================================
REM 7. Detect Python
REM ============================================================================
set "PYTHON_EXE="
for %%p in (python py python3) do (
    if not defined PYTHON_EXE (
        %%p --version >nul 2>&1
        if not errorlevel 1 set "PYTHON_EXE=%%p"
    )
)
if defined PYTHON_EXE (
    echo [Node] Python found: !PYTHON_EXE!
) else (
    echo [Node] Python not found - dashboard will not be available.
    echo         Install Python from https://www.python.org/downloads/
    echo         then run the Dashboard shortcut manually.
)

REM ============================================================================
REM 8. Write management scripts using PowerShell (handles spaces in paths)
REM ============================================================================
echo [Node] Writing management scripts...
if not exist "%INSTALL_DIR%\shortcuts" mkdir "%INSTALL_DIR%\shortcuts"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$d='%INSTALL_DIR%\shortcuts';" ^
    "[IO.File]::WriteAllText(\"$d\start-node.bat\", \"@echo off`r`ncd /d `\"%INSTALL_DIR%`\"`r`necho Starting BlockDAG node stack...`r`ndocker compose up -d`r`nif errorlevel 1 ( echo Start failed. Check Docker is running. ^& pause ^& exit /b 1 )`r`necho.`r`necho Stack started!`r`necho   Stratum  : !SERVER_IP!:%POOL_PORT%`r`necho   Dashboard: http://localhost:8088`r`necho.`r`npause`r`n\", [Text.Encoding]::ASCII);" ^
    "[IO.File]::WriteAllText(\"$d\stop-node.bat\",  \"@echo off`r`ncd /d `\"%INSTALL_DIR%`\"`r`necho Stopping BlockDAG node stack...`r`ndocker compose down`r`necho Stack stopped.`r`npause`r`n\", [Text.Encoding]::ASCII);" ^
    "[IO.File]::WriteAllText(\"$d\node-status.bat\", \"@echo off`r`ncd /d `\"%INSTALL_DIR%`\"`r`necho Stack status:`r`necho.`r`ndocker compose ps`r`necho.`r`necho Pool logs (last 30 lines):`r`ndocker compose logs --tail=30 asic-pool`r`npause`r`n\", [Text.Encoding]::ASCII);" ^
    "[IO.File]::WriteAllText(\"$d\node-logs.bat\",   \"@echo off`r`ncd /d `\"%INSTALL_DIR%`\"`r`necho Live logs - press Ctrl+C to exit`r`ndocker compose logs -f`r`n\", [Text.Encoding]::ASCII);"
if errorlevel 1 ( echo [Node] ERROR: Failed to write management scripts. & pause & exit /b 1 )

REM -- Dashboard script (depends on whether Python was found) --
if defined PYTHON_EXE (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "$d='%INSTALL_DIR%\shortcuts'; $py='!PYTHON_EXE!'; $dir='%INSTALL_DIR%\dashboard';" ^
        "[IO.File]::WriteAllText(\"$d\dashboard.bat\", \"@echo off`r`ncd /d `\"$dir`\"`r`necho Starting BlockDAG dashboard server...`r`necho Browser will open automatically.`r`necho Press Ctrl+C to stop.`r`necho.`r`n$py blockdag-dashboard-server.py`r`npause`r`n\", [Text.Encoding]::ASCII);"
) else (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "$d='%INSTALL_DIR%\shortcuts';" ^
        "[IO.File]::WriteAllText(\"$d\dashboard.bat\", \"@echo off`r`necho Dashboard requires Python.`r`necho Install from: https://www.python.org/downloads/`r`necho Then re-run this script.`r`npause`r`n\", [Text.Encoding]::ASCII);"
)

echo [Node] Management scripts written.

REM ============================================================================
REM 9. Desktop shortcut - single shortcut opening the shortcuts folder
REM ============================================================================
echo [Node] Creating desktop shortcut...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ws=New-Object -COM WScript.Shell;" ^
    "$lnk=$ws.CreateShortcut([IO.Path]::Combine([Environment]::GetFolderPath('Desktop'),'BlockDAG Node.lnk'));" ^
    "$lnk.TargetPath='%INSTALL_DIR%\shortcuts';" ^
    "$lnk.Description='BlockDAG Node Shortcuts';" ^
    "$lnk.Save()"
if not errorlevel 1 echo [Node] Desktop shortcut created: "BlockDAG Node"

REM ============================================================================
REM 10. Start the stack
REM ============================================================================
echo [Node] Starting stack...
cd /d "%INSTALL_DIR%"
docker compose up -d
if errorlevel 1 (
    echo.
    echo [Node] ERROR: Failed to start. Check that Docker Desktop is running.
    echo        For details run:  docker compose logs
    echo.
    pause & exit /b 1
)
echo [Node] Stack started successfully.

REM -- Firewall rule: dashboard (port 8088) accessible from LAN miners --
echo [Node] Adding Windows Firewall rule for dashboard port 8088...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Remove-NetFirewallRule -DisplayName 'BlockDAG Dashboard' -ErrorAction SilentlyContinue; New-NetFirewallRule -DisplayName 'BlockDAG Dashboard' -Direction Inbound -Protocol TCP -LocalPort 8088 -Profile Private,Domain -Action Allow | Out-Null" >nul 2>&1
echo [Node] Dashboard firewall rule set (LAN miners can now see their block counts).

REM -- Firewall rule: allow inbound stratum connections on the pool port --
echo [Node] Adding Windows Firewall rule for stratum port %POOL_PORT%...
netsh advfirewall firewall add rule ^
    name="BlockDAG Stratum Pool" ^
    dir=in ^
    action=allow ^
    protocol=TCP ^
    localport=%POOL_PORT% ^
    description="Allows inbound miner connections to the BlockDAG stratum pool" ^
    >nul 2>&1
if errorlevel 1 (
    echo [Node] NOTE: Could not add firewall rule ^(run as Administrator to do this automatically^).
    echo        To add it manually: allow inbound TCP on port %POOL_PORT% in Windows Defender Firewall.
) else (
    echo [Node] Firewall rule added for port %POOL_PORT%.
)

REM -- Auto-start dashboard if Python available --
if defined PYTHON_EXE (
    echo [Node] Starting dashboard server...
    start "BlockDAG Dashboard" /d "%INSTALL_DIR%\dashboard" !PYTHON_EXE! blockdag-dashboard-server.py
    echo [Node] Dashboard started - browser will open automatically.
)

REM ============================================================================
REM Done
REM ============================================================================
echo.
echo   =====================================================
echo     BlockDAG Node Installation Complete!
echo   =====================================================
echo.
echo   Desktop shortcut "BlockDAG Node" opens the shortcuts folder:
echo.
echo     start-node.bat  - starts the full stack
echo     stop-node.bat   - stops the stack
echo     node-status.bat - shows containers + pool logs
echo     node-logs.bat   - live log stream
echo     dashboard.bat   - starts the web dashboard
echo.
echo   Dashboard: http://localhost:8088
if not defined PYTHON_EXE (
    echo   ^(Python not found - install from python.org to enable the dashboard^)
)
echo.
echo   Stratum endpoint for miners:
echo     Host : !SERVER_IP!
echo     Port : %POOL_PORT%
if defined POOL_PASSWORD (echo     Pass : %POOL_PASSWORD%) else (echo     Pass : ^(none^))
echo.
echo   Install directory: %INSTALL_DIR%
echo   Config file:       %INSTALL_DIR%\.env
echo.
echo   To change settings: open the Dashboard and click Config,
echo   or edit %INSTALL_DIR%\.env and restart.
echo.
pause
