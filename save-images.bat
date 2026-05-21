@echo off
REM ============================================================================
REM BlockDAG Node - Docker Image Exporter
REM
REM Run this script on a machine that already has the BlockDAG Docker images
REM loaded (i.e. where the node is already running).
REM
REM It saves the two required images as .tar files in this folder so they can
REM be copied to another machine and loaded before running install-node.bat.
REM
REM Output files:
REM   bdag-release-asic-pool.tar   (~54 MB compressed)
REM   bdag-release-node.tar        (~86 MB compressed)
REM
REM To load on the target machine (before running install-node.bat):
REM   docker load -i bdag-release-asic-pool.tar
REM   docker load -i bdag-release-node.tar
REM ============================================================================
setlocal
cd /d "%~dp0"

set "POOL_IMAGE=bdag-release/asic-pool:local"
set "NODE_IMAGE=bdag-release/node:local"
set "POOL_TAR=%~dp0bdag-release-asic-pool.tar"
set "NODE_TAR=%~dp0bdag-release-node.tar"

echo.
echo   =====================================================
echo     BlockDAG Docker Image Exporter
echo   =====================================================
echo.

REM -- Check Docker --
docker info >nul 2>&1
if errorlevel 1 (
    echo [Export] ERROR: Docker is not running.
    pause & exit /b 1
)

REM -- Check images exist --
echo [Export] Checking images...
docker image inspect %POOL_IMAGE% >nul 2>&1
if errorlevel 1 (
    echo [Export] ERROR: Image not found: %POOL_IMAGE%
    echo          Load or pull it first, then re-run this script.
    pause & exit /b 1
)
docker image inspect %NODE_IMAGE% >nul 2>&1
if errorlevel 1 (
    echo [Export] ERROR: Image not found: %NODE_IMAGE%
    echo          Load or pull it first, then re-run this script.
    pause & exit /b 1
)
echo [Export] Both images found.
echo.

REM -- Save pool image --
echo [Export] Saving %POOL_IMAGE%...
echo          This may take a minute...
docker save -o "%POOL_TAR%" %POOL_IMAGE%
if errorlevel 1 (
    echo [Export] ERROR: Failed to save pool image.
    pause & exit /b 1
)
for %%f in ("%POOL_TAR%") do echo [Export] Saved: %POOL_TAR% (%%~zf bytes)

echo.

REM -- Save node image --
echo [Export] Saving %NODE_IMAGE%...
echo          This may take a minute...
docker save -o "%NODE_TAR%" %NODE_IMAGE%
if errorlevel 1 (
    echo [Export] ERROR: Failed to save node image.
    pause & exit /b 1
)
for %%f in ("%NODE_TAR%") do echo [Export] Saved: %NODE_TAR% (%%~zf bytes)

echo.
echo   =====================================================
echo     Export Complete!
echo   =====================================================
echo.
echo   Files saved in this folder:
echo     bdag-release-asic-pool.tar
echo     bdag-release-node.tar
echo.
echo   Copy these files (along with install-node.bat and schema.sql)
echo   to the target machine, then on that machine run:
echo.
echo     docker load -i bdag-release-asic-pool.tar
echo     docker load -i bdag-release-node.tar
echo     install-node.bat
echo.
pause
