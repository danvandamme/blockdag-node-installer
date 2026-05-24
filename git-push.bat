@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

REM -- Read current version from installer --
set "_V="
for /f "tokens=2 delims==" %%V in ('findstr /r "^set .VERSION=" install-node-windows.bat') do if not defined _V set "_V=%%V"
set "VERSION=%_V:"=%"
if "%VERSION%"=="" (
    echo [Backup] WARNING: Could not read VERSION from install-node-windows.bat
) else (
    echo   Version: %VERSION%
)

echo.
set /p MSG=  Commit message:
if "%MSG%"=="" set "MSG=Update"
echo.
git add .
git commit -m "%MSG%"
git push
if errorlevel 1 (
    echo.
    echo [Backup] Git push failed - skipping version backup.
    echo.
    pause
    exit /b 1
)

REM -- Version backup --
if "%VERSION%"=="" goto :skip_backup
set "BACKUP_DIR=C:\DATA\BlockDAG Installers\Versions\Node Installer\Node Installer - %VERSION%"
if exist "%BACKUP_DIR%" (
    echo [Backup] Version %VERSION% already backed up - skipping.
) else (
    echo [Backup] Saving snapshot of version %VERSION%...
    robocopy "%~dp0" "%BACKUP_DIR%" /E /XD .git /NFL /NDL /NJH /NJS >nul
    if errorlevel 8 (
        echo [Backup] WARNING: Some files could not be copied to Versions folder.
    ) else (
        echo [Backup] Saved: %BACKUP_DIR%
    )
)
:skip_backup

echo.
pause
