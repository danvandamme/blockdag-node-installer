@echo off
cd /d "%~dp0"
echo.
set /p MSG=  Commit message:
if "%MSG%"=="" set "MSG=Update"
echo.
git add .
git commit -m "%MSG%"
git push
echo.
pause
