@echo off
REM Rebuild + restart the intraday sweeper (InvestIntradaySweeper).
REM
REM Why this exists separately: rebuild_and_restart.bat covers only
REM daily_update.exe and intraday_snapshot.exe, and says so in its header.
REM Nothing built dist\intraday_sweep_update.exe, so the service ran code
REM from 2026-06-15 while five weeks of committed intraday/ changes sat
REM undeployed. Run this after touching intraday\ or intraday_sweep_update.py.
REM
REM The service must be stopped first: NSSM restarts it within seconds of a
REM plain taskkill, and a running exe cannot be overwritten by PyInstaller.
REM
REM Run as Administrator (nssm stop/start needs it).
REM
REM Usage:
REM   rebuild_sweeper.bat               rebuild + restart
REM   rebuild_sweeper.bat --no-restart  rebuild only (sweeper stays down)

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set REPO=C:\Claude\Invest
set NSSM=C:\Claude\Invest\tools\nssm.exe

cd /d %REPO%
if not exist logs mkdir logs

echo ============================== >> logs\rebuild_sweeper.log
echo [%DATE% %TIME%] starting %*    >> logs\rebuild_sweeper.log

REM ===== 0. Require Administrator =====
REM Without elevation nssm stop/start fails with "Can't open service!" and the
REM script would sail on to build against a locked exe.
net session >nul 2>&1
if errorlevel 1 goto :not_admin

REM ===== 1. Stop the service =====
echo [1/3] stopping InvestIntradaySweeper...
echo [%DATE% %TIME%] nssm stop InvestIntradaySweeper >> logs\rebuild_sweeper.log
"%NSSM%" stop InvestIntradaySweeper >> logs\rebuild_sweeper.log 2>&1

taskkill /IM intraday_sweep_update.exe /F >> logs\rebuild_sweeper.log 2>&1
timeout /t 2 /nobreak > nul

tasklist /FI "IMAGENAME eq intraday_sweep_update.exe" 2>nul | find /I "intraday_sweep_update.exe" > nul
if not errorlevel 1 goto :still_running

REM ===== 2. Rebuild =====
echo [2/3] rebuilding intraday_sweep_update.exe...
echo [%DATE% %TIME%] build intraday_sweep_update >> logs\rebuild_sweeper.log
"%PY%" -m PyInstaller --noconfirm --clean intraday_sweep_update.spec >> logs\rebuild_sweeper.log 2>&1
if errorlevel 1 goto :build_failed

REM ===== 3. Restart =====
if "%1"=="--no-restart" goto :no_restart

echo [3/3] starting InvestIntradaySweeper...
echo [%DATE% %TIME%] nssm start InvestIntradaySweeper >> logs\rebuild_sweeper.log
"%NSSM%" start InvestIntradaySweeper >> logs\rebuild_sweeper.log 2>&1

REM nssm start returns non-zero while the service is still START_PENDING, which
REM is not a failure -- check the actual state instead of the exit code.
timeout /t 4 /nobreak > nul
sc query InvestIntradaySweeper | find "RUNNING" > nul && goto :start_ok
sc query InvestIntradaySweeper | find "START_PENDING" > nul && goto :start_ok
goto :start_failed

:start_ok
echo [%DATE% %TIME%] done            >> logs\rebuild_sweeper.log
echo.
echo Done. InvestIntradaySweeper rebuilt and restarted.
exit /b 0

:no_restart
echo [3/3] skipping restart (--no-restart).
echo [%DATE% %TIME%] done (no restart) >> logs\rebuild_sweeper.log
echo.
echo Rebuild done. Sweeper NOT restarted. Start it with:
echo   "%NSSM%" start InvestIntradaySweeper
exit /b 0

:not_admin
echo [ERROR] Not elevated. nssm stop/start needs Administrator.
echo [%DATE% %TIME%] FAILED - not admin >> logs\rebuild_sweeper.log
exit /b 4

:still_running
echo [ERROR] intraday_sweep_update.exe still running after stop + taskkill.
echo [%DATE% %TIME%] FAILED - still running >> logs\rebuild_sweeper.log
exit /b 1

:build_failed
echo [ERROR] PyInstaller build failed; see logs\rebuild_sweeper.log
echo [%DATE% %TIME%] FAILED - build     >> logs\rebuild_sweeper.log
"%NSSM%" start InvestIntradaySweeper >> logs\rebuild_sweeper.log 2>&1
exit /b 2

:start_failed
echo [ERROR] nssm start InvestIntradaySweeper failed; see logs.
echo [%DATE% %TIME%] FAILED - start     >> logs\rebuild_sweeper.log
exit /b 3
