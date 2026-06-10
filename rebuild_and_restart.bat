@echo off
REM Complete rebuild + restart of intraday_snapshot daemon.
REM
REM Stops the running daemon, rebuilds 3 exes with the system Python's
REM PyInstaller, then relaunches the daemon in a new console window.
REM
REM Does NOT touch:
REM   - NSSM service "InvestTelegramBot" (telegram bot, unrelated)
REM   - vite dev server (node.exe)
REM   - intraday_publish.bat / feargreed_update.bat (Task Scheduler)
REM
REM Usage:
REM   rebuild_and_restart.bat               rebuild + restart
REM   rebuild_and_restart.bat --no-restart  rebuild only (daemon stays down)
REM   rebuild_and_restart.bat --no-build    restart only (skip PyInstaller)

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set REPO=C:\Claude\Invest

cd /d %REPO%
if not exist logs mkdir logs

echo ============================== >> logs\rebuild_and_restart.log
echo [%DATE% %TIME%] starting %*       >> logs\rebuild_and_restart.log

REM ===== 1. Stop intraday_snapshot daemon =====
echo [1/4] stopping intraday_snapshot daemon...
echo [%DATE% %TIME%] stopping daemon    >> logs\rebuild_and_restart.log
taskkill /IM intraday_snapshot.exe /F >> logs\rebuild_and_restart.log 2>&1

REM Give Windows a moment to release the .exe file lock.
timeout /t 2 /nobreak > nul

REM Verify nothing left holding the .exe.
tasklist /FI "IMAGENAME eq intraday_snapshot.exe" 2>nul | find /I "intraday_snapshot.exe" > nul
if not errorlevel 1 goto :still_running

REM ===== 2. Rebuild exes (unless --no-build) =====
if "%1"=="--no-build" goto :skip_build
if "%2"=="--no-build" goto :skip_build

echo [2/4] rebuilding daily_update.exe...
echo [%DATE% %TIME%] build daily_update >> logs\rebuild_and_restart.log
"%PY%" -m PyInstaller --noconfirm daily_update.spec >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :build_failed

echo [3/4] rebuilding intraday_snapshot.exe...
echo [%DATE% %TIME%] build intraday_snapshot >> logs\rebuild_and_restart.log
"%PY%" -m PyInstaller --noconfirm intraday_snapshot.spec >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :build_failed

echo [3.5/4] rebuilding feargreed_update.exe...
echo [%DATE% %TIME%] build feargreed_update >> logs\rebuild_and_restart.log
"%PY%" -m PyInstaller --noconfirm feargreed_update.spec >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :build_failed

goto :after_build

:skip_build
echo [2-3/4] skipping rebuild (--no-build).
echo [%DATE% %TIME%] skip build         >> logs\rebuild_and_restart.log

:after_build

REM ===== 3. Restart daemon (unless --no-restart) =====
if "%1"=="--no-restart" goto :no_restart
if "%2"=="--no-restart" goto :no_restart

echo [4/4] starting intraday_snapshot.exe in new window...
echo [%DATE% %TIME%] start daemon       >> logs\rebuild_and_restart.log
start "intraday_snapshot" /D %REPO% %REPO%\dist\intraday_snapshot.exe

echo [%DATE% %TIME%] done                >> logs\rebuild_and_restart.log
echo.
echo Done. daemon launched in new console window.
exit /b 0

:no_restart
echo [4/4] skipping daemon restart (--no-restart).
echo [%DATE% %TIME%] done (no restart)  >> logs\rebuild_and_restart.log
echo.
echo Rebuild done. daemon NOT restarted.
echo Start it manually with:
echo   start "intraday_snapshot" /D %REPO% %REPO%\dist\intraday_snapshot.exe
exit /b 0

:still_running
echo [ERROR] intraday_snapshot.exe still running after taskkill.
echo [%DATE% %TIME%] FAILED - still running >> logs\rebuild_and_restart.log
exit /b 1

:build_failed
echo [ERROR] PyInstaller build failed; see logs\rebuild_and_restart.log
echo [%DATE% %TIME%] FAILED - build      >> logs\rebuild_and_restart.log
exit /b 2
