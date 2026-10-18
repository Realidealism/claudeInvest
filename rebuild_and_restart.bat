@echo off
REM Complete rebuild + restart of the intraday_snapshot daemon.
REM
REM The daemon now runs as the NSSM service "InvestIntradaySnapshot" (auto
REM restarts on crash), so it MUST be stopped via `nssm stop` -- a plain
REM taskkill is futile because NSSM relaunches it within seconds, which also
REM keeps dist\intraday_snapshot.exe locked so PyInstaller can't overwrite it.
REM After rebuilding it is restarted with `nssm start` (NOT a console window,
REM which would double-launch alongside the service).
REM
REM Run as Administrator (nssm stop/start needs it).
REM
REM Does NOT touch:
REM   - InvestTelegramBot / InvestIntradaySweeper / InvestMicroPaper (services)
REM   - vite dev server (node.exe)
REM   - intraday_publish.bat (Task Scheduler; it runs feargreed_update.py,
REM     vix_update.py and yield_update.py directly with system Python)
REM
REM Prerequisite: no daily_update.exe must be running, or PyInstaller can't
REM overwrite dist\daily_update.exe. Kill stragglers first if needed:
REM   taskkill /IM daily_update.exe /F
REM
REM Usage:
REM   rebuild_and_restart.bat               rebuild + restart
REM   rebuild_and_restart.bat --no-restart  rebuild only (daemon stays down)
REM   rebuild_and_restart.bat --no-build    restart only (skip PyInstaller)
REM   rebuild_and_restart.bat --allow-older build even though the deployed
REM                                         build is not in HEAD's history
REM                                         (or dist\ has no BUILD_INFO.json)
REM   rebuild_and_restart.bat --allow-migrations  build with an uncommitted
REM                                         migration (exes apply it on start)

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set REPO=C:\Claude\Invest
set NSSM=C:\Claude\Invest\tools\nssm.exe

cd /d %REPO%
if not exist logs mkdir logs

echo ============================== >> logs\rebuild_and_restart.log
echo [%DATE% %TIME%] starting %*       >> logs\rebuild_and_restart.log

REM ===== 0. Require Administrator =====
REM Without elevation every nssm stop/start fails with "Can't open service!
REM OpenService(): Access denied" and the script sails on regardless, leaving
REM the service unrestarted (or, on a trading day, the .exe locked).
net session >nul 2>&1
if errorlevel 1 goto :not_admin

REM ===== 0b. Refuse to build from a tree that lacks the deployed build =====
REM tools\build_info.py check compares dist\BUILD_INFO.json (written by the
REM last build) with HEAD, and lists uncommitted sources as a warning. Run
REM before the service is stopped, so a refusal leaves everything running.
if "%1"=="--no-build" goto :checked
if "%2"=="--no-build" goto :checked
"%PY%" tools\build_info.py check %* > logs\build_info_last.txt 2>&1
set BI_RC=%errorlevel%
type logs\build_info_last.txt
type logs\build_info_last.txt >> logs\rebuild_and_restart.log
if not "%BI_RC%"=="0" goto :check_refused
:checked

REM ===== 1. Stop the InvestIntradaySnapshot service =====
echo [1/4] stopping InvestIntradaySnapshot service...
echo [%DATE% %TIME%] nssm stop InvestIntradaySnapshot >> logs\rebuild_and_restart.log
"%NSSM%" stop InvestIntradaySnapshot >> logs\rebuild_and_restart.log 2>&1

REM Clear any lingering multiprocessing Pool workers NSSM didn't reap, then
REM give Windows a moment to release the .exe file lock.
taskkill /IM intraday_snapshot.exe /F >> logs\rebuild_and_restart.log 2>&1
timeout /t 2 /nobreak > nul

REM Verify nothing left holding the .exe (NSSM did not relaunch it).
tasklist /FI "IMAGENAME eq intraday_snapshot.exe" 2>nul | find /I "intraday_snapshot.exe" > nul
if not errorlevel 1 goto :still_running

REM ===== 2. Rebuild exes (unless --no-build) =====
if "%1"=="--no-build" goto :skip_build
if "%2"=="--no-build" goto :skip_build

REM _build_stamp.py carries the commit into both exes (tw.snapshot_runs);
REM unstamp removes it after the builds, or on failure. Stamped before the
REM record is dropped: a stamp that fails leaves dist\ and its record as they were.
"%PY%" tools\build_info.py stamp >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :stamp_failed

REM The record describes the exes in dist\; drop it before replacing them, so
REM a build that fails half way leaves none (the next check then refuses).
if exist dist\BUILD_INFO.json del dist\BUILD_INFO.json

echo [2/4] rebuilding daily_update.exe...
echo [%DATE% %TIME%] build daily_update >> logs\rebuild_and_restart.log
"%PY%" -m PyInstaller --noconfirm --clean daily_update.spec >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :build_failed

echo [3/4] rebuilding intraday_snapshot.exe...
echo [%DATE% %TIME%] build intraday_snapshot >> logs\rebuild_and_restart.log
"%PY%" -m PyInstaller --noconfirm --clean intraday_snapshot.spec >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 goto :build_failed

"%PY%" tools\build_info.py record >> logs\rebuild_and_restart.log 2>&1
if errorlevel 1 echo [WARN] could not write dist\BUILD_INFO.json; see logs\rebuild_and_restart.log
"%PY%" tools\build_info.py unstamp >> logs\rebuild_and_restart.log 2>&1

goto :after_build

:skip_build
echo [2-3/4] skipping rebuild (--no-build).
echo [%DATE% %TIME%] skip build         >> logs\rebuild_and_restart.log

:after_build

REM ===== 3. Restart the service (unless --no-restart) =====
if "%1"=="--no-restart" goto :no_restart
if "%2"=="--no-restart" goto :no_restart

echo [4/4] starting InvestIntradaySnapshot service...
echo [%DATE% %TIME%] nssm start InvestIntradaySnapshot >> logs\rebuild_and_restart.log
"%NSSM%" start InvestIntradaySnapshot >> logs\rebuild_and_restart.log 2>&1

REM nssm start returns non-zero when the service is slow to reach RUNNING
REM (the daemon's first pass takes minutes, so SCM still sees START_PENDING
REM when the start control returns). That is NOT a failure -- verify the
REM actual service state instead of trusting the exit code.
timeout /t 4 /nobreak > nul
sc query InvestIntradaySnapshot | find "RUNNING" > nul && goto :start_ok
sc query InvestIntradaySnapshot | find "START_PENDING" > nul && goto :start_ok
goto :start_failed

:start_ok
echo [%DATE% %TIME%] done                >> logs\rebuild_and_restart.log
echo.
echo Done. daemon restarted via NSSM (InvestIntradaySnapshot).
exit /b 0

:no_restart
echo [4/4] skipping daemon restart (--no-restart).
echo [%DATE% %TIME%] done (no restart)  >> logs\rebuild_and_restart.log
echo.
echo Rebuild done. daemon NOT restarted.
echo Start it with:
echo   "%NSSM%" start InvestIntradaySnapshot
exit /b 0

:not_admin
echo [ERROR] Not elevated. nssm stop/start needs Administrator, and without it
echo [ERROR] the service is never restarted. Reopen the shell as Administrator.
echo [%DATE% %TIME%] FAILED - not admin  >> logs\rebuild_and_restart.log
exit /b 4

:check_refused
echo [ERROR] Build refused by tools\build_info.py check (reason above).
echo [ERROR] Nothing was stopped or rebuilt.
echo [%DATE% %TIME%] FAILED - build source check >> logs\rebuild_and_restart.log
exit /b 5

:still_running
echo [ERROR] intraday_snapshot.exe still running after nssm stop + taskkill.
echo [ERROR] Is the InvestIntradaySnapshot service auto-restarting? Try:
echo   "%NSSM%" stop InvestIntradaySnapshot
echo [%DATE% %TIME%] FAILED - still running >> logs\rebuild_and_restart.log
exit /b 1

:stamp_failed
echo [ERROR] tools\build_info.py stamp failed; see logs\rebuild_and_restart.log
echo [ERROR] Nothing was rebuilt; restarting the service on the old exe.
echo [%DATE% %TIME%] FAILED - stamp      >> logs\rebuild_and_restart.log
"%PY%" tools\build_info.py unstamp >> logs\rebuild_and_restart.log 2>&1
"%NSSM%" start InvestIntradaySnapshot >> logs\rebuild_and_restart.log 2>&1
exit /b 6

:build_failed
echo [ERROR] PyInstaller build failed; see logs\rebuild_and_restart.log
echo [ERROR] If it is a PermissionError on dist\daily_update.exe, a hung
echo [ERROR] daily_update.exe is locking it -- kill it then rerun:
echo   taskkill /IM daily_update.exe /F
echo [%DATE% %TIME%] FAILED - build      >> logs\rebuild_and_restart.log
"%PY%" tools\build_info.py unstamp >> logs\rebuild_and_restart.log 2>&1
"%NSSM%" start InvestIntradaySnapshot >> logs\rebuild_and_restart.log 2>&1
exit /b 2

:start_failed
echo [ERROR] nssm start InvestIntradaySnapshot failed; see logs.
echo [%DATE% %TIME%] FAILED - start      >> logs\rebuild_and_restart.log
exit /b 3
