@echo off
REM Health-check wrapper. Runs telegram_bot.health_check for the given mode
REM (daily|intraday) and appends a timestamped start/result line to the log.
REM
REM Invoked by Task Scheduler:
REM   HealthCheckDaily     -> health_check.bat daily
REM   HealthCheckIntraday  -> health_check.bat intraday
REM
REM Reconstructed 2026-07-02 after the original (untracked) .bat was lost;
REM log-line format matches the historical logs\health_check.log entries.

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set REPO=C:\Claude\Invest

cd /d %REPO%
if not exist logs mkdir logs

echo [%DATE% %TIME%] health_check %1 starting >> logs\health_check.log
"%PY%" -m telegram_bot.health_check %1 >> logs\health_check.log 2>&1
if errorlevel 1 goto :failed

echo [%DATE% %TIME%] health_check %1 ok >> logs\health_check.log
exit /b 0

:failed
echo [%DATE% %TIME%] health_check %1 FAILED >> logs\health_check.log
exit /b 1
