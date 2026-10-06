@echo off
REM Monthly database backup wrapper. Runs backup_monthly.py and appends a
REM timestamped start/result line to logs\backup_monthly.log.
REM
REM Invoked by Task Scheduler:
REM   Invest\BackupMonthly -> backup_monthly.bat   (1st of the month, 03:00)
REM
REM Reconstructed 2026-10-06 after the original (untracked) .bat was lost;
REM log-line format matches the historical logs\backup_monthly.log entries.
REM The exit code is passed on so Task Scheduler records a failed run as failed.

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set REPO=C:\Claude\Invest

cd /d %REPO%
if not exist logs mkdir logs

echo [%DATE% %TIME%] backup starting >> logs\backup_monthly.log
"%PY%" backup_monthly.py >> logs\backup_monthly.log 2>&1
if errorlevel 1 goto :failed

echo [%DATE% %TIME%] backup ok >> logs\backup_monthly.log
exit /b 0

:failed
echo [%DATE% %TIME%] backup FAILED >> logs\backup_monthly.log
exit /b 1
