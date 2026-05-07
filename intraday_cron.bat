@echo off
REM 12:50 intraday snapshot + git push wrapper.
REM Scheduled by Task Scheduler:
REM   schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI ^
REM     /TN "Invest\IntradaySnapshot" ^
REM     /TR "C:\Claude\Invest\intraday_cron.bat" /ST 12:50 /F
REM
REM Log: logs\intraday_cron.log (rotated by overwrite each run)

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo ============================== >> logs\intraday_cron.log
echo [%DATE% %TIME%] starting          >> logs\intraday_cron.log

dist\intraday_snapshot.exe >> logs\intraday_cron.log 2>&1
if errorlevel 1 (
    echo [%DATE% %TIME%] snapshot FAILED ^(exit %ERRORLEVEL%^), skipping git >> logs\intraday_cron.log
    exit /b 1
)

git add frontend/public/data/scores_intraday.json frontend/public/data/operations_intraday.json >> logs\intraday_cron.log 2>&1

REM `git diff --cached --quiet` returns 1 when there ARE staged changes, 0 when clean.
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "Update intraday data" >> logs\intraday_cron.log 2>&1
    git push origin main >> logs\intraday_cron.log 2>&1
    if errorlevel 1 (
        echo [%DATE% %TIME%] git push FAILED >> logs\intraday_cron.log
        exit /b 2
    )
    echo [%DATE% %TIME%] pushed              >> logs\intraday_cron.log
) else (
    echo [%DATE% %TIME%] no changes to commit >> logs\intraday_cron.log
)

echo [%DATE% %TIME%] done                >> logs\intraday_cron.log
exit /b 0
