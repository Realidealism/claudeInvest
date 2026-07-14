@echo off
REM Post-close P&L summary for the micro-Taiex paper engines -> Telegram.
REM Runs after each session's force-close, so every round-trip is already
REM flushed to the TradeLog CSVs.
REM
REM   daily_report.bat day     after the 13:45 day close   (scheduled 13:50)
REM   daily_report.bat night   after the 05:00 night close (scheduled 05:10)
REM
REM Weekends are skipped by the script itself, not here.

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set MTX=C:\Claude\Invest\microtaiex-daytrade
set LOG=%MTX%\reports\daily_report.log

cd /d %MTX%
if not exist reports mkdir reports

echo [%DATE% %TIME%] daily_report %1 >> "%LOG%"
"%PY%" -X utf8 daily_report.py %1 >> "%LOG%" 2>&1
if errorlevel 1 goto :failed
exit /b 0

:failed
echo [%DATE% %TIME%] FAILED >> "%LOG%"
REM cron_alert is a module of the PARENT repo and takes an event key, not free
REM text, so it must run as -m from the repo root.
cd /d C:\Claude\Invest
"%PY%" -m telegram_bot.cron_alert microtaiex_report_failed %1 >> "%LOG%" 2>&1
exit /b 1
