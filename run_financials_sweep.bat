@echo off
REM Daily top-up of quarterly statements (tw.income_statements, balance_sheets,
REM cash_flows) from FinMind. Fetches only stocks still missing the latest
REM quarter whose filing deadline has passed, so on most days it ends in
REM minutes; around a deadline it catches late filers on its own.
REM Runs on system Python rather than inside daily_update.exe: a fresh quarter
REM is ~1,900 stocks at FinMind's hourly quota, far too slow for the nightly run.

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set LOG=C:\Claude\Invest\logs\financials_sweep.log

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo [%DATE% %TIME%] financials --quarterly >> "%LOG%"
"%PY%" -u -m scrapers.financials --quarterly >> "%LOG%" 2>&1
if errorlevel 1 goto :failed
echo [%DATE% %TIME%] done >> "%LOG%"
exit /b 0

:failed
set RC=%ERRORLEVEL%
echo [%DATE% %TIME%] FAILED exit %RC% >> "%LOG%"
"%PY%" -m telegram_bot.cron_alert financials_sweep_failed %RC% >> "%LOG%" 2>&1
exit /b 1
