@echo off
REM CNN Fear & Greed standalone updater — runs at 09:35 TPE via Task Scheduler.
REM
REM Why 09:35: by 09:35 CNN has finalised its US trading-day historical
REM point (US close = ~04:00 TPE), so this run captures the most recent
REM overnight US session before daily_update runs in the afternoon.
REM
REM Schedule (Mon-Sat — Saturday catches the Fri-night US close):
REM   schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FearGreedUpdate" ^
REM     /TR "C:\Claude\Invest\feargreed_update.bat" ^
REM     /ST 09:35 /F
REM
REM Log: logs\feargreed_update.log (rotated by overwrite each run)

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo ============================== >> logs\feargreed_update.log
echo [%DATE% %TIME%] starting          >> logs\feargreed_update.log

"%PY%" feargreed_update.py >> logs\feargreed_update.log 2>&1
if errorlevel 1 goto :failed

echo [%DATE% %TIME%] done                >> logs\feargreed_update.log
exit /b 0

:failed
echo [%DATE% %TIME%] FAILED >> logs\feargreed_update.log
exit /b 1
