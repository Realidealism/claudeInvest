@echo off
REM FTSE Taiwan (SGX) and TXF (TAIFEX) night-session reference updater.
REM Runs at 07:55 TPE Tue-Sat via Task Scheduler.
REM
REM Why Tue-Sat 07:55: a TW trading day's night session (15:00-05:00 TPE)
REM ends ~05:00 the next morning, so an early-morning run captures the
REM completed overnight action before the 09:00 cash open. 07:55 (not 08:00)
REM so it lands 5 min before the Telegram 早安管家 brief, which then just
REM reads the freshly-updated tw.ftse_taiwan row instead of fetching again.
REM Tue-Sat covers Mon-Fri night sessions (Sat catches the Friday-night
REM close); no Monday run is needed (weekend has no fresh session).
REM
REM Schedule:
REM   schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FtseTxfUpdate" ^
REM     /TR "C:\Claude\Invest\ftse_txf_update.bat" ^
REM     /ST 07:55 /F
REM
REM Log: logs\ftse_txf_update.log

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo ============================== >> logs\ftse_txf_update.log
echo [%DATE% %TIME%] starting          >> logs\ftse_txf_update.log

"%PY%" ftse_txf_update.py >> logs\ftse_txf_update.log 2>&1
if errorlevel 1 goto :failed

echo [%DATE% %TIME%] done                >> logs\ftse_txf_update.log
exit /b 0

:failed
echo [%DATE% %TIME%] FAILED >> logs\ftse_txf_update.log
exit /b 1
