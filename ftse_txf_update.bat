@echo off
REM FTSE Taiwan (SGX) and TXF (TAIFEX) night-session reference updater.
REM Runs at 08:00 TPE Tue-Sat via Task Scheduler.
REM
REM Why Tue-Sat 08:00: a TW trading day's night session (15:00-05:00 TPE)
REM ends ~05:00 the next morning, so an 08:00 run captures the completed
REM overnight action before the 09:00 cash open. Tue-Sat covers Mon-Fri
REM night sessions (Sat catches the Friday-night close); no Monday run is
REM needed (weekend has no fresh session).
REM
REM Schedule:
REM   schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FtseTxfUpdate" ^
REM     /TR "C:\Claude\Invest\ftse_txf_update.bat" ^
REM     /ST 08:00 /F
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
