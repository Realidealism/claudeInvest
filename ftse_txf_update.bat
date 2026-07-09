@echo off
REM FTSE Taiwan (SGX) and TXF (TAIFEX) pre-open reference updater.
REM Runs at 08:47 TPE Tue-Sat via Task Scheduler.
REM
REM Why 08:47: the cnyes 富台 (SGX FTSE Taiwan) continuous quote stopped
REM carrying the overnight (T+1) session ~2026-06-30 — it freezes at the prior
REM day-session close and only revives when the SGX day session opens 08:45.
REM Firing at 08:47 (just after the open) and polling until the timestamped
REM quote lands (see ftse_txf_update.py's retry loop) captures a LIVE pre-open
REM quote for both legs the moment it appears; TXF likewise shows today's
REM day-session open rather than the 04:59 night settle, which is the better
REM predictor of the 09:00 open anyway. The row is ready before the Telegram
REM 早安管家 brief (08:50), which then just reads the freshly-updated
REM tw.ftse_taiwan row instead of fetching again.
REM Tue-Sat covers Mon-Fri sessions (Sat catches Friday's); no Monday run is
REM needed (weekend has no fresh session).
REM
REM Schedule:
REM   schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FtseTxfUpdate" ^
REM     /TR "C:\Claude\Invest\ftse_txf_update.bat" ^
REM     /ST 08:47 /F
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
