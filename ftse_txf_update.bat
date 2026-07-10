@echo off
REM FTSE Taiwan (SGX) + TXF (TAIFEX) pre-open reference updater.
REM Runs Tue-Sat via Task Scheduler (covers Mon-Fri night sessions; Sat
REM catches Friday's; no Monday run - the weekend has no fresh session).
REM
REM Both legs are live quotes carrying the night settle before the 09:00 cash
REM open, so a single pass suffices (no polling):
REM   FTSE-TW via Capital overseas quote (SKOSQuoteLib "SGX,TWN0000") - replaced
REM     the cnyes FTSE-TW feed 2026-07 after cnyes stopped carrying the SGX
REM     overnight session (froze pre-open, revived only at the 08:45 day open).
REM   TXF night via cnyes.
REM
REM Two schedule points recommended: a pre-dawn run (~05:30) rebuilds the
REM estimate from the just-settled night session, and a day-open run (~08:47)
REM refreshes it after the SGX 08:45 open, before the 08:50 morning brief.
REM
REM Schedule (both point at this same .bat):
REM   schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FtseTxfUpdate" /TR "C:\Claude\Invest\ftse_txf_update.bat" ^
REM     /ST 08:47 /F
REM   schtasks /Create /SC WEEKLY /D TUE,WED,THU,FRI,SAT ^
REM     /TN "Invest\FtseTxfEarly" /TR "C:\Claude\Invest\ftse_txf_update.bat" ^
REM     /ST 05:30 /F
REM
REM Log: logs\ftse_txf_update.log

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo ============================== >> logs\ftse_txf_update.log
echo [%DATE% %TIME%] starting >> logs\ftse_txf_update.log

"%PY%" ftse_txf_update.py >> logs\ftse_txf_update.log 2>&1
if errorlevel 1 goto :failed

echo [%DATE% %TIME%] done >> logs\ftse_txf_update.log
exit /b 0

:failed
echo [%DATE% %TIME%] FAILED >> logs\ftse_txf_update.log
exit /b 1
