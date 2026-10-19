@echo off
REM Monthly reconciliation of the nightly snapshot against the backtest of the
REM deployed build, plus the out-of-sample (holdout) figures. Reads the
REM database only; writes logs\reconcile\YYYY-MM.md and sends a summary.
REM Runs on system Python from source, not inside an exe: it checks out the
REM deployed commit in tmp\reconcile_wt and runs the 8-signal backtest there.
REM
REM Scheduled as \Invest\LiveReconcileMonthly (day 1, 15:00, run when missed;
REM after the intraday snapshot, whose 8 workers it would compete with):
REM   schtasks /Create /TN "Invest\LiveReconcileMonthly" /TR "C:\Claude\Invest\live_reconcile.bat" /SC MONTHLY /D 1 /ST 15:00 /F
REM   $t = Get-ScheduledTask -TaskPath "\Invest\" -TaskName "LiveReconcileMonthly"; $t.Settings.StartWhenAvailable = $true; Set-ScheduledTask -InputObject $t

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe
set LOG=C:\Claude\Invest\logs\live_reconcile.log

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo [%DATE% %TIME%] live_reconcile >> "%LOG%"
"%PY%" -u -m analysis.live_reconcile >> "%LOG%" 2>&1
if errorlevel 1 goto :failed
echo [%DATE% %TIME%] done >> "%LOG%"
exit /b 0

:failed
set RC=%ERRORLEVEL%
echo [%DATE% %TIME%] FAILED exit %RC% >> "%LOG%"
"%PY%" -m telegram_bot.cron_alert live_reconcile_failed %RC% >> "%LOG%" 2>&1
exit /b 1
