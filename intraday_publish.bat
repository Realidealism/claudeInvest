@echo off
REM Intraday publish step: push the 6 intraday JSONs (incl. vix.json,
REM yield_curve.json, fear_greed.json) to Vercel and notify Telegram of
REM new watchlist signal hits.
REM
REM Decoupled from intraday_snapshot.exe (which now runs as a back-to-back
REM daemon during the trading session) so the snapshot loop's DB updates
REM don't bloat git history with 30+ commits per day. This .bat is what
REM bridges DB state to Vercel + Telegram on its own cadence.
REM
REM Scheduled by Task Scheduler (Mon-Fri, 09:00-15:00, every 15 min):
REM   schtasks /Create /SC WEEKLY /D MON,TUE,WED,THU,FRI ^
REM     /TN "Invest\IntradayPublish" ^
REM     /TR "C:\Claude\Invest\intraday_publish.bat" ^
REM     /ST 09:00 /DU 06:00 /RI 15 /F
REM
REM Window extended from 05:00 -> 06:00 (last fire at ~15:00) so the
REM cycles past 14:30 can pull the TAIFEX official daily VIX file --
REM TAIFEX only generates Dailydownload/vix/log2data/YYYYMMnew.txt
REM ~14:30 TPE, after the session close. Cycles before then run the
REM daily scrape too but the upstream file is still 404 and the scrape
REM is a non-fatal no-op.
REM
REM Log: logs\intraday_publish.log (rotated by overwrite each run)
REM Telegram pushes: git failures, new watchlist signal hits (deduped via
REM tw.intraday_push_state).

set PY=C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe

cd /d C:\Claude\Invest
if not exist logs mkdir logs

echo ============================== >> logs\intraday_publish.log
echo [%DATE% %TIME%] starting          >> logs\intraday_publish.log

REM Push pending watchlist hits first; failure here is non-fatal so a
REM Telegram outage never blocks the JSON deploy to Vercel.
"%PY%" -m telegram_bot.push_intraday_signals >> logs\intraday_publish.log 2>&1

REM Attempt the daily TWVIX file (TAIFEX publishes ~14:30). Non-fatal:
REM before 14:30 the upstream file 404s and the scrape is a no-op,
REM leaving the intraday snapshot the daemon already wrote in place.
REM Successful runs overwrite intraday_time back to NULL so the
REM frontend "intraday HH:MM" tag flips to the normal date label.
"%PY%" vix_update.py --no-push >> logs\intraday_publish.log 2>&1

REM Pull the US Treasury daily yield curve. Treasury updates ~16:00 ET
REM (~04:00 TPE next day) so the first IntradayPublish cycle of the
REM session picks up yesterday's settled value; later cycles are
REM idempotent no-ops (same data, COALESCE upsert keeps rows intact).
REM Non-fatal if Treasury 504s -- yesterday's row stays.
"%PY%" yield_update.py --no-push >> logs\intraday_publish.log 2>&1

REM Pull CNN Fear & Greed. CNN's graphdata endpoint refreshes during
REM US trading hours (TPE 21:30-04:00) and freezes overnight; the
REM session-time cycles here pick up the previous US trading day's
REM value once it lands. Non-fatal: a CNN outage just leaves the row
REM untouched and the next cycle retries.
"%PY%" feargreed_update.py --no-push >> logs\intraday_publish.log 2>&1

git add frontend/public/data/scores_intraday.json frontend/public/data/operations_intraday.json frontend/public/data/positions_intraday.json frontend/public/data/breadth.json frontend/public/data/vix.json frontend/public/data/yield_curve.json frontend/public/data/fear_greed.json >> logs\intraday_publish.log 2>&1
if errorlevel 1 goto :git_failed

REM `git diff --cached --quiet` returns 1 when there ARE staged changes, 0 when clean.
git diff --cached --quiet
if not errorlevel 1 goto :nothing_to_commit

git commit -m "Update intraday data" >> logs\intraday_publish.log 2>&1
if errorlevel 1 goto :git_failed
git push origin main >> logs\intraday_publish.log 2>&1
if errorlevel 1 goto :git_failed

echo [%DATE% %TIME%] pushed              >> logs\intraday_publish.log
goto :done

:nothing_to_commit
echo [%DATE% %TIME%] no changes to commit >> logs\intraday_publish.log
goto :done

:git_failed
echo [%DATE% %TIME%] git step FAILED >> logs\intraday_publish.log
"%PY%" -m telegram_bot.cron_alert git_push_failed >> logs\intraday_publish.log 2>&1
exit /b 2

:done
echo [%DATE% %TIME%] done                >> logs\intraday_publish.log
exit /b 0
