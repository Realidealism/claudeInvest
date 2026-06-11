@echo off
REM RETIRED -- this wrapper used to fire at 12:50 to run a one-shot
REM intraday snapshot + Telegram push + git push. The architecture has
REM moved to:
REM
REM   intraday_snapshot.exe   -- long-running daemon, back-to-back passes
REM                             during the trading session (09:00-13:30)
REM                             plus one final pass after close. Started
REM                             once at boot via Task Scheduler with
REM                             trigger "At system startup".
REM
REM   intraday_publish.bat    -- every 15 min during 09:00-14:00, git pushes
REM                             the 3 intraday JSONs and runs
REM                             telegram_bot.push_intraday_signals (deduped
REM                             via tw.intraday_push_state so each
REM                             (date, stock, signal) is messaged at most
REM                             once per day).
REM
REM This script is left as a safe no-op so any Task Scheduler entry that
REM still calls it doesn't fail loudly. To fully retire it, delete the
REM "Invest\IntradaySnapshot" entry in Task Scheduler.

cd /d C:\Claude\Invest
if not exist logs mkdir logs
echo [%DATE% %TIME%] intraday_cron.bat invoked but retired - see intraday_publish.bat >> logs\intraday_cron.log
exit /b 0
