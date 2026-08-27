@echo off
REM Daily sweep for annual-report product revenue mix.
REM Runs on system Python on purpose: pdfplumber and its dependency chain are
REM NOT bundled into daily_update.exe, and this step can run for hours during
REM filing season, which would stall the daily pipeline.
cd /d C:\Claude\Invest
"C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe" -m scrapers.annual_report_revenue --daily >> tmp\annual_report_sweep.log 2>&1
