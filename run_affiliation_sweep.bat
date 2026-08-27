@echo off
REM Quarterly refresh of parent/investee holdings (tw.company_affiliations).
REM Runs on system Python rather than inside daily_update.exe: a fresh quarter
REM means ~2,300 companies at one request per second, which has no business
REM sitting in front of the nightly snapshot and export steps.
cd /d C:\Claude\Invest
"C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe" -m scrapers.mops_affiliations --quarterly >> tmp\affiliation_sweep.log 2>&1
