@echo off
REM Install Invest intraday services as NSSM-managed Windows services.
REM
REM Services managed by this script:
REM   InvestIntradaySnapshot - dist\intraday_snapshot.exe   (snapshot daemon)
REM   InvestIntradaySweeper  - dist\intraday_sweep_update.exe (h(t) sweeper)
REM
REM Both auto-start at boot, get auto-restarted on crash by NSSM, write
REM stdout/stderr to logs\ with 10 MB rotation.
REM
REM Does NOT touch:
REM   InvestTelegramBot (existing NSSM service, unrelated)
REM   intraday_publish (Task Scheduler)
REM
REM Run as Administrator (required by sc / nssm).
REM
REM Usage:
REM   install_invest_services.bat            install + start (idempotent)
REM   install_invest_services.bat uninstall  stop + remove (rollback)

setlocal
set NSSM=C:\Claude\Invest\tools\nssm.exe
set REPO=C:\Claude\Invest

if not exist "%NSSM%" (
    echo [ERROR] nssm.exe not found at %NSSM%
    exit /b 1
)

if not exist %REPO%\logs mkdir %REPO%\logs

if /I "%1"=="uninstall" goto :uninstall

REM ============================================================
REM Install + start (idempotent: removes existing first)
REM ============================================================

call :install_one InvestIntradaySnapshot intraday_snapshot.exe      snapshot
if errorlevel 1 exit /b 2

call :install_one InvestIntradaySweeper  intraday_sweep_update.exe  sweep_update
if errorlevel 1 exit /b 2

echo.
echo === Done ===
echo.
echo Verify:
echo   sc query InvestIntradaySnapshot
echo   sc query InvestIntradaySweeper
echo.
echo Logs:
echo   %REPO%\logs\snapshot.log      / snapshot.err.log
echo   %REPO%\logs\sweep_update.log  / sweep_update.err.log
echo.
echo To restart after a rebuild:
echo   nssm restart InvestIntradaySnapshot
echo   nssm restart InvestIntradaySweeper
exit /b 0


REM ------------------------------------------------------------
:install_one
REM %1 = service name, %2 = exe filename, %3 = log filename stem
set SVC=%1
set EXE_NAME=%2
set LOG_STEM=%3

echo.
echo === %SVC% ===

REM Stop + remove if already installed (idempotent reinstall).
sc query %SVC% > nul 2>&1
if not errorlevel 1 call :remove_existing %SVC%

if not exist "%REPO%\dist\%EXE_NAME%" (
    echo [ERROR] %REPO%\dist\%EXE_NAME% missing - build it first
    exit /b 1
)

echo Installing %SVC%...
"%NSSM%" install %SVC% "%REPO%\dist\%EXE_NAME%"
if errorlevel 1 (
    echo [ERROR] nssm install %SVC% failed
    exit /b 1
)

"%NSSM%" set %SVC% AppDirectory                     "%REPO%"               > nul
"%NSSM%" set %SVC% AppStdout                        "%REPO%\logs\%LOG_STEM%.log"     > nul
"%NSSM%" set %SVC% AppStderr                        "%REPO%\logs\%LOG_STEM%.err.log" > nul
"%NSSM%" set %SVC% AppStdoutCreationDisposition     4                      > nul
"%NSSM%" set %SVC% AppStderrCreationDisposition     4                      > nul
"%NSSM%" set %SVC% AppRotateFiles                   1                      > nul
"%NSSM%" set %SVC% AppRotateOnline                  1                      > nul
"%NSSM%" set %SVC% AppRotateBytes                   10485760               > nul
"%NSSM%" set %SVC% Start                            SERVICE_AUTO_START     > nul
"%NSSM%" set %SVC% AppRestartDelay                  5000                   > nul
"%NSSM%" set %SVC% AppExit                          Default Restart        > nul

echo Starting %SVC%...
"%NSSM%" start %SVC%
goto :eof


REM ------------------------------------------------------------
:remove_existing
set OLD=%1
echo Stopping existing %OLD%...
"%NSSM%" stop %OLD% > nul 2>&1
echo Removing existing %OLD%...
"%NSSM%" remove %OLD% confirm > nul 2>&1
REM Wait for SCM to release the service handle. If services.msc or Task
REM Manager Services tab is open, this might not be enough - close them.
timeout /t 5 /nobreak > nul
goto :eof


REM ------------------------------------------------------------
:uninstall
echo.
echo === Uninstalling Invest intraday services ===
call :uninstall_one InvestIntradaySnapshot
call :uninstall_one InvestIntradaySweeper
echo.
echo Done. InvestTelegramBot untouched.
exit /b 0


:uninstall_one
set SVC=%1
sc query %SVC% > nul 2>&1
if errorlevel 1 (
    echo %SVC% not installed.
    goto :eof
)
echo Stopping %SVC%...
"%NSSM%" stop %SVC% > nul 2>&1
echo Removing %SVC%...
"%NSSM%" remove %SVC% confirm > nul 2>&1
goto :eof
