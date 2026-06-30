# Install the micro-Taiex PAPER forward-test as an NSSM service.
# Run as Administrator. Requires nssm.exe (pass -Nssm <path> or have it on PATH).
#
#   powershell -ExecutionPolicy Bypass -File ops\install_paper_service.ps1
#
# Distinct service name "microtaiex-paper" so it is unambiguous to start/stop and
# never collides with other tools' processes. Logs -> reports\paper_service.log
# (rotated). Auto-starts on boot, auto-restarts on crash. It idles off-session and
# reconnects on disconnect (serve()), so leaving it running 24/7 is fine.

param(
  [string]$Python  = "C:\Users\Real\AppData\Local\Programs\Python\Python312\python.exe",
  [string]$Project = "C:\Claude\Invest\microtaiex-daytrade",
  [string]$Nssm    = "C:\Claude\Invest\tools\nssm.exe",
  [string]$Service = "InvestMicroPaper"
)

if (-not (Test-Path $Nssm)) {
  Write-Host "[ERROR] nssm.exe not found at $Nssm  -- pass -Nssm <path>"
  exit 1
}
$log = Join-Path $Project "reports\paper_service.log"
$err = Join-Path $Project "reports\paper_service.err.log"

& $Nssm install $Service $Python "$Project\run_live.py" "--paper"
& $Nssm set $Service AppDirectory $Project            # so load_dotenv finds .env here
& $Nssm set $Service AppEnvironmentExtra "PYTHONUNBUFFERED=1" "PYTHONIOENCODING=utf-8"   # live + readable logs
& $Nssm set $Service AppStdout $log
& $Nssm set $Service AppStderr $err
& $Nssm set $Service AppStdoutCreationDisposition 4    # append (4 = OPEN_ALWAYS)
& $Nssm set $Service AppStderrCreationDisposition 4
& $Nssm set $Service AppRotateFiles 1
& $Nssm set $Service AppRotateOnline 1
& $Nssm set $Service AppRotateBytes 10485760          # rotate at ~10 MB
& $Nssm set $Service Start SERVICE_AUTO_START         # start on boot
& $Nssm set $Service AppExit Default Restart          # restart on crash
& $Nssm set $Service AppRestartDelay 15000            # wait 15s before restart

Write-Host ""
Write-Host "Installed service '$Service'."
Write-Host "  start:   $Nssm start $Service"
Write-Host "  stop:    $Nssm stop $Service"
Write-Host "  status:  $Nssm status $Service"
Write-Host "  log:     $log"
Write-Host ""
Write-Host "NOTE: services run in Session 0 (no desktop). If SKCOM cannot log in"
Write-Host "under LocalSystem, run the service as your user account instead:"
Write-Host "  $Nssm set $Service ObjectName .\<your_user> <your_password>"
