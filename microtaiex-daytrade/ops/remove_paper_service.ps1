# Stop and remove the micro-Taiex PAPER NSSM service.
# Run as Administrator.
#
#   powershell -ExecutionPolicy Bypass -File ops\remove_paper_service.ps1

param(
  [string]$Nssm    = "C:\Claude\Invest\tools\nssm.exe",
  [string]$Service = "InvestMicroPaper"
)

& $Nssm stop $Service
& $Nssm remove $Service confirm
Write-Host "Removed service '$Service'."
