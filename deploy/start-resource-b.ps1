param(
  [Parameter(Mandatory=$true)][string]$Config,
  [ValidateSet('files','images','lora')][string]$Preset = 'files',
  [string]$Python = 'python.exe'
)
$ErrorActionPreference = 'Stop'
$Config = (Resolve-Path -LiteralPath $Config).ProviderPath
$runtimeDirectory = Split-Path -Parent $Config
$pidPath = Join-Path $runtimeDirectory 'b.pid'
if (Test-Path -LiteralPath $pidPath) {
  $oldId = [int](Get-Content -LiteralPath $pidPath -Raw)
  $oldProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$oldId"
  if ($oldProcess -and $oldProcess.CommandLine -and $oldProcess.CommandLine.Contains($Config) -and $oldProcess.CommandLine.Contains('aas.resources.cli')) {
    Write-Output "AAS B already running pid=$oldId"
    exit 0
  }
}
$logDirectory = Join-Path $runtimeDirectory 'logs'
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
$service = Start-Process -FilePath $Python -ArgumentList @('-c','"from aas.resources.cli import main_b; main_b()"','--config',('"'+$Config+'"'),'--preset',$Preset,'run') -WorkingDirectory $runtimeDirectory -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logDirectory 'b.log') -RedirectStandardError (Join-Path $logDirectory 'b.error.log') -PassThru
[IO.File]::WriteAllText($pidPath,[string]$service.Id)
Write-Output "AAS B started pid=$($service.Id)"
