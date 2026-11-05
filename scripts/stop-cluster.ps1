param(
    [string]$BaseDirectory = (Join-Path $PSScriptRoot '..\tmp\local-cluster'),
    [int]$GraceSeconds = 8
)

$ErrorActionPreference = 'Stop'
$BaseDirectory = [System.IO.Path]::GetFullPath($BaseDirectory)
foreach ($Id in @('n1', 'n2', 'n3')) {
    $NodeDirectory = Join-Path $BaseDirectory $Id
    $PidFile = Join-Path $NodeDirectory 'node.pid'
    if (-not (Test-Path -LiteralPath $PidFile)) { continue }
    $ProcessId = [int](Get-Content -LiteralPath $PidFile -Raw)
    New-Item -ItemType File -Force -Path (Join-Path $NodeDirectory 'stop.request') | Out-Null
    $Deadline = (Get-Date).AddSeconds($GraceSeconds)
    while ((Get-Process -Id $ProcessId -ErrorAction SilentlyContinue) -and (Get-Date) -lt $Deadline) {
        Start-Sleep -Milliseconds 100
    }
    if (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue) {
        Stop-Process -Id $ProcessId -Force
        Write-Warning "Forced stop for $Id after graceful timeout"
    } else {
        Write-Host "Stopped $Id gracefully"
    }
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
}
