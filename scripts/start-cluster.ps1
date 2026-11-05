param(
    [string]$BaseDirectory = (Join-Path $PSScriptRoot '..\tmp\local-cluster'),
    [switch]$NoBuild
)

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$BaseDirectory = [System.IO.Path]::GetFullPath($BaseDirectory)
$BinDirectory = Join-Path $Root 'bin'
$Server = Join-Path $BinDirectory 'raftkv-server.exe'
$Client = Join-Path $BinDirectory 'raftkvctl.exe'
$Peers = 'n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003'

New-Item -ItemType Directory -Force -Path $BaseDirectory, $BinDirectory | Out-Null
if (-not $NoBuild) {
    $Go = (Get-Command go -ErrorAction SilentlyContinue).Source
    if (-not $Go) { $Go = 'C:\Program Files\Go\bin\go.exe' }
    & $Go build -o $Server ./cmd/raftkv-server
    & $Go build -o $Client ./cmd/raftkvctl
}

$Nodes = @(
    @{ Id = 'n1'; Address = '127.0.0.1:7001' },
    @{ Id = 'n2'; Address = '127.0.0.1:7002' },
    @{ Id = 'n3'; Address = '127.0.0.1:7003' }
)

foreach ($Node in $Nodes) {
    $NodeDirectory = Join-Path $BaseDirectory $Node.Id
    New-Item -ItemType Directory -Force -Path $NodeDirectory | Out-Null
    $StopFile = Join-Path $NodeDirectory 'stop.request'
    Remove-Item -LiteralPath $StopFile -Force -ErrorAction SilentlyContinue
    $Arguments = @(
        '-id', $Node.Id,
        '-listen', $Node.Address,
        '-peers', $Peers,
        '-data-dir', $NodeDirectory,
        '-stop-file', $StopFile
    )
    $Process = Start-Process -FilePath $Server -ArgumentList $Arguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $NodeDirectory 'stdout.log') `
        -RedirectStandardError (Join-Path $NodeDirectory 'stderr.log')
    Set-Content -LiteralPath (Join-Path $NodeDirectory 'node.pid') -Value $Process.Id
    Write-Host "Started $($Node.Id) pid=$($Process.Id) address=$($Node.Address)"
}

Write-Host "Cluster data: $BaseDirectory"
Write-Host "Client: $Client -client-id demo -seq 1 put greeting hello"
