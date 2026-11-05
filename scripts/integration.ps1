param(
    [string]$BaseDirectory = (Join-Path $PSScriptRoot "..\tmp\process-integration-$PID")
)

$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$BaseDirectory = [System.IO.Path]::GetFullPath($BaseDirectory)
$Server = Join-Path $Root 'bin\raftkv-server.exe'
$Client = Join-Path $Root 'bin\raftkvctl.exe'
$Addresses = '127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003'
$Peers = 'n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003'
$AddressByNode = @{ n1 = '127.0.0.1:7001'; n2 = '127.0.0.1:7002'; n3 = '127.0.0.1:7003' }

function Invoke-Client([string[]]$Arguments) {
    $Output = & $Client -addr $Addresses -timeout 15s @Arguments
    if ($LASTEXITCODE -ne 0) { throw "client failed: $Output" }
    return ($Output | ConvertFrom-Json)
}

function Start-Node([string]$Id, [string]$LogSuffix = '') {
    $NodeDirectory = Join-Path $BaseDirectory $Id
    New-Item -ItemType Directory -Force -Path $NodeDirectory | Out-Null
    $StopFile = Join-Path $NodeDirectory 'stop.request'
    Remove-Item -LiteralPath $StopFile -Force -ErrorAction SilentlyContinue
    $Arguments = @(
        '-id', $Id, '-listen', $AddressByNode[$Id], '-peers', $Peers,
		'-data-dir', $NodeDirectory, '-stop-file', $StopFile,
		'-snapshot-threshold', '3', '-max-snapshot-bytes', '2097152'
    )
    $Process = Start-Process -FilePath $Server -ArgumentList $Arguments -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $NodeDirectory "stdout$LogSuffix.log") `
        -RedirectStandardError (Join-Path $NodeDirectory "stderr$LogSuffix.log")
    Set-Content -LiteralPath (Join-Path $NodeDirectory 'node.pid') -Value $Process.Id
    return $Process
}

New-Item -ItemType Directory -Force -Path (Join-Path $Root 'bin'), $BaseDirectory | Out-Null
$Go = (Get-Command go -ErrorAction SilentlyContinue).Source
if (-not $Go) { $Go = 'C:\Program Files\Go\bin\go.exe' }
& $Go build -o $Server ./cmd/raftkv-server
& $Go build -o $Client ./cmd/raftkvctl

try {
    $Processes = @{}
    foreach ($Id in @('n1', 'n2', 'n3')) { $Processes[$Id] = Start-Node $Id }

    $FirstWrite = Invoke-Client @('-client-id', 'integration-writer', '-seq', '1', 'put', 'alpha', 'one')
    $FirstRead = Invoke-Client @('-client-id', 'integration-reader', '-seq', '1', 'get', 'alpha')
    if (-not $FirstRead.found -or $FirstRead.value -ne 'one') { throw 'first read did not observe first write' }

    $OldLeader = [string]$FirstWrite.node_id
    Stop-Process -Id $Processes[$OldLeader].Id -Force
    $SecondWrite = Invoke-Client @('-client-id', 'integration-writer', '-seq', '2', 'put', 'beta', 'two')
    if ($SecondWrite.node_id -eq $OldLeader) { throw 'write after failure was served by stopped leader' }
    $SecondRead = Invoke-Client @('-client-id', 'integration-reader', '-seq', '2', 'get', 'beta')
    if (-not $SecondRead.found -or $SecondRead.value -ne 'two') { throw 'second read did not observe failover write' }

    $Processes[$OldLeader] = Start-Node $OldLeader '-restart'
    $StatePath = Join-Path (Join-Path $BaseDirectory $OldLeader) 'raft-state.json'
    $Deadline = (Get-Date).AddSeconds(15)
    $CaughtUp = $false
    do {
        try {
            $State = Get-Content -Raw -LiteralPath $StatePath | ConvertFrom-Json
			$InLog = $State.payload.Log | Where-Object { $_.Command.Key -eq 'beta' -and $_.Command.Value -eq 'two' }
			$InSnapshot = $false
			if ($State.payload.Snapshot.Data) {
				$SnapshotJson = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($State.payload.Snapshot.Data)) | ConvertFrom-Json
				$InSnapshot = $SnapshotJson.values.beta -eq 'two'
			}
			if ($InLog -or $InSnapshot) {
                $CaughtUp = $true
            }
        } catch {}
        if (-not $CaughtUp) { Start-Sleep -Milliseconds 100 }
    } while (-not $CaughtUp -and (Get-Date) -lt $Deadline)
    if (-not $CaughtUp) { throw "$OldLeader did not catch up before deadline" }

    $FinalRead = Invoke-Client @('-client-id', 'integration-reader', '-seq', '3', 'get', 'beta')
    if (-not $FinalRead.found -or $FinalRead.value -ne 'two') { throw 'final read failed after old leader restart' }

    [pscustomobject]@{
        old_leader = $OldLeader
        replacement_leader = $SecondWrite.node_id
        first_value = $FirstRead.value
        second_value = $SecondRead.value
        restarted_node_caught_up = $CaughtUp
        data_directory = $BaseDirectory
    } | ConvertTo-Json
}
finally {
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'stop-cluster.ps1') -BaseDirectory $BaseDirectory | Out-Host
}
