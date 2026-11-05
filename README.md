# raftkv

`raftkv` is a portfolio project for learning how to build a fault-tolerant,
replicated key-value store in Go. The consensus algorithm will be implemented
in this repository rather than delegated to an existing Raft library.

> Status: Raft leader election, log replication, durable state/restart
> recovery, an in-memory key-value state machine, and gRPC peer/client
> networking are implemented and tested. Snapshots and production hardening
> remain planned.

## Scope

The planned system has static three-node and five-node clusters, leader
election, replicated logs, durable state, crash recovery, snapshots, and
linearizable `Put`, `Get`, and `Delete` client operations. gRPC will carry peer
and client traffic. A deterministic test harness and reproducible local demo
will exercise failures.

Dynamic membership, Kubernetes deployment, a web dashboard, and claims of
production readiness are deliberately out of scope.

## Repository layout

```text
api/raftkv/v1/       protobuf source (added with the transport stage)
cmd/raftkv-server/   server executable
cmd/raftkvctl/       command-line client
internal/raft/       consensus state machine and log rules
internal/storage/    durable Raft log, metadata, and snapshots
internal/transport/  gRPC peer and client transport
internal/kv/         deterministic key-value state machine
internal/server/     process wiring and request coordination
test/                deterministic integration and failure tests
docs/                design and implementation progress
```

The Raft package currently contains election and replication through ordered
state-machine application. See [`docs/design.md`](docs/design.md) for the
implemented flow, client retry assumptions, planned boundaries, and
invariants.

## Module path

The module currently uses the documented placeholder
`github.com/yourusername/raftkv`. Replace it with the final repository path
before publishing or adding generated protobuf imports.

## Prerequisites on Windows

Install Go and Protocol Buffers with Windows Package Manager, then open a new
PowerShell session:

```powershell
winget install --id GoLang.Go -e
winget install --id Google.Protobuf -e
```

After Go is available, install the Go protobuf generators:

```powershell
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Ensure Go's binary directory is on `PATH` (normally `%USERPROFILE%\go\bin`),
then verify:

```powershell
go version
protoc --version
protoc-gen-go --version
protoc-gen-go-grpc --version
```

## Checks

```powershell
go fmt ./...
go vet ./...
go test ./... -count=1
```

## Protobuf generation

```powershell
protoc --go_out=. --go_opt=paths=source_relative `
  --go-grpc_out=. --go-grpc_opt=paths=source_relative `
  api/raftkv/v1/raftkv.proto
```

The current generated files use `protoc` 36.2, `protoc-gen-go` 1.36.12, and
`protoc-gen-go-grpc` 1.6.2. Runtime dependencies are gRPC-Go 1.85.0 and
protobuf Go 1.36.12.

## Local three-node demonstration

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/start-cluster.ps1
bin/raftkvctl.exe -client-id demo -seq 1 put greeting hello
bin/raftkvctl.exe -client-id demo -seq 2 get greeting
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/stop-cluster.ps1
```

Nodes bind to `127.0.0.1:7001` through `127.0.0.1:7003` by default and use
separate directories below `tmp/local-cluster`. Run the complete leader
failure and restart scenario with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/integration.ps1
```

This is a local demonstration. It uses plaintext insecure gRPC credentials and
has no authentication, authorization, TLS, hostile-input hardening, or
production deployment support.

## Client retries and timeouts

`raftkvctl` reuses connections, follows leader hints, and retries transient
failures until its overall deadline. Every retry preserves the same client ID
and sequence number. Supply a stable `-client-id` and increase `-seq` by one
for each completed operation.

An RPC timeout is ambiguous: the command may have committed even though its
response was lost. The CLI reports that the outcome is unknown and prints the
identity that must be retried. A timeout does not prove that a write failed.

Completed work is tracked separately in [`docs/progress.md`](docs/progress.md).

