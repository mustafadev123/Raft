# raftkv

`raftkv` is a portfolio project for learning how to build a fault-tolerant,
replicated key-value store in Go. The consensus algorithm will be implemented
in this repository rather than delegated to an existing Raft library.

> Status: repository skeleton only. Raft, persistence, RPCs, the key-value
> operations, recovery, snapshots, and failure tests are planned but are not
> implemented yet.

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

The packages currently contain only compiling placeholders. See
[`docs/design.md`](docs/design.md) for the planned boundaries and invariants.

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

## Current checks

Once Go is installed, the stage-one skeleton should be checked with:

```powershell
go fmt ./...
go vet ./...
go test ./...
```

No generated code is checked in at this stage because no RPC schema or Raft
implementation has been requested yet.

## Planned stages

1. Define Raft types and a deterministic single-node core with unit tests.
2. Add election timers, RequestVote, and deterministic election tests.
3. Add AppendEntries, conflict handling, commit rules, and replication tests.
4. Add durable storage and crash recovery.
5. Define protobuf APIs and implement gRPC peer transport.
6. Add the KV state machine and client-facing `Put`, `Get`, and `Delete` RPCs.
7. Add snapshots and log compaction.
8. Add deterministic three-node/five-node failure tests and a local demo.

Completed work is tracked separately in [`docs/progress.md`](docs/progress.md).

