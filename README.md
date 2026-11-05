# raftkv

`raftkv` is an educational, fault-tolerant key-value store written in Go. It implements Raft directly, uses gRPC for peer and client traffic, and supports static three-node and five-node clusters. It is a portfolio implementation, not a production service.

Implemented capabilities include leader election, replicated `Put`, `Get`, and `Delete`, durable term/vote/log/commit state, restart recovery, bounded retry deduplication, snapshots, log compaction, snapshot catch-up, and deterministic failure tests. `Get` deliberately goes through the replicated log for a simple linearizable read path.

```text
raftkvctl -> gRPC client service -> serialized node runtime -> Raft core
                                                        |       |
                                                        |       +-> peer gRPC
                                                        +-> KV state machine
                                                        +-> checksummed FileStore
```

The Raft core has no dependency on gRPC, wall-clock timers, or filesystem APIs. A runtime goroutine serializes real events; tests inject logical time, storage, and an in-memory network.

## Repository layout

```text
api/raftkv/v1/       protobuf API and generated Go
cmd/raftkv-server/   node executable
cmd/raftkvctl/       retrying client CLI
internal/raft/       consensus core, tests, randomized harness, benchmark
internal/kv/         deterministic KV state machine and snapshot encoding
internal/storage/    checksummed atomic-replacement stable store
internal/transport/  gRPC peer transport and response capture
internal/server/     serialized runtime and gRPC services
scripts/             Windows local-cluster and integration scripts
docs/                design, audit, evidence, and interview guide
```

## Setup and build

The module path `github.com/yourusername/raftkv` is a documented placeholder and should be replaced before publication under a final repository URL.

On Windows, install prerequisites and open a new PowerShell session:

```powershell
winget install --id GoLang.Go -e
winget install --id Google.Protobuf -e
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
```

Build and regenerate protobuf code:

```powershell
go build -o bin/raftkv-server.exe ./cmd/raftkv-server
go build -o bin/raftkvctl.exe ./cmd/raftkvctl
protoc --go_out=. --go_opt=paths=source_relative `
  --go-grpc_out=. --go-grpc_opt=paths=source_relative `
  api/raftkv/v1/raftkv.proto
```

The checked-in generated files use protobuf 36.2, `protoc-gen-go` 1.36.12, and `protoc-gen-go-grpc` 1.6.2. Runtime dependencies are pinned in `go.mod` and `go.sum`.

## Three-node operation

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/start-cluster.ps1
bin/raftkvctl.exe -client-id demo -seq 1 put greeting hello
bin/raftkvctl.exe -client-id demo -seq 2 get greeting
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/stop-cluster.ps1
```

Nodes bind to `127.0.0.1:7001` through `:7003` and use separate directories under `tmp/local-cluster`. Run the leader-failure, replacement-leader, restart, and snapshot catch-up demonstration with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/integration.ps1 `
  -BaseDirectory ./tmp/snapshot-integration
```

## Five-node operation

Start five server processes with distinct IDs, listen addresses, and data directories and the same peer map:

```text
n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003,n4=127.0.0.1:7004,n5=127.0.0.1:7005
```

Run each as `raftkv-server -id nN -listen 127.0.0.1:700N -peers <map> -data-dir tmp/five/nN`. Five nodes tolerate two failures but require three mutually connected nodes to elect and commit.

## Snapshots and restart

The server defaults to compacting after 100 applied entries and accepts a bounded unary snapshot up to 2 MiB. Override `-snapshot-threshold` and `-max-snapshot-bytes`. Restart a node with the same identity, peer map, and data directory to restore its snapshot, committed suffix, and deduplication records. A follower behind the leader's compacted boundary receives `InstallSnapshot`, then resumes `AppendEntries`.

Snapshots contain only committed, applied KV and deduplication state. They are persisted with the retained suffix in one complete-state replacement before the old prefix is discarded in memory. Transfer is a single unary RPC with no streaming or chunking.

## Verification and benchmark

```powershell
gofmt -l .
go vet ./...
go test ./... -count=1
$env:RAFTKV_RANDOMIZED='1'; $env:RAFTKV_SEEDS='101,202,303,404,505'
go test ./internal/raft -run TestRandomizedFiveNodeSafetyAndRecovery -count=1 -v
go test ./internal/raft -run '^$' -bench BenchmarkThreeNodeDurablePut `
  -benchtime=20x -count=3 -benchmem
go mod verify
```

See [docs/test-report.md](docs/test-report.md) for commands actually completed, measured results, source identity, and blocked checks. See [docs/correctness-audit.md](docs/correctness-audit.md) for the claim-by-claim audit.

## Tradeoffs and limitations

- Static membership; no joint consensus or membership changes.
- Each safety-critical change rewrites and syncs the complete JSON state, which is simple but slow compared with a WAL and group commit.
- Bounded deduplication assumes unique client IDs, monotonically increasing sequences, one outstanding request per client, and retry before eviction. It is not universal exactly-once delivery.
- Timeouts are ambiguous: retry the same client ID and sequence.
- `Get` consumes a log entry; there is no ReadIndex or lease-read optimization.
- Unary snapshots are bounded and copied in memory; no streaming.
- Plaintext loopback gRPC only: no TLS, authentication, authorization, hostile-input hardening, dynamic discovery, or production operations.
- Deterministic and bounded history checks are testing evidence, not a formal proof of Raft safety or linearizability.

Technical references are the extended Raft paper by Diego Ongaro and John Ousterhout, official Go documentation and memory model, and official gRPC Go and Protocol Buffers documentation.
