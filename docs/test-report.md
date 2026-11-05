# Test report

Date: 2026-11-05 (America/New_York). Commands ran from the repository root in Windows PowerShell. Durations are wall-clock measurements from `Stopwatch` or the command runner; small values vary by machine load.

## Source identity

- HEAD: `fa3c0b5466821681b83a02108ed7443f26a593f7` (`bully_algo_implemented`).
- Worktree: dirty. Documentation files and a formatting-only change to `cmd/raftkv-server/main.go` are uncommitted. HEAD alone does not identify this tested source.
- Reproducible content fingerprint (SHA-256 over sorted `path<TAB>file-SHA256` records for all non-ignored source files except this report): `d44854c056fc87610094d6f45a78e733e2e7448699d769187d486c8b69da2f08`.
- Ignored `bin/` and `tmp/` artifacts are excluded from the fingerprint.

Toolchain: Go 1.27.3 windows/amd64, `CGO_ENABLED=0`; protobuf 36.2; `protoc-gen-go` 1.36.12; `protoc-gen-go-grpc` 1.6.2.

## Formatting and vet

Initial `gofmt -l` found `cmd/raftkv-server/main.go`; it was formatted. Final commands:

```powershell
$files = Get-ChildItem -Recurse -Filter *.go | Where-Object { $_.FullName -notmatch '\\.git\\|\\bin\\|\\tmp\\' } | ForEach-Object FullName
gofmt -l $files
go vet ./...
```

Outcomes: formatting passed with no listed files in 21 ms; vet passed in 3,613 ms. A final repeat after documentation-only edits was blocked by the same Windows Go-cache access policy, so the recorded pass is from the final code state but before the last documentation edits. `git diff --check` passed, with only Git's LF-to-CRLF working-copy warnings.

## Unit and deterministic tests

The ordinary command was attempted:

```powershell
go test ./... -count=1
```

It was incomplete/failed after 4,556 ms because Windows Application Control blocked a temporary `storage.test.exe` and the Go cache returned access denied. Raft and transport happened to pass in that partial run, but the aggregate command is not reported as passed.

The reliable local equivalent compiled test packages into ignored `bin/` and executed them:

```powershell
go test -c -o bin/raft-final.test.exe ./internal/raft
./bin/raft-final.test.exe -test.count=1 -test.v
go test -c -o bin/storage-final.test.exe ./internal/storage
./bin/storage-final.test.exe -test.count=1 -test.v
go test -c -o bin/transport-final.test.exe ./internal/transport
./bin/transport-final.test.exe -test.count=1 -test.v
```

All executed tests passed. Compile/run durations were Raft 1,329/274 ms, storage 1,276/299 ms, and transport 2,909/1,016 ms. The gated randomized test was skipped in this ordinary Raft invocation and run separately below. The bounded history test reported the order `put-one, get-one, put-two, get-two`.

The history checker exhaustively searches at most eight completed operations. It enforces real-time precedence among completed calls. Calls with no completion (including modeled timeouts) are omitted because their outcome is unknown. Therefore this pass establishes a sequential explanation only for the four-operation completed subhistory. It says nothing conclusive about the omitted delete, and a history above the bound returns failure without distinguishing resource-bound inconclusive from a true counterexample.

## Seeded five-node randomized scenarios

```powershell
$env:RAFTKV_RANDOMIZED='1'
$env:RAFTKV_SEEDS=((1000..1099) -join ',')
./bin/raft-final.test.exe `
  -test.run='^TestRandomizedFiveNodeSafetyAndRecovery$' -test.count=1 -test.v
```

Outcome: 100 seeds (`1000` through `1099`, inclusive) passed in 145 ms wall time; the Go test reported 0.13 s. The harness exercises delivery, loss, duplication, partitions, crashes/restarts, and snapshot catch-up, checks safety during disruption, then heals the cluster and checks liveness under fair delivery. It prints the seed and the last 200 events on failure; no failure trace was produced.

## Real-process integration

Final command:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/integration.ps1 `
  -BaseDirectory ./tmp/final-integration
```

Outcome: blocked/failed after 4,553 ms. Windows Application Control blocked the newly built `raftkvctl.exe`; the Go build also encountered cache access denial. The script stopped all three processes gracefully. This final-source process check is not passed.

Follow-up diagnostics identified Code Integrity events 3033/3077 for policy ID `0283ac0f-fff1-49ae-ada1-8a933130cad6`: the generated executable did not meet enterprise signing requirements. The file has no `Zone.Identifier` alternate stream, so `Unblock-File` cannot resolve it. The active system state reports `VerifiedAndReputablePolicyState=1`, and the matching compiled policy is installed under the Windows Code Integrity policy directory. This is a machine-wide WDAC/Smart App Control restriction, not a repository permission setting. WSL, Docker, Podman, GCC, Clang, and MSVC were not installed, so no existing alternate execution environment was available.

Prior local evidence from this audit session, before the formatting-only server change, completed the same script in 10.2 s: leader `n2` was stopped, `n3` replaced it, reads returned `one` and `two`, the old node restarted and caught up through snapshot/log state, and all nodes stopped gracefully. This is reported as prior evidence, not as a successful final rerun.

## Race testing and CI

```powershell
go test -race ./internal/... -count=1
```

Outcome: blocked in 30 ms with `go: -race requires cgo; enable cgo by setting CGO_ENABLED=1`. The environment still has `CGO_ENABLED=0` and no C compiler was installed or configured.

Installing a Windows C compiler alone would not complete this check because WDAC also rejects the unsigned Go-generated test executable. Safe remedies are: run the existing Linux CI workflow, use an installed Linux/WSL environment, or have the machine administrator deploy a scoped WDAC supplemental allow policy/enterprise signing rule for this development workspace. Disabling Smart App Control globally was not performed because it is a machine-wide security reduction and can be irreversible without a Windows reset/reinstall.

`.github/workflows/ci.yml` defines an Ubuntu `go test -race ./internal/... -count=1` job and a Windows integration job. `.github/workflows/randomized.yml` defines manually dispatched fixed-seed testing. A workflow definition is not execution evidence. No completed run for this exact dirty source was accessible, so race verification is pending CI.

## Generated code and dependencies

```powershell
protoc --go_out=. --go_opt=paths=source_relative `
  --go-grpc_out=. --go-grpc_opt=paths=source_relative `
  api/raftkv/v1/raftkv.proto
go mod verify
go list -m all
```

Protobuf regeneration passed in 97 ms and reproduced both files byte-for-byte: `raftkv.pb.go` SHA-256 `8BBEB6AE053D51FD9E7E4A72A5E007C186DC4BD4FD89F412FD1FA5E32BB25B4E`; `raftkv_grpc.pb.go` SHA-256 `991F516D81E6F3327C4DF9B38BE47CC72E64866118D655D6CE7989CB5B577102`. `go mod verify` passed in 360 ms. `go list -m all` initially could not reach the sandbox proxy, then passed with approved network access in 329 ms. Direct dependencies are gRPC-Go 1.85.0 and protobuf Go 1.36.12; the full module graph was inspected and contained their expected transitive/test ecosystem.

## Repository-content review

Commands used `git ls-files`, `git status --ignored --short`, and recursive scans excluding `.git`, `bin`, and `tmp` for local Windows/macOS/Linux home paths and secret-like assignments.

- No tracked executable, test binary, log, or runtime data directory was found.
- No machine-specific home or workspace absolute paths were found in repository source after this review.
- No credential-like assignment matched the scan. This is a heuristic review, not a guarantee that no secret exists.
- Ignored local artifacts exist in `bin/` and `tmp/`; they are not tracked and were not deleted.
- Generated protobuf files are necessary and reproducible.
- The remote URL contains only a public GitHub repository address; no embedded credential was observed.
- Public-review caveats are stated: placeholder module path, plaintext demo, bounded snapshots/deduplication/checker, no production-readiness claim.

## Durable three-node benchmark

Environment: Microsoft Windows NT 10.0.26300.0, windows/amd64, AMD Ryzen 9 5900HS with Radeon Graphics, 16 logical processors, Go 1.27.3. Physical memory information was not available through the restricted local query.

Topology/workload: three in-process Raft nodes, each using its own `FileStore`; one elected leader; sequential committed `Put` operations; concurrency 1; key `key` (3 bytes); values `value-N` (7 bytes for these 20-operation samples); a unique client ID per operation; 20 operations per sample; three samples. Every safety-critical save rewrites the complete checksummed JSON state, syncs the temporary file, and atomically replaces the live file. No batching or group commit is used.

```powershell
go test -c -o bin/raft-final-bench.test.exe ./internal/raft
./bin/raft-final-bench.test.exe -test.run='^$' `
  -test.bench='^BenchmarkThreeNodeDurablePut$' `
  -test.benchtime=20x -test.count=3 -test.benchmem
```

Compile passed in 1,442 ms; benchmark execution passed in 1,339 ms. Raw samples:

```text
20  13,530,685 ns/op  45,971 B/op  157 allocs/op
20  13,144,645 ns/op  45,538 B/op  157 allocs/op
20  13,676,700 ns/op  45,724 B/op  157 allocs/op
```

Measured latency mean: 13,450,677 ns/op (13.451 ms/op); median: 13,530,685 ns/op (13.531 ms/op). Mean allocation volume: 45,744 B/op; all samples reported 157 allocations/op. These are only the measured local samples, not a production capacity claim.

## Final status

Passed locally: formatting, vet, compiled unit/deterministic packages, 100 seeded randomized scenarios, protobuf reproducibility, module checksum verification, module-graph listing, repository-content scans, and the durable benchmark.

Blocked/pending: direct aggregate `go test ./...`, final real-process rerun, local race detector, and completed CI evidence for this exact source. The prior process integration success remains useful but does not replace a final-source pass.
