# Progress

## Completed

- Implemented a caller-serialized Raft core with follower, candidate, and leader roles, randomized elections, heartbeats, higher-term step-down, and vote freshness checks.
- Implemented absolute-indexed log replication, conflict repair, per-follower progress, leader no-op entries, majority commit with the current-term rule, and ordered application.
- Implemented replicated `Put`, `Delete`, and `Get`, plus bounded replicated client retry deduplication.
- Implemented checksummed durable term, vote, log, commit index, snapshot, and suffix state using synced temporary files and atomic replacement.
- Implemented recovery that restores snapshot state and replays only the known committed suffix; persisted uncommitted entries are not applied.
- Implemented committed/applied snapshots, log compaction, compatible suffix preservation, `InstallSnapshot`, and resumption of ordinary replication.
- Implemented gRPC peer/client APIs, reusable connections, bounded deadlines, stable retry identities, leader hints, structured logs, and graceful process shutdown.
- Preserved deterministic in-memory transport tests and added five-node seeded failure scenarios, a bounded history checker, a durable benchmark, and a real-process failover/snapshot demonstration.
- Audited and fixed duplicate gRPC response capture: response keys now retain FIFO waiter lists instead of overwriting an earlier concurrent waiter.
- Added Linux unit/race CI, Windows process-integration CI, and separately runnable seeded randomized CI.

## Verification status

Exact local commands, durations, seeds, source fingerprint, raw benchmark samples, and blocked checks are in `docs/test-report.md`. Local Windows race testing remains blocked by `CGO_ENABLED=0`, no C compiler, and WDAC. GitHub Actions successfully completed Linux formatting/vet/unit/race, Windows real-process integration, and the separately dispatched five-node randomized workflow on reviewed commit `a7b6ee6e46537a35cec9abcebe06a011cbd9bec8`.

## Current limitations

- Static three-node/five-node membership only.
- Whole-state synchronous rewrites rather than a WAL, batching, or group commit.
- Snapshot transfer is one bounded unary RPC (default 2 MiB), not streamed.
- Deduplication is bounded and relies on one outstanding monotonic sequence per client; it is not universal exactly-once delivery.
- `Get` is replicated through the log; there is no optimized read protocol.
- No TLS, authentication, authorization, dynamic membership, Kubernetes, dashboard, online backup, or production-hardening claim.
- The history checker explores at most eight completed operations and omits incomplete operations. A pass covers only the explored completed subhistory; timed-out outcomes remain unknown.
- Local race verification is blocked; Linux CI race verification passed on the reviewed implementation commit.
- The Go module path remains a publication placeholder.

## Next steps

No new feature is requested in this stage. Before broader publication, replace the placeholder module path and confirm that the documented filesystem assumptions match the intended deployment.
