# Design

## Architecture

The implementation separates a deterministic Raft state machine (`internal/raft`) from timing and event serialization (`internal/server`), durable storage (`internal/storage`), networking (`internal/transport`), and deterministic command execution (`internal/kv`). The production runtime has one goroutine that owns each Raft node. The core does not open files, start goroutines, read a wall clock, or call gRPC.

Static membership is supplied at startup. Three-node and five-node configurations are supported; membership changes are out of scope.

## Election and replication

A follower starts an election after an injected randomized deadline. It increments and persists its term and self-vote before sending `RequestVote`. Vote freshness compares last-log term and then absolute index. A majority includes the candidate's own vote. Higher terms cancel election/leadership contexts, clear `votedFor`, persist the new term, and force follower state. Same-term valid `AppendEntries` or `InstallSnapshot` establishes a leader and resets the election deadline. Rejected stale vote traffic does not reset it.

Every leader appends and persists a current-term no-op. Followers accept entries only when the preceding absolute index/term matches. A conflict truncates only an uncommitted suffix. Leaders track `nextIndex` and `matchIndex`; failed replies are acted on only when their request ID is current, while successful delayed duplicates can only increase progress. Commit advances after a majority matches an entry from the leader's current term. Earlier entries become committed as part of that prefix.

`lastApplied` advances one index at a time through `commitIndex`. `Put`, `Delete`, and `Get` all use this path, and client responses wait for local commitment and application. Replicating `Get` avoids unsafe follower/local-leader reads at the cost of a log entry per read.

## Client identity and deduplication

Commands replicate a unique client ID and monotonically increasing sequence. The KV state machine retains the latest sequence and result for a bounded number of clients. A retry of that retained request returns the original result without repeating a mutation. The model assumes one outstanding request per client, sequences beginning at one and increasing by one, and retry before deterministic cache eviction. It is not universal exactly-once delivery. RPC timeouts are ambiguous and must be retried with the same identity.

## Persistence and recovery

Persistent state contains `currentTerm`, `votedFor`, the retained log suffix, `commitIndex`, and a snapshot. A versioned JSON payload has a SHA-256 checksum. Each save writes a same-directory temporary file, syncs it, closes it, and atomically replaces the live file. Unix additionally syncs the directory; Windows uses `MoveFileExW` with replace-existing and write-through flags.

Writing copies bytes toward the OS cache. Flushing a language buffer would only pass buffered bytes to the OS; this implementation writes directly. `Sync` asks the OS to push contents and metadata toward stable storage. Atomic replacement chooses a complete generation but is not itself a media-durability guarantee.

Safety-critical term, vote, log, and commit changes are saved before dependent RPC responses, replication sends, state-machine application, or successful client completion. A save error faults the node and cancels pending leadership work. Loading rejects invalid JSON, checksums, versions, boundaries, gaps, and impossible commit indices rather than silently starting empty.

Recovery restores snapshot data first and applies only the persisted committed suffix. Persisted entries beyond `commitIndex` remain uncommitted. Replaying committed commands reconstructs deduplication state when no snapshot covers them.

This assumes one writer per node directory, a local filesystem honoring same-directory atomic replacement and successful sync, and truthful filesystem/device flush reporting. It does not protect against lying controllers, volatile caches without power protection, disabled barriers, filesystem bugs, concurrent writers, later media corruption, or every possible power-loss mode.

## Snapshot boundary and indexing

`lastIncludedIndex` and `lastIncludedTerm` describe the final entry represented by the snapshot. Log entries after compaction retain absolute indices; the first suffix entry is `lastIncludedIndex + 1`. Index zero is the empty boundary.

Compaction snapshots only state through `lastApplied`, which cannot exceed `commitIndex`. KV values and bounded client deduplication records are encoded together. The new snapshot and retained suffix are persisted as one complete state before the in-memory prefix is removed. Repeated compaction therefore never renumbers entries.

On installation, a follower rejects oversized or invalid data and safely acknowledges an obsolete boundary without moving backward. If it has the same term at the incoming boundary, it preserves the compatible suffix; otherwise it discards only the incompatible suffix, and it never replaces a conflicting already-committed prefix. The snapshot is persisted before the state machine is restored and before success is returned. Subsequent suffix entries are applied in order and normal replication resumes.

Snapshot transfer is a single unary gRPC request. The server default maximum is 2 MiB plus protocol overhead, and the default compaction threshold is 100 applied entries. There is no chunking, streaming, compression, or incremental snapshot.

## Networking and lifecycle

Protobuf defines `RequestVote`, `AppendEntries`, `InstallSnapshot`, and client `Put`, `Get`, and `Delete`. The gRPC transport reuses one connection per peer and gives each call a bounded context. It never holds consensus state across network calls. Response captures are FIFO lists keyed by peer plus election/request ID so concurrent duplicate inbound RPCs do not overwrite waiters. Handler cancellation removes its own waiter.

Shutdown stops new server traffic, cancels the process context, waits for the serialized runtime, cancels transport calls, waits for its goroutines, and closes connections. This ordering is important because the transport wait group assumes `Send` is no longer racing with `Close`.

## Invariants

- Terms and snapshot/log absolute indices never decrease.
- A node persists at most one vote per term and a leader requires a fixed-membership majority.
- Matching log index/term implies the preceding replicated prefix matches.
- A committed entry is never overwritten.
- Leaders use the current-term restriction when directly advancing commit.
- `lastIncludedIndex <= lastApplied <= commitIndex <= lastLogIndex`.
- Application is ordered and once per running/recovered state-machine history.
- Safety-critical state is persisted before a response or action that depends on it.

These are design invariants supported by code review and tests; they are not a formal proof. See `docs/correctness-audit.md` and `docs/test-report.md`.

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*.
- Official Go documentation and memory model at `go.dev`.
- Official gRPC Go and Protocol Buffers documentation.
