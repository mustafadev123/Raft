# Design

This document describes the implemented in-memory Raft and key-value stages
and the intended architecture. Sections explicitly marked planned are not
completed.

## Architecture

The Raft core will be a deterministic state machine. It will accept explicit
events (RPCs, client proposals, and timer ticks) and emit actions (persist,
send, apply, and reset-timer). Keeping clocks, disks, and networks outside the
core makes election and partition tests reproducible.

`internal/storage` owns durable Raft metadata and log entries; snapshots remain
planned.
Durability must complete before dependent messages or acknowledgements are
emitted. `internal/transport` adapts generated gRPC services to Raft events.
`internal/kv` deterministically applies committed commands.
`internal/server` wires these parts together, routes client requests to the
leader, and coordinate lifecycle and recovery.

Cluster configuration is static and supplied at startup. Only three-node and
five-node configurations are in scope; membership changes are not.

## Leader election (implemented)

Each node is a caller-serialized state machine. Its owner advances a supplied
logical time with `Tick` and delivers one transport-neutral message at a time
with `Step`. The core starts no goroutines, reads no wall clock, opens no
network connection, and accesses no disk.

A follower whose randomized election deadline expires increments its term,
becomes a candidate, votes for itself, and sends vote requests to its fixed
peers. A candidate becomes leader after votes from a majority, including its
own vote. Becoming leader sends an immediate heartbeat; later ticks send
periodic heartbeats. A valid heartbeat returns candidates and leaders to the
follower role and resets the election deadline.

Vote recipients compare last-log term first and then last-log index. They
grant at most one candidate per term and reset their timer only for a granted
vote. Any higher observed term clears the recorded vote and forces follower
state. Election requests carry a cancellation context and generation number:
queued requests can be canceled, while a response that was already produced
is ignored unless it belongs to the candidate's current term and generation.

The in-memory test transport queues messages and lets tests explicitly delay,
drop, partition, or deliver them. Logical time and fixed timeout sources make
the scenarios deterministic without sleep-based synchronization.

## Log replication and commitment (implemented in memory)

Log entries contain a one-based index, the leader term that created them, and
a command. A newly elected leader appends a no-op entry in its current term.
For each follower it tracks `nextIndex` and `matchIndex`, sends the suffix
starting at `nextIndex`, and uses conflict term/index responses to back up
efficiently after a mismatch.

Followers accept AppendEntries only after its preceding index and term match.
They truncate only conflicting uncommitted suffixes, never an entry at or
below `commitIndex`. Duplicate requests are idempotent. Leaders attach request
IDs; an old failed response cannot move `nextIndex` backward after a newer RPC
has made progress, while duplicate successful responses only take a maximum.

A leader advances `commitIndex` when a majority has matched an index whose
entry belongs to the leader's current term. Committing that entry also commits
the preceding prefix. Each node increments `lastApplied` and applies entries
in index order, so an entry is applied once per running state-machine
instance. No disk-backed restart guarantee exists yet.

## Key-value commands and responses (implemented in memory)

`Put`, `Delete`, and `Get` all enter the replicated log. In particular, `Get`
is not served from a follower or from a leader-role check alone: its result is
returned only after the read command is committed and applied. This is a
deliberately simple linearizable-read approach with the cost of a log entry
for every read.

`Propose` returns a completion object. It becomes ready only after the local
leader commits and applies the command. Losing leadership completes still
pending proposals with a not-leader error, allowing a client to retry safely
under the model below.

### Bounded retry deduplication

Every client command carries a unique client ID and a sequence number starting
at one and increasing by exactly one. A client may have only one outstanding
request. The command identity is replicated in the log, and every KV state
machine deterministically caches that client's latest sequence and result.
A retry of that latest request returns the original result without applying
the mutation again.

The cache is bounded by a configured number of clients and deterministically
evicts the least recently applied client record. Deduplication is guaranteed
only while the relevant record remains cached. After eviction, an old retry
can no longer be recognized and may be treated as a new client history. This
is not universal exactly-once delivery; clients must respect the single-
outstanding rule and retry within the configured retention window.

## Raft invariants

- Terms increase monotonically and each node casts at most one vote per term.
- A node becomes leader only after receiving votes from a majority of the
  configured cluster.
- Log indices and terms identify entries; committed entries are never
  overwritten.
- A leader advances `commitIndex` using a majority match only for an entry
  from its current term.
- `lastApplied` never exceeds `commitIndex`, and application is ordered and
  exactly once within a process/recovery history.
- Persistent term, vote, and log changes reach stable storage before an RPC
  response that depends on them.
- The state machine consumes only committed entries and has no nondeterministic
  behavior.
- Snapshot installation preserves the term/index boundary and never rolls the
  state machine or commit index backward.

## gRPC transport and process runtime (implemented)

The protobuf API defines RequestVote, AppendEntries, and client `Put`, `Get`,
and `Delete` unary RPCs. The in-memory transport remains unchanged for
deterministic tests.

One runtime goroutine owns each Raft node and serializes ticks, inbound peer
messages, state inspection, and proposals. gRPC handlers submit events and
wait for results; they never mutate consensus state directly. The peer
transport returns immediately from `Send`, performs bounded RPCs in separate
goroutines, reuses one `ClientConn` per peer, and posts responses back to the
runtime. Network failures are logged and drive ordinary Raft timeout behavior
rather than crashing the runtime or unrelated goroutines.

Peer handlers register a short-lived response capture before submitting an
RPC to the core. The response emitted by the core after its persistence
barrier becomes the unary gRPC response, preserving the deterministic core's
ordering.

Client RPCs return structured not-leader responses with the latest known
leader address. The CLI reuses connections, follows hints, and retries with an
unchanged client ID and sequence. Per-attempt and overall deadlines are
bounded. Deadline expiration is an unknown outcome because an entry may have
committed while its response was lost; safe retry uses the same identity.

The node executable accepts node ID, loopback listen address, static peer
address map, and data directory settings. JSON logs identify node, term, role,
leader, commit index, and significant runtime/network events. Shutdown first
uses gRPC graceful stop and then forces stop after a bounded wait.

The demonstration uses plaintext insecure transport credentials and has no
TLS, authentication, or authorization. It is intended only for loopback use.

## Persistence and recovery (implemented, without snapshots)

The Raft core depends on a small `StableStore` interface and does not perform
filesystem operations itself. Durable state contains `currentTerm`,
`votedFor`, the complete log, and `commitIndex`. Persisting `commitIndex` is
necessary to distinguish a known committed prefix from a persisted but
uncommitted suffix during recovery.

The filesystem implementation stores one versioned `raft-state.json` envelope.
Its JSON payload is protected by a SHA-256 checksum. Loading rejects unknown
versions, invalid JSON, checksum failures, non-contiguous indices, zero or
future entry terms, and a commit index beyond the log. Missing state denotes a
new node; corrupt or incomplete live state is a startup error and is never
silently discarded.

Each save encodes the complete state into a uniquely named temporary file in
the same directory, writes it, syncs and closes it, then atomically replaces
the live file. Unix-like systems additionally sync the containing directory.
Windows uses `MoveFileExW` with replace-existing and write-through flags.
Crash hooks cover before the temporary write, after the write, after its sync,
and after atomic replacement. The in-memory test store can fail the next save.

The terms have distinct meanings:

- A write copies bytes toward the operating-system page cache.
- Flushing a language-level buffer only hands buffered bytes to the OS; this
  implementation writes directly to an OS file and has no extra user buffer.
- `Sync` asks the OS to push file contents and relevant metadata toward stable
  storage.
- Atomic replacement selects either the old complete file or the new complete
  file as the live generation; it does not itself guarantee the new bytes are
  durable, which is why the temporary file is synced first.

Safety-critical changes are saved before dependent messages: new terms and
votes before vote traffic, appended/truncated logs before AppendEntries
success or replication, and commit-index advancement before state-machine
application or client completion. A save failure faults the node, cancels its
active election/leadership work, emits no dependent response, and is returned
to the caller.

Restart loads the term, vote, log, and commit index, then replays only entries
through `commitIndex` into a fresh KV state machine. Uncommitted suffixes remain
in the log but are not applied. Replaying committed request identities rebuilds
the bounded deduplication records and preserves retry results subject to the
same retention assumptions described above.

### Filesystem and hardware assumptions

The design assumes one writer per node directory and a local filesystem that
honors same-directory atomic replacement, successful file sync requests, and
the platform replacement primitive. It does not claim protection against
drive firmware or controllers that falsely report flush completion, volatile
device caches without power protection, disabled barriers, filesystem bugs,
post-sync media corruption, concurrent writers, or every possible power-loss
sequence. The checksum detects damage but cannot repair it.

### Performance tradeoffs

Every safety-critical update rewrites and syncs the full log, making a save
O(total log size) in bytes and placing synchronous storage latency on election
and write paths. This is simple to inspect and test but unsuitable for high
throughput. A production-oriented design would use a checksummed write-ahead
log, batching/group commit, and snapshots; those are outside this stage.

## Testing strategy (planned)

Core tests will use a logical clock, seeded scheduling, and an in-memory
network that can deterministically drop, delay, duplicate, and reorder
messages. Storage tests will inject write and restart boundaries. Integration
tests will launch static three-node and five-node clusters, record the seed and
event trace, and verify election safety, log matching, state convergence, and
recovery. The local demo will use fixed configuration files and documented
commands.

## References

Technical decisions should be checked against primary sources:

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Extended Version)*.
- The official Go documentation and memory model at `go.dev`.
- The official gRPC Go and Protocol Buffers documentation.

