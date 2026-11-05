# Design

This document describes the intended architecture. Except for the package and
executable skeleton, these capabilities are plans rather than completed work.

## Architecture

The Raft core will be a deterministic state machine. It will accept explicit
events (RPCs, client proposals, and timer ticks) and emit actions (persist,
send, apply, and reset-timer). Keeping clocks, disks, and networks outside the
core makes election and partition tests reproducible.

`internal/storage` will own durable Raft metadata, log entries, and snapshots.
Durability must complete before dependent messages or acknowledgements are
emitted. `internal/transport` will adapt generated gRPC services to Raft
events. `internal/kv` will deterministically apply committed commands.
`internal/server` will wire these parts together, route client requests to the
leader, and coordinate lifecycle and recovery.

Cluster configuration is static and supplied at startup. Only three-node and
five-node configurations are in scope; membership changes are not.

## Planned Raft invariants

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

## Client semantics (planned)

Writes and deletes will enter the replicated log through the leader and return
only after commitment and application. Reads will use a Raft-safe leader-read
mechanism selected and documented during the KV stage. Requests sent to a
follower will receive a structured not-leader response with a leader hint when
known. Retry/deduplication semantics will be decided before the client API is
implemented.

## Persistence and recovery (planned)

The storage interface will keep current term, voted-for, log entries, and
snapshot metadata. Startup will load the latest valid snapshot, replay the
remaining committed log, and resume without violating persisted voting and
log promises. The on-disk format and atomic update strategy will be chosen in
the storage stage and tested with injected failures.

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

