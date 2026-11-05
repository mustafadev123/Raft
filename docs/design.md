# Design

This document describes both the implemented leader-election stage and the
intended architecture. Sections explicitly marked planned are not completed.

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

Current terms and votes exist only in memory. Disk durability and crash
recovery are not implemented in the election stage.

## Replication and client operations (planned)

Election heartbeats currently model only the empty, leadership-establishing
part of AppendEntries. Replicated log entries, commitment, and client-facing
`Put`, `Get`, and `Delete` operations are not implemented.

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

