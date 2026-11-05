# Correctness audit

This audit traces claims to the implementation and tests. It separates executed tests from code reasoning and assumptions. It is not a formal proof.

## Election safety and higher terms

Reasoning: `startElection` persists the incremented term and self-vote before sending requests. `handleRequestVote` grants only to a member whose candidate ID matches the sender, only once per term, and only when its last log is at least as fresh. `adoptHigherTerm` cancels election and leadership contexts, clears the vote, becomes a follower, and persists before returning. Vote replies are accepted only for the current candidate term and election generation. A leader requires `len(members)/2+1`, including itself; intersecting fixed majorities and durable one-vote-per-term enforce election safety under the stated storage assumptions.

Tested: single/three-node election, majority replacement of an isolated leader, minority failure, stale-log rejection, higher-term step-down, stale election replies, timer-reset rules, persisted vote across restart, and five-node seeded partitions/crashes. Tests do not enumerate every schedule.

## Log matching, leader completeness, and commit

Reasoning: `AppendEntries` validates the preceding absolute index and term before accepting entries. A term conflict truncates only above `commitIndex`; an attempted committed conflict is rejected. Conflict term/index feedback backs up `nextIndex`. Successful delayed responses update progress only by maximum; failed responses must match the latest request ID. Leaders append a persisted current-term no-op and advance commit only when a majority has matched an index whose term equals the current term. Vote freshness plus majority intersection is the Raft leader-completeness argument.

Tested: replication/application, isolated-leader non-acknowledgment, conflicting suffix repair, leader change, delayed/duplicate traffic, committed-entry protection, current-term advancement in scenario tests, and randomized leader-completeness/committed-conflict assertions. Leader completeness remains partly a protocol argument rather than exhaustive state exploration.

## Persistence before dependent responses

Reasoning: elections persist term/self-vote before vote traffic; granted votes persist before their response; leaders persist the no-op and proposals before replication; followers persist appended/truncated log and new commit index before success/application; leaders persist commit before application/client completion; snapshot installation persists the snapshot and suffix before restore and success. `savePersistentState` faults the node and cancels pending work after an error.

Tested: restart cannot vote twice, full-cluster acknowledged-write recovery, uncommitted entries not applied, proposal/commit/vote storage failures preventing unsafe responses, corrupt state rejection, and injected file crash points. The tests model API-visible failures and complete-file generations, not arbitrary hardware behavior.

Assumptions: a single writer, truthful `Sync`, same-directory atomic replacement, and platform/filesystem semantics documented in `design.md`. No claim covers every power-loss or faulty-controller case.

## Ordered application and deduplication

Reasoning: `applyCommitted` increments `lastApplied` by one, fetches that exact absolute entry, applies it, then completes its matching proposal. KV application stores the latest client sequence and original result; snapshots include those records. Recovery restores a snapshot and applies only the committed suffix. The bounded model assumes one outstanding, monotonic request per client and retention before eviction.

Tested: ordered exactly-once application per running state machine, mutation retry, completed-write/Get ordering, full restart deduplication, and snapshot restore deduplication. This is not universal exactly-once delivery.

## Snapshot boundary, suffix, and recovery

Reasoning: snapshots are taken at `lastApplied`, and absolute helper functions translate the retained suffix relative to `lastIncludedIndex`. Compaction persists the snapshot plus suffix before changing the in-memory boundary. Installation validates data/size, ignores obsolete boundaries, preserves a suffix only when the local boundary term is compatible, refuses an incompatible snapshot below an already committed boundary, persists before restore, and reapplies a committed suffix in order.

Tested: restart after compaction, lagging follower installation, writes during transfer, stale snapshot handling, interrupted persistence, repeated compaction/off-by-one behavior, compatible suffix preservation, deduplication restore, and real-process snapshot catch-up.

Assumptions/limits: snapshot creation is serialized with core events, transfer is unary and bounded, and restore failure after a successful persistent replacement faults the node and requires restart. Streaming/chunk-resume behavior is absent.

## Stale RPCs and snapshot transfers

Reasoning: vote responses carry term and election generation; append failures carry a monotonically generated request ID; success can only increase follower progress. Higher-term replies force step-down. Leadership/election contexts cancel in-flight outbound calls. Snapshot replies require current leader/current term and only increase progress. Obsolete snapshot requests cannot move the follower backward.

Tested: delayed election response, duplicate/delayed append traffic, stale snapshots, concurrent writes during snapshot transfer, and randomized drop/delay/duplication. There is no exhaustive adversarial network proof.

## gRPC waiters, cancellation, and shutdown

Reasoning: inbound handlers register response captures before submitting serialized core events and defer waiter removal. Captures are FIFO slices rather than a single map value, so identical concurrent RPC keys retain all waiters. Buffered channels prevent core response delivery from blocking if a handler cancels at the same instant. Outbound RPCs have deadlines and inherit election/leadership cancellation. Runtime submission watches request context and runtime termination.

Tested: `TestDuplicateRPCResponseWaitersAreNotOverwritten` is the targeted regression test for the discovered overwrite/leak defect. The real-process integration covers graceful shutdown and restart.

Unresolved limitation: `GRPC.Close` waits on a `sync.WaitGroup`; correctness relies on server/runtime shutdown ordering preventing new `Send` calls from racing with `Close`. There is no dedicated concurrent close/send stress test. Local race execution is blocked, although the configured Linux race suite passed on reviewed commit `a7b6ee6e46537a35cec9abcebe06a011cbd9bec8`.

## Overall conclusion

The reviewed code follows the central Raft safety rules and the executed deterministic scenarios cover the requested failure modes. Remaining uncertainty is concentrated in unenumerated schedules, unavailable local race evidence, bounded/incomplete linearizability checking, filesystem/hardware assumptions, and shutdown ordering assumptions. These limitations prevent describing the result as formally verified or production-ready.
