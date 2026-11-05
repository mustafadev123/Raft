## Why Raft?

Replicas must agree on one ordered command history despite crashes, delays, duplicates, and partitions. Simply broadcasting writes can produce conflicting orders and acknowledged data loss. Raft elects one leader per term through intersecting majorities and makes followers accept only log extensions consistent with their prefix.

## Leader election

Followers use randomized deadlines. A timeout increments and persists the term, records a self-vote, and requests votes. A voter grants at most once per term and only to a candidate whose last log is at least as fresh. A majority makes the candidate leader. Any higher term causes immediate persisted step-down. Election generations and cancellation prevent old replies from reviving an obsolete campaign.

## Log replication and commitment

The leader sends the previous absolute index/term plus a suffix. A follower rejects a mismatch and returns conflict information; the leader backs up `nextIndex` until prefixes match. It never overwrites committed entries. The leader acknowledges a client only after a majority matches an entry, the commit index is durably stored, and the local state machine applies it. Raft directly advances commit only through an entry from the current term; committing that entry commits its prefix.

Persisted does not mean committed. A former leader may have stored an entry on only itself before losing leadership. Recovery therefore persists `commitIndex` and replays only the snapshot plus entries known committed; an uncommitted suffix remains eligible for repair.

## Why a majority?

Any two majorities intersect. The overlap carries durable votes and committed log information into later elections. A minority cannot safely distinguish a partition from failures and therefore cannot elect or acknowledge new commands.

## Why does Get use the log?

Leader role alone does not prove the node is still connected to a majority; an isolated old leader can be stale. This implementation puts `Get` in the log and waits for majority commitment/application, creating a simple linearizable ordering point. It is slower and grows the log; production systems commonly use ReadIndex or carefully bounded leases.

## Retry deduplication

Every command includes a client ID and sequence. The state machine caches the latest result and returns it for a retry without repeating a mutation. The cache is snapshotted. The guarantee is bounded: one outstanding request, monotonically increasing sequences, unique client ID, and retry before eviction. A timeout remains an unknown outcome and must reuse the same identity.

## Snapshots and crash recovery

A snapshot represents state through `lastIncludedIndex/Term` and includes KV data plus deduplication records. The retained log keeps absolute indices after that boundary. Snapshot and suffix are saved atomically before the prefix is forgotten. A far-behind follower installs the snapshot, preserves a compatible suffix when possible, then resumes append replication. On restart, the snapshot is restored and only the persisted committed suffix is applied.

## Network partitions

A minority, including an isolated old leader, cannot commit. A connected majority can elect a replacement and progress. When the partition heals, higher terms step old leaders down and log consistency repair removes only conflicting uncommitted suffixes. Safety is expected throughout the partition; liveness is asserted only after a connected majority and fair delivery return.

## Actual bugs found during the rebuild

- Duplicate gRPC response waiter overwrite: response captures originally stored one channel per peer/request key. Concurrent identical RPCs replaced the first waiter, leaving its handler blocked until deadline. The map now stores FIFO waiter slices and cancellation removes only the registering handler's channel; a regression test delivers two identical-key responses to two waiters.
- Heartbeat consistency bypass: an earlier standalone heartbeat path could reset leadership state without the normal `AppendEntries` previous-index/term checks. Heartbeats now use empty `AppendEntries` and the same consistency path as replication.
- Synthetic last-log metadata: early election scaffolding allowed a configured last-log position to diverge from the actual replicated log. The implementation now derives freshness from the snapshot/log boundary and tests use real initial entries.

A test timing assumption was also corrected during development; it was a harness issue, not a consensus defect. No bug count or formal-correctness claim should be inferred from this list.
