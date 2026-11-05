package raft_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yourusername/raftkv/internal/raft"
)

func committedPut(t *testing.T, c *testCluster, client string, sequence uint64, key, value string) {
	t.Helper()
	proposal := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: client, Sequence: sequence, Key: key, Value: value,
	})
	c.deliverAll()
	requireResult(t, proposal)
}

func TestRestartAfterLocalCompactionPreservesStateAndDeduplication(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	committedPut(t, c, "writer", 1, "a", "one")
	committedPut(t, c, "writer", 2, "b", "two")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	before := c.nodes["a"].State()
	if before.LastIncludedIndex != 3 || len(before.Log) != 0 {
		t.Fatalf("compacted state = %+v, want boundary 3 and empty suffix", before)
	}

	c.restart(t, "a")
	after := c.nodes["a"].State()
	if after.LastIncludedIndex != 3 || after.CommitIndex != 3 || after.LastApplied != 3 {
		t.Fatalf("recovered compacted state = %+v", after)
	}
	if value, found := c.machines["a"].Value("b"); !found || value != "two" {
		t.Fatalf("recovered value = %q, %v", value, found)
	}
	if result, ok := c.machines["a"].CachedResult("writer", 2); !ok || result.Error != "" {
		t.Fatalf("recovered dedup result = %+v, %v", result, ok)
	}
}

func TestLaggingFollowerInstallsSnapshotAndResumesReplication(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["c"] = true
	committedPut(t, c, "writer", 1, "k1", "v1")
	committedPut(t, c, "writer", 2, "k2", "v2")
	committedPut(t, c, "writer", 3, "k3", "v3")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	boundary := c.nodes["a"].State().LastIncludedIndex

	c.disconnected["c"] = false
	c.advance(5 * time.Millisecond)
	c.tick("a")
	c.deliverAll()
	if got := c.nodes["c"].State().LastIncludedIndex; got != boundary {
		t.Fatalf("follower snapshot boundary = %d, want %d", got, boundary)
	}
	if value, found := c.machines["c"].Value("k3"); !found || value != "v3" {
		t.Fatalf("snapshot value = %q, %v", value, found)
	}

	committedPut(t, c, "writer", 4, "after", "snapshot")
	if value, found := c.machines["c"].Value("after"); !found || value != "snapshot" {
		t.Fatalf("post-snapshot replication value = %q, %v", value, found)
	}
}

func TestWriteDuringSnapshotTransferIsReplicatedAfterBoundary(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["c"] = true
	committedPut(t, c, "writer", 1, "before", "snapshot")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	c.disconnected["c"] = false
	c.advance(5 * time.Millisecond)
	c.tick("a") // queues InstallSnapshot for c

	proposal := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 2, Key: "during", Value: "transfer",
	})
	c.deliverAll()
	requireResult(t, proposal)
	if value, found := c.machines["c"].Value("during"); !found || value != "transfer" {
		t.Fatalf("write during transfer = %q, %v", value, found)
	}
}

func TestStaleSnapshotDoesNotMoveFollowerBackward(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	committedPut(t, c, "writer", 1, "key", "value")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	c.disconnected["c"] = true
	committedPut(t, c, "writer", 2, "newer", "value")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	c.disconnected["c"] = false
	c.advance(5 * time.Millisecond)
	c.tick("a")
	c.deliverAll()
	before := c.nodes["c"].State()
	stored, _, err := c.stores["a"].Load()
	if err != nil {
		t.Fatal(err)
	}
	stale := raft.Message{
		From: "a", To: "c", Type: raft.MessageInstallSnapshot,
		InstallSnapshot: raft.InstallSnapshotRequest{
			Term: before.Term, LeaderID: "a", LastIncludedIndex: before.LastIncludedIndex - 1,
			LastIncludedTerm: before.LastIncludedTerm, Data: stored.Snapshot.Data, RequestID: 9999,
		},
	}
	if err := c.nodes["c"].Step(stale, c.now); err != nil {
		t.Fatal(err)
	}
	after := c.nodes["c"].State()
	if after.LastIncludedIndex != before.LastIncludedIndex || after.CommitIndex != before.CommitIndex || !reflect.DeepEqual(after.Log, before.Log) {
		t.Fatalf("stale snapshot moved state: before=%+v after=%+v", before, after)
	}
}

func TestInterruptedCompactionKeepsRecoverableFullLog(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	committedPut(t, c, "writer", 1, "safe", "value")
	c.stores["a"].FailNextSave(errors.New("snapshot persistence interrupted"))
	if err := c.nodes["a"].Compact(); !errors.Is(err, raft.ErrStorageFault) {
		t.Fatalf("Compact error = %v, want storage fault", err)
	}
	if got := c.nodes["a"].State(); got.LastIncludedIndex != 0 || len(got.Log) != 2 {
		t.Fatalf("failed compaction removed log data: %+v", got)
	}
	c.restart(t, "a")
	if value, found := c.machines["a"].Value("safe"); !found || value != "value" {
		t.Fatalf("recovered value after interrupted compaction = %q, %v", value, found)
	}
}

func TestRepeatedCompactionUsesAbsoluteIndices(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	for sequence := uint64(1); sequence <= 5; sequence++ {
		committedPut(t, c, "writer", sequence, "key", string(rune('0'+sequence)))
		if err := c.nodes["a"].Compact(); err != nil {
			t.Fatal(err)
		}
		state := c.nodes["a"].State()
		wantBoundary := sequence + 1 // initial leader no-op plus writes
		if state.LastIncludedIndex != wantBoundary || state.CommitIndex != wantBoundary || state.LastApplied != wantBoundary || len(state.Log) != 0 {
			t.Fatalf("sequence %d compacted state = %+v, want boundary %d", sequence, state, wantBoundary)
		}
	}
}

func TestInstallSnapshotPreservesCompatibleSuffix(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	committedPut(t, c, "writer", 1, "base", "value")
	committedPut(t, c, "writer", 2, "boundary", "value")
	if err := c.nodes["a"].Compact(); err != nil {
		t.Fatal(err)
	}
	stored, _, err := c.stores["a"].Load()
	if err != nil {
		t.Fatal(err)
	}
	leader := c.nodes["a"].State()
	boundary := stored.Snapshot.LastIncludedIndex
	boundaryTerm := stored.Snapshot.LastIncludedTerm
	suffix := raft.Entry{
		Index: boundary + 1, Term: leader.Term,
		Command: raft.Command{Operation: raft.OperationPut, ClientID: "suffix", Sequence: 1, Key: "suffix", Value: "kept"},
	}
	if err := c.nodes["c"].Step(raft.Message{
		From: "a", To: "c", Type: raft.MessageAppendEntries,
		AppendEntries: raft.AppendEntriesRequest{
			Term: leader.Term, LeaderID: "a", PrevLogIndex: boundary, PrevLogTerm: boundaryTerm,
			Entries: []raft.Entry{suffix}, LeaderCommit: boundary, RequestID: 700,
		},
	}, c.now); err != nil {
		t.Fatal(err)
	}
	if err := c.nodes["c"].Step(raft.Message{
		From: "a", To: "c", Type: raft.MessageInstallSnapshot,
		InstallSnapshot: raft.InstallSnapshotRequest{
			Term: leader.Term, LeaderID: "a", LastIncludedIndex: boundary,
			LastIncludedTerm: boundaryTerm, Data: stored.Snapshot.Data, RequestID: 701,
		},
	}, c.now); err != nil {
		t.Fatal(err)
	}
	state := c.nodes["c"].State()
	if len(state.Log) != 1 || state.Log[0] != suffix {
		t.Fatalf("compatible suffix was not preserved: %+v", state)
	}
}
