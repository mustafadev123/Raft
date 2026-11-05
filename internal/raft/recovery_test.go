package raft_test

import (
	"errors"
	"testing"
	"time"

	"github.com/yourusername/raftkv/internal/raft"
)

func TestRestartedNodeDoesNotVoteTwiceInSameTerm(t *testing.T) {
	c := newTestCluster(t)
	first := raft.Message{
		From: "a", To: "b", Type: raft.MessageRequestVote,
		RequestVote: raft.RequestVoteRequest{Term: 5, CandidateID: "a", Election: 1},
	}
	if err := c.nodes["b"].Step(first, c.now); err != nil {
		t.Fatal(err)
	}
	if len(c.queue) != 1 || !c.queue[0].message.RequestVoteResponse.VoteGranted {
		t.Fatalf("first vote response = %+v, want granted", c.queue)
	}
	c.queue = nil
	c.restart(t, "b")

	second := raft.Message{
		From: "c", To: "b", Type: raft.MessageRequestVote,
		RequestVote: raft.RequestVoteRequest{Term: 5, CandidateID: "c", Election: 1},
	}
	if err := c.nodes["b"].Step(second, c.now); err != nil {
		t.Fatal(err)
	}
	if len(c.queue) != 1 || c.queue[0].message.RequestVoteResponse.VoteGranted {
		t.Fatalf("second vote response = %+v, want denied", c.queue)
	}
	state := c.nodes["b"].State()
	if state.Term != 5 || state.VotedFor != "a" {
		t.Fatalf("recovered vote state = %+v", state)
	}
}

func TestFollowerRejoinsAfterRestartAndCatchesUp(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["c"] = true
	write := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "catchup", Value: "yes",
	})
	c.deliverAll()
	requireResult(t, write)
	c.restart(t, "c")
	if _, found := c.machines["c"].Value("catchup"); found {
		t.Fatal("unreplicated value appeared during follower recovery")
	}

	c.disconnected["c"] = false
	c.advance(5 * time.Millisecond)
	c.tick("a")
	c.deliverAll()
	value, found := c.machines["c"].Value("catchup")
	if !found || value != "yes" {
		t.Fatalf("caught-up value = %q, %v", value, found)
	}
}

func TestFullClusterRestartRecoversAcknowledgedWriteAndDeduplication(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	command := raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "restart", Value: "preserved",
	}
	write := propose(t, c.nodes["a"], command)
	for {
		if _, ready := write.Result(); ready {
			break
		}
		if len(c.queue) == 0 {
			t.Fatal("message queue drained before write acknowledgment")
		}
		c.deliverNext()
	}
	requireResult(t, write)
	// Crash immediately: discard commit notifications still queued in memory.
	c.queue = nil

	for _, id := range []raft.NodeID{"a", "b", "c"} {
		c.restart(t, id)
	}
	c.advance(10 * time.Millisecond)
	c.tick("a", "b", "c")
	c.deliverAll()
	if c.nodes["a"].State().Role != raft.Leader {
		t.Fatal("a did not become leader after full restart")
	}
	for _, id := range []raft.NodeID{"a", "b", "c"} {
		value, found := c.machines[id].Value("restart")
		if !found || value != "preserved" {
			t.Fatalf("node %s recovered value = %q, %v", id, value, found)
		}
	}
	before := len(c.nodes["a"].State().Log)
	retry := propose(t, c.nodes["a"], command)
	requireResult(t, retry)
	if after := len(c.nodes["a"].State().Log); after != before {
		t.Fatalf("recovered deduplication appended retry: before=%d after=%d", before, after)
	}
}

func TestUncommittedEntryIsNotAppliedOnRestart(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["a"] = true
	_ = propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "unsafe", Value: "no",
	})
	c.deliverAll()
	c.restart(t, "a")

	state := c.nodes["a"].State()
	if len(state.Log) != 2 || state.CommitIndex != 1 || state.LastApplied != 1 {
		t.Fatalf("recovered state = %+v, want persisted uncommitted suffix but applied prefix only", state)
	}
	if _, found := c.machines["a"].Value("unsafe"); found {
		t.Fatal("uncommitted value was applied during recovery")
	}
}

func TestStorageFailurePreventsUnsafeAcknowledgment(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.stores["a"].FailNextSave(errors.New("injected disk failure"))
	proposal, err := c.nodes["a"].Propose(raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "unsafe", Value: "no",
	})
	if !errors.Is(err, raft.ErrStorageFault) {
		t.Fatalf("Propose error = %v, want storage fault", err)
	}
	if proposal != nil {
		t.Fatal("failed durable append returned a proposal")
	}
	if len(c.queue) != 0 {
		t.Fatalf("failed durable append emitted %d messages", len(c.queue))
	}
	c.restart(t, "a")
	if _, found := c.machines["a"].Value("unsafe"); found {
		t.Fatal("failed durable append survived as committed data")
	}
}

func TestCommitStorageFailureReturnsErrorInsteadOfSuccess(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	proposal := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "commit", Value: "unsafe",
	})
	// Persisting the local append succeeded. Fail the later commit-index save.
	c.stores["a"].FailNextSave(errors.New("injected commit sync failure"))
	c.deliverNext() // append to b
	c.deliverNext() // append to c
	queued := c.queue[0]
	c.queue = c.queue[1:]
	if queued.message.Type != raft.MessageAppendEntriesResponse || queued.message.To != "a" {
		t.Fatalf("next message = %+v, want follower acknowledgment", queued.message)
	}
	err := c.nodes["a"].Step(queued.message, c.now)
	if !errors.Is(err, raft.ErrStorageFault) {
		t.Fatalf("commit response error = %v, want storage fault", err)
	}
	result, ready := proposal.Result()
	if !ready || result.Error == "" {
		t.Fatalf("proposal result = %+v, ready %v; want explicit failure", result, ready)
	}
	c.queue = nil
	c.restart(t, "a")
	if _, found := c.machines["a"].Value("commit"); found {
		t.Fatal("entry with failed commit persistence was applied after restart")
	}
}

func TestVoteStorageFailureSendsNoResponse(t *testing.T) {
	c := newTestCluster(t)
	c.stores["b"].FailNextSave(errors.New("injected vote failure"))
	err := c.nodes["b"].Step(raft.Message{
		From: "a", To: "b", Type: raft.MessageRequestVote,
		RequestVote: raft.RequestVoteRequest{Term: 1, CandidateID: "a", Election: 1},
	}, c.now)
	if !errors.Is(err, raft.ErrStorageFault) {
		t.Fatalf("Step error = %v, want storage fault", err)
	}
	if len(c.queue) != 0 {
		t.Fatalf("vote storage failure emitted %d responses", len(c.queue))
	}
}
