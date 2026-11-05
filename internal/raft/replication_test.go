package raft_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/yourusername/raftkv/internal/kv"
	"github.com/yourusername/raftkv/internal/raft"
	"github.com/yourusername/raftkv/internal/storage"
)

type fixedTimeout time.Duration

func (f fixedTimeout) ElectionTimeout() time.Duration { return time.Duration(f) }

type queuedMessage struct {
	ctx     context.Context
	message raft.Message
}

type testCluster struct {
	now          time.Time
	nodes        map[raft.NodeID]*raft.Node
	machines     map[raft.NodeID]*kv.StateMachine
	stores       map[raft.NodeID]*storage.MemoryStore
	queue        []queuedMessage
	disconnected map[raft.NodeID]bool
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	c := &testCluster{
		now:          time.Unix(0, 0),
		nodes:        make(map[raft.NodeID]*raft.Node),
		machines:     make(map[raft.NodeID]*kv.StateMachine),
		stores:       make(map[raft.NodeID]*storage.MemoryStore),
		disconnected: make(map[raft.NodeID]bool),
	}
	members := []raft.NodeID{"a", "b", "c"}
	timeouts := map[raft.NodeID]time.Duration{
		"a": 10 * time.Millisecond,
		"b": 20 * time.Millisecond,
		"c": 30 * time.Millisecond,
	}
	for _, id := range members {
		machine := kv.New(32)
		store := &storage.MemoryStore{}
		node, err := raft.NewNode(raft.Config{
			ID: id, Members: members, HeartbeatInterval: 5 * time.Millisecond,
			Timeouts: fixedTimeout(timeouts[id]), Transport: c, StateMachine: machine, Store: store,
		}, c.now)
		if err != nil {
			t.Fatalf("NewNode(%q): %v", id, err)
		}
		c.nodes[id] = node
		c.machines[id] = machine
		c.stores[id] = store
	}
	return c
}

func (c *testCluster) Send(ctx context.Context, message raft.Message) error {
	c.queue = append(c.queue, queuedMessage{ctx: ctx, message: message})
	return nil
}

func (c *testCluster) advance(duration time.Duration) { c.now = c.now.Add(duration) }

func (c *testCluster) tick(ids ...raft.NodeID) {
	for _, id := range ids {
		c.nodes[id].Tick(c.now)
	}
}

func (c *testCluster) restart(t *testing.T, id raft.NodeID) {
	t.Helper()
	timeout := map[raft.NodeID]time.Duration{
		"a": 10 * time.Millisecond,
		"b": 20 * time.Millisecond,
		"c": 30 * time.Millisecond,
	}[id]
	machine := kv.New(32)
	node, err := raft.NewNode(raft.Config{
		ID: id, Members: []raft.NodeID{"a", "b", "c"}, HeartbeatInterval: 5 * time.Millisecond,
		Timeouts: fixedTimeout(timeout), Transport: c, StateMachine: machine, Store: c.stores[id],
	}, c.now)
	if err != nil {
		t.Fatalf("restart node %q: %v", id, err)
	}
	c.nodes[id] = node
	c.machines[id] = machine
}

func (c *testCluster) deliverAll() {
	for len(c.queue) > 0 {
		c.deliverNext()
	}
}

func (c *testCluster) deliverNext() {
	queued := c.queue[0]
	c.queue = c.queue[1:]
	message := queued.message
	if queued.ctx.Err() != nil || c.disconnected[message.From] || c.disconnected[message.To] {
		return
	}
	if err := c.nodes[message.To].Step(message, c.now); err != nil {
		panic(err)
	}
}

func (c *testCluster) electA(t *testing.T) {
	t.Helper()
	c.advance(10 * time.Millisecond)
	c.tick("a", "b", "c")
	c.deliverAll()
	if state := c.nodes["a"].State(); state.Role != raft.Leader || state.CommitIndex != 1 {
		t.Fatalf("initial leader state = %+v, want leader with committed no-op", state)
	}
}

func (c *testCluster) electBWithoutA(t *testing.T) {
	t.Helper()
	c.disconnected["a"] = true
	// The last accepted heartbeat set b's deadline to c.now+20ms.
	c.advance(20 * time.Millisecond)
	c.tick("b", "c")
	c.deliverAll()
	if state := c.nodes["b"].State(); state.Role != raft.Leader {
		t.Fatalf("replacement leader state = %+v", state)
	}
}

func propose(t *testing.T, node *raft.Node, command raft.Command) *raft.Proposal {
	t.Helper()
	proposal, err := node.Propose(command)
	if err != nil {
		t.Fatalf("Propose(%+v): %v", command, err)
	}
	return proposal
}

func requireResult(t *testing.T, proposal *raft.Proposal) raft.Result {
	t.Helper()
	result, ready := proposal.Result()
	if !ready {
		t.Fatal("proposal is not committed and applied")
	}
	if result.Error != "" {
		t.Fatalf("proposal result error = %q", result.Error)
	}
	return result
}

func TestSuccessfulWriteReplicatesAndApplies(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	proposal := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "color", Value: "blue",
	})
	c.deliverAll()
	requireResult(t, proposal)

	for _, id := range []raft.NodeID{"a", "b", "c"} {
		value, found := c.machines[id].Value("color")
		if !found || value != "blue" {
			t.Fatalf("node %s value = %q, %v; want blue, true", id, value, found)
		}
		state := c.nodes[id].State()
		if state.CommitIndex != 2 || state.LastApplied != 2 {
			t.Fatalf("node %s state = %+v, want index 2 committed/applied", id, state)
		}
	}
}

func TestIsolatedLeaderCannotAcknowledgeWrite(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["a"] = true
	proposal := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "x", Value: "isolated",
	})
	c.deliverAll()

	if result, ready := proposal.Result(); ready {
		t.Fatalf("isolated proposal completed with %+v", result)
	}
	if state := c.nodes["a"].State(); state.CommitIndex != 1 {
		t.Fatalf("isolated leader commit index = %d, want 1", state.CommitIndex)
	}
}

func TestConflictingUncommittedSuffixIsRepaired(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	c.disconnected["a"] = true
	_ = propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "old", Sequence: 1, Key: "version", Value: "old",
	})
	c.deliverAll()

	c.electBWithoutA(t)
	committed := propose(t, c.nodes["b"], raft.Command{
		Operation: raft.OperationPut, ClientID: "new", Sequence: 1, Key: "version", Value: "new",
	})
	c.deliverAll()
	requireResult(t, committed)

	c.disconnected["a"] = false
	c.advance(5 * time.Millisecond)
	c.tick("b")
	c.deliverAll()

	if !reflect.DeepEqual(c.nodes["a"].State().Log, c.nodes["b"].State().Log) {
		t.Fatalf("repaired log = %+v, leader log = %+v", c.nodes["a"].State().Log, c.nodes["b"].State().Log)
	}
	if value, _ := c.machines["a"].Value("version"); value != "new" {
		t.Fatalf("repaired follower value = %q, want new", value)
	}
}

func TestCommittedDataSurvivesLeaderChangeInMemory(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	write := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "durable", Value: "in-memory",
	})
	c.deliverAll()
	requireResult(t, write)

	c.electBWithoutA(t)
	for _, id := range []raft.NodeID{"b", "c"} {
		value, found := c.machines[id].Value("durable")
		if !found || value != "in-memory" {
			t.Fatalf("node %s lost committed data: %q, %v", id, value, found)
		}
	}
}

func TestRetryDoesNotApplyMutationTwice(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	command := raft.Command{
		Operation: raft.OperationPut, ClientID: "writer", Sequence: 1, Key: "once", Value: "value",
	}
	first := propose(t, c.nodes["a"], command)
	c.deliverAll()
	firstResult := requireResult(t, first)
	logLength := len(c.nodes["a"].State().Log)

	retry := propose(t, c.nodes["a"], command)
	retryResult := requireResult(t, retry)
	if retryResult != firstResult {
		t.Fatalf("retry result = %+v, original = %+v", retryResult, firstResult)
	}
	if got := c.machines["a"].MutationCount(); got != 1 {
		t.Fatalf("leader mutation count = %d, want 1", got)
	}
	if got := len(c.nodes["a"].State().Log); got != logLength {
		t.Fatalf("retry appended a log entry: length %d, want %d", got, logLength)
	}
}

func TestGetReflectsPrecedingCompletedWrite(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	put := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "client", Sequence: 1, Key: "answer", Value: "42",
	})
	c.deliverAll()
	requireResult(t, put)

	get := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationGet, ClientID: "client", Sequence: 2, Key: "answer",
	})
	c.deliverAll()
	result := requireResult(t, get)
	if !result.Found || result.Value != "42" {
		t.Fatalf("Get result = %+v, want 42", result)
	}
}

func TestCommittedEntriesApplyInOrderExactlyOnce(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	commands := []raft.Command{
		{Operation: raft.OperationPut, ClientID: "client", Sequence: 1, Key: "k", Value: "one"},
		{Operation: raft.OperationPut, ClientID: "client", Sequence: 2, Key: "k", Value: "two"},
		{Operation: raft.OperationDelete, ClientID: "client", Sequence: 3, Key: "k"},
	}
	for _, command := range commands {
		proposal := propose(t, c.nodes["a"], command)
		c.deliverAll()
		requireResult(t, proposal)
	}

	for _, id := range []raft.NodeID{"a", "b", "c"} {
		indices := c.machines[id].AppliedIndices()
		want := []uint64{1, 2, 3, 4}
		if !reflect.DeepEqual(indices, want) {
			t.Fatalf("node %s applied indices = %v, want %v", id, indices, want)
		}
		if c.nodes[id].State().LastApplied != 4 {
			t.Fatalf("node %s lastApplied = %d, want 4", id, c.nodes[id].State().LastApplied)
		}
	}
}

func TestDuplicateAndDelayedAppendTrafficIsSafe(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	write := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "client", Sequence: 1, Key: "k", Value: "v",
	})
	c.deliverAll()
	requireResult(t, write)

	leaderState := c.nodes["a"].State()
	followerBefore := c.nodes["b"].State()
	duplicate := raft.Message{
		From: "a", To: "b", Type: raft.MessageAppendEntries,
		AppendEntries: raft.AppendEntriesRequest{
			Term: leaderState.Term, LeaderID: "a", PrevLogIndex: 1,
			PrevLogTerm: leaderState.Log[0].Term, Entries: leaderState.Log[1:],
			LeaderCommit: leaderState.CommitIndex, RequestID: 999,
		},
	}
	if err := c.nodes["b"].Step(duplicate, c.now); err != nil {
		t.Fatal(err)
	}
	if got := c.machines["b"].MutationCount(); got != 1 {
		t.Fatalf("duplicate append mutation count = %d, want 1", got)
	}
	if got := c.nodes["b"].State().LastApplied; got != followerBefore.LastApplied {
		t.Fatalf("duplicate append advanced lastApplied from %d to %d", followerBefore.LastApplied, got)
	}

	nextBefore := leaderState.NextIndex["b"]
	if err := c.nodes["a"].Step(raft.Message{
		From: "b", To: "a", Type: raft.MessageAppendEntriesResponse,
		AppendEntriesResponse: raft.AppendEntriesResponse{
			Term: leaderState.Term, Success: false, ConflictIndex: 1, RequestID: 0,
		},
	}, c.now); err != nil {
		t.Fatal(err)
	}
	if got := c.nodes["a"].State().NextIndex["b"]; got != nextBefore {
		t.Fatalf("delayed failure regressed nextIndex from %d to %d", nextBefore, got)
	}
}

func TestCommittedEntryIsNeverOverwritten(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	write := propose(t, c.nodes["a"], raft.Command{
		Operation: raft.OperationPut, ClientID: "client", Sequence: 1, Key: "safe", Value: "committed",
	})
	c.deliverAll()
	requireResult(t, write)
	before := c.nodes["b"].State()

	conflict := raft.Message{
		From: "a", To: "b", Type: raft.MessageAppendEntries,
		AppendEntries: raft.AppendEntriesRequest{
			Term: before.Term, LeaderID: "a", PrevLogIndex: 1, PrevLogTerm: before.Log[0].Term,
			Entries: []raft.Entry{{
				Index: 2, Term: before.Term + 10,
				Command: raft.Command{Operation: raft.OperationDelete, ClientID: "other", Sequence: 1, Key: "safe"},
			}},
			LeaderCommit: before.CommitIndex, RequestID: 1000,
		},
	}
	if err := c.nodes["b"].Step(conflict, c.now); err != nil {
		t.Fatal(err)
	}
	after := c.nodes["b"].State()
	if !reflect.DeepEqual(after.Log, before.Log) {
		t.Fatalf("committed log changed: before=%+v after=%+v", before.Log, after.Log)
	}
	if value, found := c.machines["b"].Value("safe"); !found || value != "committed" {
		t.Fatalf("committed value changed: %q, %v", value, found)
	}
}
