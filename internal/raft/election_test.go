package raft

import (
	"context"
	"testing"
	"time"
)

type fixedTimeout time.Duration

func (f fixedTimeout) ElectionTimeout() time.Duration { return time.Duration(f) }

type queuedMessage struct {
	ctx     context.Context
	message Message
}

// memoryNetwork is a deterministic transport. Tests explicitly choose when a
// queued message is delivered, and can partition or drop traffic without real
// networking or sleeps.
type memoryNetwork struct {
	now          time.Time
	nodes        map[NodeID]*Node
	queue        []queuedMessage
	disconnected map[NodeID]bool
}

func newMemoryNetwork() *memoryNetwork {
	return &memoryNetwork{
		now:          time.Unix(0, 0),
		nodes:        make(map[NodeID]*Node),
		disconnected: make(map[NodeID]bool),
	}
}

func (n *memoryNetwork) Send(ctx context.Context, message Message) error {
	n.queue = append(n.queue, queuedMessage{ctx: ctx, message: message})
	return nil
}

func (n *memoryNetwork) addNode(t *testing.T, id NodeID, members []NodeID, timeout time.Duration, lastLog LogPosition) *Node {
	t.Helper()
	node, err := NewNode(Config{
		ID:                id,
		Members:           members,
		LastLog:           lastLog,
		HeartbeatInterval: 5 * time.Millisecond,
		Timeouts:          fixedTimeout(timeout),
		Transport:         n,
	}, n.now)
	if err != nil {
		t.Fatalf("NewNode(%q): %v", id, err)
	}
	n.nodes[id] = node
	return node
}

func (n *memoryNetwork) advance(d time.Duration) { n.now = n.now.Add(d) }

func (n *memoryNetwork) tick(ids ...NodeID) {
	for _, id := range ids {
		n.nodes[id].Tick(n.now)
	}
}

func (n *memoryNetwork) deliverNext(match func(Message) bool) bool {
	for i, queued := range n.queue {
		if !match(queued.message) {
			continue
		}
		n.queue = append(n.queue[:i], n.queue[i+1:]...)
		if queued.ctx.Err() != nil || n.disconnected[queued.message.From] || n.disconnected[queued.message.To] {
			return false
		}
		if err := n.nodes[queued.message.To].Step(queued.message, n.now); err != nil {
			panic(err)
		}
		return true
	}
	return false
}

func (n *memoryNetwork) deliverAll() {
	for len(n.queue) > 0 {
		n.deliverNext(func(Message) bool { return true })
	}
}

func (n *memoryNetwork) dropNext(match func(Message) bool) bool {
	for i, queued := range n.queue {
		if match(queued.message) {
			n.queue = append(n.queue[:i], n.queue[i+1:]...)
			return true
		}
	}
	return false
}

func TestSingleNodeElectsItself(t *testing.T) {
	network := newMemoryNetwork()
	node := network.addNode(t, "a", []NodeID{"a"}, 10*time.Millisecond, LogPosition{})

	network.advance(10 * time.Millisecond)
	network.tick("a")

	state := node.State()
	if state.Role != Leader || state.Term != 1 || state.VotedFor != "a" {
		t.Fatalf("state = %+v, want single-node leader in term 1", state)
	}
}

func TestHealthyThreeNodeClusterElectsOneLeader(t *testing.T) {
	network, nodes := newThreeNodeCluster(t, nil)

	network.advance(10 * time.Millisecond)
	network.tick("a", "b", "c")
	network.deliverAll()

	assertLeaderCount(t, nodes, 1)
	if nodes["a"].State().Role != Leader {
		t.Fatalf("a role = %s, want leader", nodes["a"].State().Role)
	}
}

func TestDisconnectedLeaderIsReplacedByMajority(t *testing.T) {
	network, nodes := newThreeNodeCluster(t, nil)
	network.advance(10 * time.Millisecond)
	network.tick("a", "b", "c")
	network.deliverAll()
	if nodes["a"].State().Role != Leader {
		t.Fatal("a did not win initial election")
	}

	network.disconnected["a"] = true
	// The initial leader heartbeat reset b's election deadline to 30ms.
	network.advance(20 * time.Millisecond)
	network.tick("b", "c")
	network.deliverAll()

	if nodes["b"].State().Role != Leader {
		t.Fatalf("b state = %+v, want replacement leader", nodes["b"].State())
	}
	if nodes["c"].State().Term != nodes["b"].State().Term {
		t.Fatalf("majority terms differ: b=%d c=%d", nodes["b"].State().Term, nodes["c"].State().Term)
	}
}

func TestMinorityCannotElectLeader(t *testing.T) {
	network, nodes := newThreeNodeCluster(t, nil)
	network.disconnected["a"] = true

	network.advance(10 * time.Millisecond)
	network.tick("a")
	network.deliverAll()

	if nodes["a"].State().Role != Candidate {
		t.Fatalf("isolated node role = %s, want candidate", nodes["a"].State().Role)
	}
	assertLeaderCount(t, nodes, 0)
}

func TestCandidateWithStaleLogCannotWin(t *testing.T) {
	logs := map[NodeID]LogPosition{
		"a": {Index: 8, Term: 2},
		"b": {Index: 4, Term: 3},
		"c": {Index: 4, Term: 3},
	}
	network, nodes := newThreeNodeCluster(t, logs)

	network.advance(10 * time.Millisecond)
	network.tick("a")
	network.deliverAll()

	if nodes["a"].State().Role != Candidate || nodes["a"].State().Votes != 1 {
		t.Fatalf("stale candidate state = %+v, want one self-vote", nodes["a"].State())
	}
	assertLeaderCount(t, nodes, 0)
}

func TestNodesStepDownOnHigherTerm(t *testing.T) {
	network, nodes := newThreeNodeCluster(t, nil)
	network.advance(10 * time.Millisecond)
	network.tick("a")
	network.deliverAll()
	if nodes["a"].State().Role != Leader {
		t.Fatal("a did not become leader")
	}

	err := nodes["a"].Step(Message{
		From: "b", To: "a", Type: MessageHeartbeatResponse,
		HeartbeatResponse: HeartbeatResponse{Term: 7},
	}, network.now)
	if err != nil {
		t.Fatal(err)
	}

	state := nodes["a"].State()
	if state.Role != Follower || state.Term != 7 || state.VotedFor != "" {
		t.Fatalf("state after higher term = %+v, want term-7 follower", state)
	}
}

func TestDelayedResponseFromEarlierElectionIsIgnored(t *testing.T) {
	network, nodes := newThreeNodeCluster(t, nil)
	network.advance(10 * time.Millisecond)
	network.tick("a") // election 1

	if !network.deliverNext(func(m Message) bool {
		return m.Type == MessageRequestVote && m.From == "a" && m.To == "b"
	}) {
		t.Fatal("term-1 vote request was not delivered")
	}
	// b's response is now queued, while a has only its own recorded vote.
	network.advance(10 * time.Millisecond)
	network.tick("a") // election 2 cancels election 1

	if !network.deliverNext(func(m Message) bool {
		return m.Type == MessageRequestVoteResponse && m.From == "b" && m.RequestVoteResponse.Election == 1
	}) {
		t.Fatal("delayed term-1 response was not delivered")
	}

	state := nodes["a"].State()
	if state.Role != Candidate || state.Term != 2 || state.Election != 2 || state.Votes != 1 {
		t.Fatalf("state after stale response = %+v, want unchanged election 2", state)
	}
	// The still-queued request to c belongs to the canceled first election.
	if network.deliverNext(func(m Message) bool {
		return m.Type == MessageRequestVote && m.To == "c" && m.RequestVote.Election == 1
	}) {
		t.Fatal("canceled election request was delivered")
	}
}

func TestElectionDeadlineResetsOnlyForGrantedVoteOrHeartbeat(t *testing.T) {
	network := newMemoryNetwork()
	members := []NodeID{"a", "b", "c"}
	node := network.addNode(t, "b", members, 20*time.Millisecond, LogPosition{Index: 5, Term: 2})
	original := node.State().ElectionDeadline

	network.advance(time.Millisecond)
	_ = node.Step(Message{
		From: "a", To: "b", Type: MessageRequestVote,
		RequestVote: RequestVoteRequest{Term: 1, CandidateID: "a", LastLog: LogPosition{Index: 9, Term: 1}, Election: 1},
	}, network.now)
	if got := node.State().ElectionDeadline; !got.Equal(original) {
		t.Fatalf("denied vote reset deadline to %v; want %v", got, original)
	}

	network.advance(time.Millisecond)
	_ = node.Step(Message{
		From: "a", To: "b", Type: MessageRequestVote,
		RequestVote: RequestVoteRequest{Term: 1, CandidateID: "a", LastLog: LogPosition{Index: 5, Term: 2}, Election: 1},
	}, network.now)
	if got := node.State().ElectionDeadline; !got.Equal(network.now.Add(20 * time.Millisecond)) {
		t.Fatalf("granted vote deadline = %v, want reset from now", got)
	}
}

func newThreeNodeCluster(t *testing.T, logs map[NodeID]LogPosition) (*memoryNetwork, map[NodeID]*Node) {
	t.Helper()
	network := newMemoryNetwork()
	members := []NodeID{"a", "b", "c"}
	nodes := map[NodeID]*Node{
		"a": network.addNode(t, "a", members, 10*time.Millisecond, logs["a"]),
		"b": network.addNode(t, "b", members, 20*time.Millisecond, logs["b"]),
		"c": network.addNode(t, "c", members, 30*time.Millisecond, logs["c"]),
	}
	return network, nodes
}

func assertLeaderCount(t *testing.T, nodes map[NodeID]*Node, want int) {
	t.Helper()
	got := 0
	for _, node := range nodes {
		if node.State().Role == Leader {
			got++
		}
	}
	if got != want {
		t.Fatalf("leader count = %d, want %d", got, want)
	}
}
