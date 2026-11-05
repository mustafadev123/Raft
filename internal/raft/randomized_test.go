package raft_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yourusername/raftkv/internal/kv"
	"github.com/yourusername/raftkv/internal/raft"
	"github.com/yourusername/raftkv/internal/storage"
)

type fiveCluster struct {
	t           *testing.T
	now         time.Time
	members     []raft.NodeID
	nodes       map[raft.NodeID]*raft.Node
	machines    map[raft.NodeID]*kv.StateMachine
	stores      map[raft.NodeID]*storage.MemoryStore
	active      map[raft.NodeID]bool
	blocked     map[string]bool
	queue       []queuedMessage
	trace       []string
	random      *rand.Rand
	clientCount int
}

func newFiveCluster(t *testing.T, seed int64) *fiveCluster {
	c := &fiveCluster{
		t: t, now: time.Unix(0, 0), members: []raft.NodeID{"a", "b", "c", "d", "e"},
		nodes: make(map[raft.NodeID]*raft.Node), machines: make(map[raft.NodeID]*kv.StateMachine),
		stores: make(map[raft.NodeID]*storage.MemoryStore), active: make(map[raft.NodeID]bool),
		blocked: make(map[string]bool), random: rand.New(rand.NewSource(seed)),
	}
	for _, id := range c.members {
		c.stores[id] = &storage.MemoryStore{}
		c.restart(id)
	}
	return c
}

func (c *fiveCluster) Send(ctx context.Context, message raft.Message) error {
	c.queue = append(c.queue, queuedMessage{ctx: ctx, message: message})
	return nil
}

func (c *fiveCluster) restart(id raft.NodeID) {
	machine := kv.New(64)
	timeout := time.Duration(50+10*strings.Index("abcde", string(id))) * time.Millisecond
	node, err := raft.NewNode(raft.Config{
		ID: id, Members: c.members, HeartbeatInterval: 15 * time.Millisecond,
		Timeouts: fixedTimeout(timeout), Transport: c, StateMachine: machine, Store: c.stores[id],
		MaxSnapshotBytes: 1 << 20,
	}, c.now)
	if err != nil {
		c.fail("restart %s: %v", id, err)
	}
	c.nodes[id], c.machines[id], c.active[id] = node, machine, true
	c.record("restart %s", id)
}

func (c *fiveCluster) record(format string, args ...any) {
	c.trace = append(c.trace, fmt.Sprintf("t=%s ", c.now.Sub(time.Unix(0, 0)))+fmt.Sprintf(format, args...))
}

func (c *fiveCluster) fail(format string, args ...any) {
	start := 0
	if len(c.trace) > 200 {
		start = len(c.trace) - 200
	}
	c.t.Fatalf(format+"\ntrace:\n%s", append(args, strings.Join(c.trace[start:], "\n"))...)
}

func linkKey(a, b raft.NodeID) string {
	if a > b {
		a, b = b, a
	}
	return string(a) + "-" + string(b)
}

func (c *fiveCluster) partition(left []raft.NodeID) {
	inLeft := make(map[raft.NodeID]bool)
	for _, id := range left {
		inLeft[id] = true
	}
	for _, a := range c.members {
		for _, b := range c.members {
			if inLeft[a] != inLeft[b] {
				c.blocked[linkKey(a, b)] = true
			}
		}
	}
	c.record("partition left=%v", left)
}

func (c *fiveCluster) heal() {
	c.blocked = make(map[string]bool)
	c.record("heal all links")
}

func (c *fiveCluster) tickAll() {
	c.now = c.now.Add(5 * time.Millisecond)
	for _, id := range c.members {
		if c.active[id] {
			if err := c.nodes[id].Tick(c.now); err != nil {
				c.fail("tick %s: %v", id, err)
			}
		}
	}
}

func (c *fiveCluster) deliverAt(index int, allowDrop bool) {
	queued := c.queue[index]
	c.queue = append(c.queue[:index], c.queue[index+1:]...)
	message := queued.message
	if queued.ctx.Err() != nil || !c.active[message.From] || !c.active[message.To] || c.blocked[linkKey(message.From, message.To)] {
		c.record("discard type=%d %s->%s", message.Type, message.From, message.To)
		return
	}
	if allowDrop && c.random.Intn(100) < 15 {
		c.record("drop type=%d %s->%s", message.Type, message.From, message.To)
		return
	}
	if allowDrop && c.random.Intn(100) < 10 {
		c.queue = append(c.queue, queued)
		c.record("duplicate type=%d %s->%s", message.Type, message.From, message.To)
	}
	if err := c.nodes[message.To].Step(message, c.now); err != nil {
		c.fail("deliver type=%d %s->%s: %v", message.Type, message.From, message.To, err)
	}
	c.record("deliver type=%d %s->%s", message.Type, message.From, message.To)
}

func (c *fiveCluster) leader() (raft.NodeID, bool) {
	for _, id := range c.members {
		if c.active[id] && c.nodes[id].State().Role == raft.Leader {
			return id, true
		}
	}
	return "", false
}

func (c *fiveCluster) checkSafety() {
	leaders := make(map[uint64]raft.NodeID)
	committed := make(map[uint64]raft.Entry)
	for _, id := range c.members {
		if !c.active[id] {
			continue
		}
		state := c.nodes[id].State()
		if state.LastIncludedIndex > state.LastApplied || state.LastApplied > state.CommitIndex {
			c.fail("snapshot/apply bounds on %s: %+v", id, state)
		}
		if state.Role == raft.Leader {
			if prior, exists := leaders[state.Term]; exists && prior != id {
				c.fail("two leaders in term %d: %s and %s", state.Term, prior, id)
			}
			leaders[state.Term] = id
		}
		for _, entry := range state.Log {
			if entry.Index > state.CommitIndex {
				continue
			}
			if prior, exists := committed[entry.Index]; exists && (prior.Term != entry.Term || prior.Command != entry.Command) {
				c.fail("committed conflict index %d: %+v vs %+v", entry.Index, prior, entry)
			}
			committed[entry.Index] = entry
		}
	}
	for term, leader := range leaders {
		state := c.nodes[leader].State()
		for index, entry := range committed {
			if index <= state.LastIncludedIndex {
				continue
			}
			found := false
			for _, candidate := range state.Log {
				if candidate.Index == index && candidate.Term == entry.Term && candidate.Command == entry.Command {
					found = true
				}
			}
			if !found {
				c.fail("leader %s term %d lacks committed entry %d", leader, term, index)
			}
		}
	}
}

func (c *fiveCluster) fairRound() {
	c.tickAll()
	for len(c.queue) > 0 {
		c.deliverAt(0, false)
	}
}

func TestRandomizedFiveNodeSafetyAndRecovery(t *testing.T) {
	if os.Getenv("RAFTKV_RANDOMIZED") != "1" {
		t.Skip("set RAFTKV_RANDOMIZED=1 to run seeded randomized scenarios")
	}
	for _, seed := range randomizedSeeds(t) {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			c := newFiveCluster(t, seed)
			for step := 0; step < 400; step++ {
				switch step {
				case 80:
					c.partition([]raft.NodeID{"a", "b"})
				case 150:
					c.heal()
				case 190:
					id := c.members[c.random.Intn(len(c.members))]
					c.active[id] = false
					c.record("crash %s", id)
				case 230:
					for _, id := range c.members {
						if !c.active[id] {
							c.restart(id)
						}
					}
				case 270:
					if leader, ok := c.leader(); ok {
						if err := c.nodes[leader].Compact(); err != nil {
							c.fail("compact %s: %v", leader, err)
						}
						c.partition([]raft.NodeID{"e"})
					}
				case 330:
					c.heal()
				}
				c.tickAll()
				if len(c.queue) > 0 && c.random.Intn(100) < 75 {
					c.deliverAt(c.random.Intn(len(c.queue)), true)
				}
				if step%35 == 0 {
					if leader, ok := c.leader(); ok {
						c.clientCount++
						_, _ = c.nodes[leader].Propose(raft.Command{
							Operation: raft.OperationPut, ClientID: fmt.Sprintf("client-%d", c.clientCount),
							Sequence: 1, Key: fmt.Sprintf("key-%d", c.clientCount%3), Value: strconv.Itoa(c.clientCount),
						})
					}
				}
				c.checkSafety()
			}

			c.heal()
			for _, id := range c.members {
				if !c.active[id] {
					c.restart(id)
				}
			}
			for round := 0; round < 500; round++ {
				c.fairRound()
				if _, ok := c.leader(); ok && len(c.queue) == 0 {
					break
				}
			}
			leader, ok := c.leader()
			if !ok {
				c.fail("no leader after fair delivery")
			}
			proposal, err := c.nodes[leader].Propose(raft.Command{Operation: raft.OperationPut, ClientID: "liveness", Sequence: 1, Key: "live", Value: "yes"})
			if err != nil {
				c.fail("liveness proposal: %v", err)
			}
			for round := 0; round < 500; round++ {
				c.fairRound()
				if _, ready := proposal.Result(); ready {
					return
				}
			}
			c.fail("proposal did not complete after healthy majority and fair delivery")
		})
	}
}

func randomizedSeeds(t *testing.T) []int64 {
	t.Helper()
	value := os.Getenv("RAFTKV_SEEDS")
	if value == "" {
		value = "101,202,303,404,505"
	}
	parts := strings.Split(value, ",")
	seeds := make([]int64, 0, len(parts))
	for _, part := range parts {
		seed, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			t.Fatalf("invalid RAFTKV_SEEDS: %v", err)
		}
		seeds = append(seeds, seed)
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i] < seeds[j] })
	return seeds
}
