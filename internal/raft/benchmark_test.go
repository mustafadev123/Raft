package raft_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/yourusername/raftkv/internal/kv"
	"github.com/yourusername/raftkv/internal/raft"
	"github.com/yourusername/raftkv/internal/storage"
)

type benchmarkNetwork struct {
	now   time.Time
	nodes map[raft.NodeID]*raft.Node
	queue []queuedMessage
}

func (n *benchmarkNetwork) Send(ctx context.Context, message raft.Message) error {
	n.queue = append(n.queue, queuedMessage{ctx: ctx, message: message})
	return nil
}

func (n *benchmarkNetwork) deliverAll(b *testing.B) {
	b.Helper()
	for len(n.queue) > 0 {
		queued := n.queue[0]
		n.queue = n.queue[1:]
		if queued.ctx.Err() != nil {
			continue
		}
		if err := n.nodes[queued.message.To].Step(queued.message, n.now); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkThreeNodeDurablePut measures sequential committed Put operations
// through a three-node in-process cluster. Every node uses FileStore, so each
// safety-critical update rewrites, syncs, and atomically replaces its complete
// state file. Run with -benchtime=20x for a bounded reproducible sample count.
func BenchmarkThreeNodeDurablePut(b *testing.B) {
	network := &benchmarkNetwork{now: time.Unix(0, 0), nodes: make(map[raft.NodeID]*raft.Node)}
	members := []raft.NodeID{"a", "b", "c"}
	for i, id := range members {
		node, err := raft.NewNode(raft.Config{
			ID: id, Members: members, HeartbeatInterval: 5 * time.Millisecond,
			Timeouts: fixedTimeout(time.Duration(10+i*10) * time.Millisecond), Transport: network,
			StateMachine: kv.New(1024),
			Store:        &storage.FileStore{Path: filepath.Join(b.TempDir(), string(id), "raft-state.json")},
		}, network.now)
		if err != nil {
			b.Fatal(err)
		}
		network.nodes[id] = node
	}
	network.now = network.now.Add(10 * time.Millisecond)
	for _, node := range network.nodes {
		if err := node.Tick(network.now); err != nil {
			b.Fatal(err)
		}
	}
	network.deliverAll(b)
	if network.nodes["a"].State().Role != raft.Leader {
		b.Fatal("node a did not become leader")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proposal, err := network.nodes["a"].Propose(raft.Command{
			Operation: raft.OperationPut, ClientID: fmt.Sprintf("bench-%d", i), Sequence: 1,
			Key: "key", Value: fmt.Sprintf("value-%d", i),
		})
		if err != nil {
			b.Fatal(err)
		}
		network.deliverAll(b)
		if result, ready := proposal.Result(); !ready || result.Error != "" {
			b.Fatalf("proposal result = %+v, ready=%v", result, ready)
		}
	}
}
