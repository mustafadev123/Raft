package raft_test

import (
	"fmt"
	"testing"

	"github.com/yourusername/raftkv/internal/raft"
)

type historyOperation struct {
	name     string
	command  raft.Command
	invoke   int
	complete int // zero means timed out/incomplete and may be omitted
	result   raft.Result
}

func TestBoundedClientHistoryIsLinearizable(t *testing.T) {
	c := newTestCluster(t)
	c.electA(t)
	clock := 1
	history := make([]historyOperation, 0, 5)

	firstCommand := raft.Command{Operation: raft.OperationPut, ClientID: "p1", Sequence: 1, Key: "x", Value: "one"}
	first := propose(t, c.nodes["a"], firstCommand)
	history = append(history, historyOperation{name: "put-one", command: firstCommand, invoke: clock})
	clock++
	c.deliverAll()
	firstResult := requireResult(t, first)
	history[0].complete, history[0].result = clock, firstResult
	clock++

	commands := []struct {
		name    string
		command raft.Command
	}{
		{"get-one", raft.Command{Operation: raft.OperationGet, ClientID: "g1", Sequence: 1, Key: "x"}},
		{"put-two", raft.Command{Operation: raft.OperationPut, ClientID: "p2", Sequence: 1, Key: "x", Value: "two"}},
		{"get-two", raft.Command{Operation: raft.OperationGet, ClientID: "g2", Sequence: 1, Key: "x"}},
	}
	proposals := make([]*raft.Proposal, 0, len(commands))
	for _, item := range commands {
		proposals = append(proposals, propose(t, c.nodes["a"], item.command))
		history = append(history, historyOperation{name: item.name, command: item.command, invoke: clock})
		clock++
	}
	c.deliverAll()
	for i, proposal := range proposals {
		result := requireResult(t, proposal)
		history[i+1].complete, history[i+1].result = clock, result
		clock++
	}

	// This operation models an RPC timeout. It is invoked but never completed;
	// the bounded checker may omit it because its outcome is unknown.
	c.disconnected["a"] = true
	incompleteCommand := raft.Command{Operation: raft.OperationDelete, ClientID: "timeout", Sequence: 1, Key: "x"}
	_ = propose(t, c.nodes["a"], incompleteCommand)
	history = append(history, historyOperation{name: "timed-out-delete", command: incompleteCommand, invoke: clock})

	if ok, order := checkLinearizable(history, 8); !ok {
		t.Fatalf("history is not linearizable: %+v", history)
	} else {
		t.Logf("linearization order: %v", order)
	}
}

// checkLinearizable exhaustively searches completed operations up to limit.
// Incomplete operations are conservatively omitted because a timeout leaves
// their outcome unknown.
func checkLinearizable(history []historyOperation, limit int) (bool, []string) {
	completed := make([]historyOperation, 0, len(history))
	for _, operation := range history {
		if operation.complete != 0 {
			completed = append(completed, operation)
		}
	}
	if len(completed) > limit {
		return false, nil
	}
	used := make([]bool, len(completed))
	return searchHistory(completed, used, map[string]string{}, nil)
}

func searchHistory(operations []historyOperation, used []bool, state map[string]string, order []string) (bool, []string) {
	if len(order) == len(operations) {
		return true, order
	}
	for i, operation := range operations {
		if used[i] || !predecessorsUsed(operations, used, operation) {
			continue
		}
		nextState := cloneValues(state)
		expected := applySequential(nextState, operation.command)
		if expected != operation.result {
			continue
		}
		used[i] = true
		if ok, result := searchHistory(operations, used, nextState, append(order, operation.name)); ok {
			return true, result
		}
		used[i] = false
	}
	return false, nil
}

func predecessorsUsed(operations []historyOperation, used []bool, candidate historyOperation) bool {
	for i, operation := range operations {
		if !used[i] && operation.complete < candidate.invoke && operation.complete != 0 {
			return false
		}
	}
	return true
}

func applySequential(values map[string]string, command raft.Command) raft.Result {
	value, found := values[command.Key]
	switch command.Operation {
	case raft.OperationPut:
		values[command.Key] = command.Value
		return raft.Result{Value: value, Found: found}
	case raft.OperationDelete:
		if found {
			delete(values, command.Key)
		}
		return raft.Result{Value: value, Found: found}
	case raft.OperationGet:
		return raft.Result{Value: value, Found: found}
	default:
		panic(fmt.Sprintf("unsupported operation %d", command.Operation))
	}
}

func cloneValues(values map[string]string) map[string]string {
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
