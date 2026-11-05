package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/yourusername/raftkv/internal/raft"
)

type event struct {
	apply func() (any, error)
	done  chan eventResult
}

type eventResult struct {
	value any
	err   error
}

// Runtime gives one goroutine exclusive ownership of a Raft node.
type Runtime struct {
	node   *raft.Node
	logger *slog.Logger
	events chan event
	done   chan struct{}
}

func NewRuntime(node *raft.Node, logger *slog.Logger) *Runtime {
	return &Runtime{node: node, logger: logger, events: make(chan event, 256), done: make(chan struct{})}
}

func (r *Runtime) Run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-r.events:
			before := r.node.State()
			value, err := request.apply()
			r.logTransition(before, r.node.State(), err)
			request.done <- eventResult{value: value, err: err}
		case now := <-ticker.C:
			before := r.node.State()
			err := r.node.Tick(now)
			if err == nil {
				err = r.node.MaybeCompact()
			}
			r.logTransition(before, r.node.State(), err)
		}
	}
}

func (r *Runtime) Wait() { <-r.done }

func (r *Runtime) Step(ctx context.Context, message raft.Message) error {
	_, err := r.submit(ctx, func() (any, error) {
		return nil, r.node.Step(message, time.Now())
	})
	return err
}

func (r *Runtime) State(ctx context.Context) (raft.State, error) {
	value, err := r.submit(ctx, func() (any, error) { return r.node.State(), nil })
	if err != nil {
		return raft.State{}, err
	}
	return value.(raft.State), nil
}

func (r *Runtime) Propose(ctx context.Context, command raft.Command) (*raft.Proposal, raft.State, error) {
	value, err := r.submit(ctx, func() (any, error) {
		proposal, proposeErr := r.node.Propose(command)
		return struct {
			proposal *raft.Proposal
			state    raft.State
		}{proposal: proposal, state: r.node.State()}, proposeErr
	})
	if err != nil {
		state, _ := r.State(ctx)
		return nil, state, err
	}
	result := value.(struct {
		proposal *raft.Proposal
		state    raft.State
	})
	return result.proposal, result.state, nil
}

func (r *Runtime) submit(ctx context.Context, apply func() (any, error)) (any, error) {
	request := event{apply: apply, done: make(chan eventResult, 1)}
	select {
	case r.events <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.done:
		return nil, context.Canceled
	}
	select {
	case result := <-request.done:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.done:
		return nil, context.Canceled
	}
}

func (r *Runtime) logTransition(before, after raft.State, err error) {
	if err != nil {
		r.logger.Error("raft event failed", "node", after.ID, "term", after.Term, "role", after.Role.String(), "error", err)
		return
	}
	if before.Term != after.Term || before.Role != after.Role || before.CommitIndex != after.CommitIndex || len(before.Log) != len(after.Log) {
		r.logger.Info("raft state changed",
			"node", after.ID, "term", after.Term, "role", after.Role.String(),
			"leader", after.LeaderID, "commit_index", after.CommitIndex, "log_length", len(after.Log))
	}
}
