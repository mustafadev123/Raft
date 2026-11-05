package transport

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	raftkvv1 "github.com/yourusername/raftkv/api/raftkv/v1"
	"github.com/yourusername/raftkv/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Receiver func(context.Context, raft.Message) error

type voteKey struct {
	peer     raft.NodeID
	election uint64
}

type appendKey struct {
	peer      raft.NodeID
	requestID uint64
}

type snapshotKey struct {
	peer      raft.NodeID
	requestID uint64
}

// GRPC reuses one ClientConn per peer. Send never waits for network I/O.
type GRPC struct {
	localID  raft.NodeID
	peers    map[raft.NodeID]string
	timeout  time.Duration
	logger   *slog.Logger
	receiver Receiver

	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	conns     map[raft.NodeID]*grpc.ClientConn
	votes     map[voteKey][]chan raft.RequestVoteResponse
	appends   map[appendKey][]chan raft.AppendEntriesResponse
	snapshots map[snapshotKey][]chan raft.InstallSnapshotResponse
	wg        sync.WaitGroup
}

func NewGRPC(localID raft.NodeID, peers map[raft.NodeID]string, timeout time.Duration, logger *slog.Logger) *GRPC {
	ctx, cancel := context.WithCancel(context.Background())
	return &GRPC{
		localID: localID, peers: peers, timeout: timeout, logger: logger,
		ctx: ctx, cancel: cancel, conns: make(map[raft.NodeID]*grpc.ClientConn),
		votes: make(map[voteKey][]chan raft.RequestVoteResponse), appends: make(map[appendKey][]chan raft.AppendEntriesResponse),
		snapshots: make(map[snapshotKey][]chan raft.InstallSnapshotResponse),
	}
}

func (t *GRPC) SetReceiver(receiver Receiver) { t.receiver = receiver }

func (t *GRPC) Send(parent context.Context, message raft.Message) error {
	if t.captureCoreResponse(message) {
		return nil
	}
	if message.Type != raft.MessageRequestVote && message.Type != raft.MessageAppendEntries && message.Type != raft.MessageInstallSnapshot {
		return fmt.Errorf("unsupported outbound message type %d", message.Type)
	}
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ctx, cancel := context.WithTimeout(parent, t.timeout)
		defer cancel()
		response, err := t.call(ctx, message)
		if err != nil {
			t.logger.Warn("peer RPC failed", "node", t.localID, "peer", message.To, "type", message.Type, "error", err)
			return
		}
		if t.receiver != nil {
			if err := t.receiver(t.ctx, response); err != nil && t.ctx.Err() == nil {
				t.logger.Warn("peer response delivery failed", "node", t.localID, "peer", message.To, "error", err)
			}
		}
	}()
	return nil
}

func (t *GRPC) call(ctx context.Context, message raft.Message) (raft.Message, error) {
	connection, err := t.connection(message.To)
	if err != nil {
		return raft.Message{}, err
	}
	client := raftkvv1.NewRaftPeerClient(connection)
	switch message.Type {
	case raft.MessageRequestVote:
		request := message.RequestVote
		response, err := client.RequestVote(ctx, &raftkvv1.RequestVoteRequest{
			Term: request.Term, CandidateId: string(request.CandidateID), LastLogIndex: request.LastLog.Index,
			LastLogTerm: request.LastLog.Term, Election: request.Election,
		})
		if err != nil {
			return raft.Message{}, err
		}
		return raft.Message{
			From: message.To, To: message.From, Type: raft.MessageRequestVoteResponse,
			RequestVoteResponse: raft.RequestVoteResponse{Term: response.GetTerm(), VoteGranted: response.GetVoteGranted(), Election: response.GetElection()},
		}, nil
	case raft.MessageAppendEntries:
		request := message.AppendEntries
		response, err := client.AppendEntries(ctx, encodeAppendRequest(request))
		if err != nil {
			return raft.Message{}, err
		}
		return raft.Message{
			From: message.To, To: message.From, Type: raft.MessageAppendEntriesResponse,
			AppendEntriesResponse: raft.AppendEntriesResponse{
				Term: response.GetTerm(), Success: response.GetSuccess(), MatchIndex: response.GetMatchIndex(),
				ConflictIndex: response.GetConflictIndex(), ConflictTerm: response.GetConflictTerm(), RequestID: response.GetRequestId(),
			},
		}, nil
	case raft.MessageInstallSnapshot:
		request := message.InstallSnapshot
		response, err := client.InstallSnapshot(ctx, &raftkvv1.InstallSnapshotRequest{
			Term: request.Term, LeaderId: string(request.LeaderID), LastIncludedIndex: request.LastIncludedIndex,
			LastIncludedTerm: request.LastIncludedTerm, Data: request.Data, RequestId: request.RequestID,
		})
		if err != nil {
			return raft.Message{}, err
		}
		return raft.Message{
			From: message.To, To: message.From, Type: raft.MessageInstallSnapshotResponse,
			InstallSnapshotResponse: raft.InstallSnapshotResponse{
				Term: response.GetTerm(), Success: response.GetSuccess(), MatchIndex: response.GetMatchIndex(), RequestID: response.GetRequestId(),
			},
		}, nil
	default:
		return raft.Message{}, fmt.Errorf("unsupported request type %d", message.Type)
	}
}

func (t *GRPC) connection(peer raft.NodeID) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if connection := t.conns[peer]; connection != nil {
		return connection, nil
	}
	address, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("no address for peer %q", peer)
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[peer] = connection
	return connection, nil
}

func (t *GRPC) CaptureVote(peer raft.NodeID, election uint64) (<-chan raft.RequestVoteResponse, func()) {
	key := voteKey{peer: peer, election: election}
	channel := make(chan raft.RequestVoteResponse, 1)
	t.mu.Lock()
	t.votes[key] = append(t.votes[key], channel)
	t.mu.Unlock()
	return channel, func() {
		t.mu.Lock()
		t.votes[key] = removeWaiter(t.votes[key], channel)
		if len(t.votes[key]) == 0 {
			delete(t.votes, key)
		}
		t.mu.Unlock()
	}
}

func (t *GRPC) CaptureAppend(peer raft.NodeID, requestID uint64) (<-chan raft.AppendEntriesResponse, func()) {
	key := appendKey{peer: peer, requestID: requestID}
	channel := make(chan raft.AppendEntriesResponse, 1)
	t.mu.Lock()
	t.appends[key] = append(t.appends[key], channel)
	t.mu.Unlock()
	return channel, func() {
		t.mu.Lock()
		t.appends[key] = removeWaiter(t.appends[key], channel)
		if len(t.appends[key]) == 0 {
			delete(t.appends, key)
		}
		t.mu.Unlock()
	}
}

func (t *GRPC) CaptureSnapshot(peer raft.NodeID, requestID uint64) (<-chan raft.InstallSnapshotResponse, func()) {
	key := snapshotKey{peer: peer, requestID: requestID}
	channel := make(chan raft.InstallSnapshotResponse, 1)
	t.mu.Lock()
	t.snapshots[key] = append(t.snapshots[key], channel)
	t.mu.Unlock()
	return channel, func() {
		t.mu.Lock()
		t.snapshots[key] = removeWaiter(t.snapshots[key], channel)
		if len(t.snapshots[key]) == 0 {
			delete(t.snapshots, key)
		}
		t.mu.Unlock()
	}
}

func (t *GRPC) captureCoreResponse(message raft.Message) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch message.Type {
	case raft.MessageRequestVoteResponse:
		key := voteKey{peer: message.To, election: message.RequestVoteResponse.Election}
		if waiters := t.votes[key]; len(waiters) > 0 {
			channel := waiters[0]
			t.votes[key] = waiters[1:]
			if len(t.votes[key]) == 0 {
				delete(t.votes, key)
			}
			channel <- message.RequestVoteResponse
			return true
		}
	case raft.MessageAppendEntriesResponse:
		key := appendKey{peer: message.To, requestID: message.AppendEntriesResponse.RequestID}
		if waiters := t.appends[key]; len(waiters) > 0 {
			channel := waiters[0]
			t.appends[key] = waiters[1:]
			if len(t.appends[key]) == 0 {
				delete(t.appends, key)
			}
			channel <- message.AppendEntriesResponse
			return true
		}
	case raft.MessageInstallSnapshotResponse:
		key := snapshotKey{peer: message.To, requestID: message.InstallSnapshotResponse.RequestID}
		if waiters := t.snapshots[key]; len(waiters) > 0 {
			channel := waiters[0]
			t.snapshots[key] = waiters[1:]
			if len(t.snapshots[key]) == 0 {
				delete(t.snapshots, key)
			}
			channel <- message.InstallSnapshotResponse
			return true
		}
	}
	return false
}

func removeWaiter[T any](waiters []chan T, target chan T) []chan T {
	for i, waiter := range waiters {
		if waiter == target {
			return append(waiters[:i], waiters[i+1:]...)
		}
	}
	return waiters
}

func (t *GRPC) Close() error {
	t.cancel()
	t.wg.Wait()
	t.mu.Lock()
	defer t.mu.Unlock()
	var first error
	for _, connection := range t.conns {
		if err := connection.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func encodeAppendRequest(request raft.AppendEntriesRequest) *raftkvv1.AppendEntriesRequest {
	entries := make([]*raftkvv1.LogEntry, 0, len(request.Entries))
	for _, entry := range request.Entries {
		entries = append(entries, &raftkvv1.LogEntry{
			Index: entry.Index, Term: entry.Term,
			Command: &raftkvv1.Command{
				Operation: encodeOperation(entry.Command.Operation), ClientId: entry.Command.ClientID,
				Sequence: entry.Command.Sequence, Key: entry.Command.Key, Value: entry.Command.Value,
			},
		})
	}
	return &raftkvv1.AppendEntriesRequest{
		Term: request.Term, LeaderId: string(request.LeaderID), PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm: request.PrevLogTerm, Entries: entries, LeaderCommit: request.LeaderCommit, RequestId: request.RequestID,
	}
}

func encodeOperation(operation raft.Operation) raftkvv1.Operation {
	switch operation {
	case raft.OperationPut:
		return raftkvv1.Operation_OPERATION_PUT
	case raft.OperationDelete:
		return raftkvv1.Operation_OPERATION_DELETE
	case raft.OperationGet:
		return raftkvv1.Operation_OPERATION_GET
	default:
		return raftkvv1.Operation_OPERATION_NOOP
	}
}
