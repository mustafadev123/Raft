package server

import (
	"context"
	"errors"
	"time"

	raftkvv1 "github.com/yourusername/raftkv/api/raftkv/v1"
	"github.com/yourusername/raftkv/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PeerService struct {
	raftkvv1.UnimplementedRaftPeerServer
	Runtime   *Runtime
	Responses interface {
		CaptureVote(raft.NodeID, uint64) (<-chan raft.RequestVoteResponse, func())
		CaptureAppend(raft.NodeID, uint64) (<-chan raft.AppendEntriesResponse, func())
		CaptureSnapshot(raft.NodeID, uint64) (<-chan raft.InstallSnapshotResponse, func())
	}
}

func (s *PeerService) InstallSnapshot(ctx context.Context, request *raftkvv1.InstallSnapshotRequest) (*raftkvv1.InstallSnapshotResponse, error) {
	state, err := s.Runtime.State(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	message := raft.Message{
		From: raft.NodeID(request.GetLeaderId()), To: state.ID, Type: raft.MessageInstallSnapshot,
		InstallSnapshot: raft.InstallSnapshotRequest{
			Term: request.GetTerm(), LeaderID: raft.NodeID(request.GetLeaderId()),
			LastIncludedIndex: request.GetLastIncludedIndex(), LastIncludedTerm: request.GetLastIncludedTerm(),
			Data: append([]byte(nil), request.GetData()...), RequestID: request.GetRequestId(),
		},
	}
	responseChannel, release := s.Responses.CaptureSnapshot(message.From, request.GetRequestId())
	defer release()
	if err := s.Runtime.Step(ctx, message); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	select {
	case response := <-responseChannel:
		return &raftkvv1.InstallSnapshotResponse{
			Term: response.Term, Success: response.Success, MatchIndex: response.MatchIndex, RequestId: response.RequestID,
		}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
	}
}

func (s *PeerService) RequestVote(ctx context.Context, request *raftkvv1.RequestVoteRequest) (*raftkvv1.RequestVoteResponse, error) {
	message := raft.Message{
		From: raft.NodeID(request.GetCandidateId()), Type: raft.MessageRequestVote,
		RequestVote: raft.RequestVoteRequest{
			Term: request.GetTerm(), CandidateID: raft.NodeID(request.GetCandidateId()),
			LastLog:  raft.LogPosition{Index: request.GetLastLogIndex(), Term: request.GetLastLogTerm()},
			Election: request.GetElection(),
		},
	}
	state, err := s.Runtime.State(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	message.To = state.ID
	responseChannel, release := s.Responses.CaptureVote(message.From, request.GetElection())
	defer release()
	if err := s.Runtime.Step(ctx, message); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	select {
	case response := <-responseChannel:
		return &raftkvv1.RequestVoteResponse{Term: response.Term, VoteGranted: response.VoteGranted, Election: response.Election}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
	}
}

func (s *PeerService) AppendEntries(ctx context.Context, request *raftkvv1.AppendEntriesRequest) (*raftkvv1.AppendEntriesResponse, error) {
	state, err := s.Runtime.State(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	message := raft.Message{
		From: raft.NodeID(request.GetLeaderId()), To: state.ID, Type: raft.MessageAppendEntries,
		AppendEntries: decodeAppendRequest(request),
	}
	responseChannel, release := s.Responses.CaptureAppend(message.From, request.GetRequestId())
	defer release()
	if err := s.Runtime.Step(ctx, message); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	select {
	case response := <-responseChannel:
		return &raftkvv1.AppendEntriesResponse{
			Term: response.Term, Success: response.Success, MatchIndex: response.MatchIndex,
			ConflictIndex: response.ConflictIndex, ConflictTerm: response.ConflictTerm, RequestId: response.RequestID,
		}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
	}
}

type KeyValueService struct {
	raftkvv1.UnimplementedKeyValueServer
	Runtime       *Runtime
	PeerAddresses map[raft.NodeID]string
	MaxWait       time.Duration
}

func (s *KeyValueService) Put(ctx context.Context, request *raftkvv1.PutRequest) (*raftkvv1.ClientResponse, error) {
	return s.execute(ctx, raft.Command{Operation: raft.OperationPut, ClientID: request.GetClientId(), Sequence: request.GetSequence(), Key: request.GetKey(), Value: request.GetValue()})
}

func (s *KeyValueService) Get(ctx context.Context, request *raftkvv1.GetRequest) (*raftkvv1.ClientResponse, error) {
	return s.execute(ctx, raft.Command{Operation: raft.OperationGet, ClientID: request.GetClientId(), Sequence: request.GetSequence(), Key: request.GetKey()})
}

func (s *KeyValueService) Delete(ctx context.Context, request *raftkvv1.DeleteRequest) (*raftkvv1.ClientResponse, error) {
	return s.execute(ctx, raft.Command{Operation: raft.OperationDelete, ClientID: request.GetClientId(), Sequence: request.GetSequence(), Key: request.GetKey()})
}

func (s *KeyValueService) execute(ctx context.Context, command raft.Command) (*raftkvv1.ClientResponse, error) {
	wait := s.MaxWait
	if wait <= 0 {
		wait = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	proposal, state, err := s.Runtime.Propose(ctx, command)
	if errors.Is(err, raft.ErrNotLeader) {
		return s.notLeader(state), nil
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	select {
	case <-proposal.Done():
		result, _ := proposal.Result()
		if result.Error == raft.ErrNotLeader.Error() {
			latest, _ := s.Runtime.State(context.Background())
			return s.notLeader(latest), nil
		}
		if result.Error != "" {
			return &raftkvv1.ClientResponse{Status: raftkvv1.ClientStatus_CLIENT_STATUS_FAILED, Error: result.Error, NodeId: string(state.ID), Term: state.Term}, nil
		}
		return &raftkvv1.ClientResponse{Status: raftkvv1.ClientStatus_CLIENT_STATUS_OK, Value: result.Value, Found: result.Found, NodeId: string(state.ID), Term: state.Term}, nil
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "request timed out; outcome is unknown and the same client ID/sequence must be retried")
	}
}

func (s *KeyValueService) notLeader(state raft.State) *raftkvv1.ClientResponse {
	return &raftkvv1.ClientResponse{
		Status: raftkvv1.ClientStatus_CLIENT_STATUS_NOT_LEADER,
		Error:  "not leader", LeaderHint: s.PeerAddresses[state.LeaderID], NodeId: string(state.ID), Term: state.Term,
	}
}

func decodeAppendRequest(request *raftkvv1.AppendEntriesRequest) raft.AppendEntriesRequest {
	entries := make([]raft.Entry, 0, len(request.GetEntries()))
	for _, entry := range request.GetEntries() {
		command := entry.GetCommand()
		entries = append(entries, raft.Entry{Index: entry.GetIndex(), Term: entry.GetTerm(), Command: raft.Command{
			Operation: decodeOperation(command.GetOperation()), ClientID: command.GetClientId(), Sequence: command.GetSequence(), Key: command.GetKey(), Value: command.GetValue(),
		}})
	}
	return raft.AppendEntriesRequest{
		Term: request.GetTerm(), LeaderID: raft.NodeID(request.GetLeaderId()), PrevLogIndex: request.GetPrevLogIndex(),
		PrevLogTerm: request.GetPrevLogTerm(), Entries: entries, LeaderCommit: request.GetLeaderCommit(), RequestID: request.GetRequestId(),
	}
}

func decodeOperation(operation raftkvv1.Operation) raft.Operation {
	switch operation {
	case raftkvv1.Operation_OPERATION_PUT:
		return raft.OperationPut
	case raftkvv1.Operation_OPERATION_DELETE:
		return raft.OperationDelete
	case raftkvv1.Operation_OPERATION_GET:
		return raft.OperationGet
	default:
		return raft.OperationNoop
	}
}
