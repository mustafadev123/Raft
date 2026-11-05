package raft

import (
	"context"
	"fmt"
	"time"
)

// Node is a caller-serialized Raft election state machine. Tick and Step must
// be called serially, which keeps networking, clocks, and locking out of the
// consensus core.
type Node struct {
	id        NodeID
	members   []NodeID
	memberSet map[NodeID]struct{}
	majority  int

	role     Role
	term     uint64
	votedFor NodeID
	lastLog  LogPosition

	timeouts          TimeoutSource
	heartbeatInterval time.Duration
	transport         Transport
	electionDeadline  time.Time
	heartbeatDue      time.Time

	election       uint64
	electionCtx    context.Context
	cancelElection context.CancelFunc
	votes          map[NodeID]struct{}
}

func NewNode(config Config, now time.Time) (*Node, error) {
	if config.ID == "" || config.Transport == nil || config.Timeouts == nil || config.HeartbeatInterval <= 0 {
		return nil, ErrInvalidConfig
	}

	members := append([]NodeID(nil), config.Members...)
	memberSet := make(map[NodeID]struct{}, len(members))
	for _, id := range members {
		if id == "" {
			return nil, ErrInvalidConfig
		}
		if _, exists := memberSet[id]; exists {
			return nil, fmt.Errorf("%w: duplicate member %q", ErrInvalidConfig, id)
		}
		memberSet[id] = struct{}{}
	}
	if _, exists := memberSet[config.ID]; !exists || len(members) == 0 {
		return nil, fmt.Errorf("%w: local node must be a member", ErrInvalidConfig)
	}

	n := &Node{
		id:                config.ID,
		members:           members,
		memberSet:         memberSet,
		majority:          len(members)/2 + 1,
		role:              Follower,
		lastLog:           config.LastLog,
		timeouts:          config.Timeouts,
		heartbeatInterval: config.HeartbeatInterval,
		transport:         config.Transport,
	}
	n.resetElectionDeadline(now)
	return n, nil
}

func (n *Node) State() State {
	return State{
		ID:               n.id,
		Role:             n.role,
		Term:             n.term,
		VotedFor:         n.votedFor,
		LastLog:          n.lastLog,
		Election:         n.election,
		ElectionDeadline: n.electionDeadline,
		HeartbeatDue:     n.heartbeatDue,
		Votes:            len(n.votes),
	}
}

// Tick advances time from the core's perspective. The caller controls time.
func (n *Node) Tick(now time.Time) {
	if n.role == Leader {
		if !now.Before(n.heartbeatDue) {
			n.sendHeartbeats()
			n.heartbeatDue = now.Add(n.heartbeatInterval)
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElection(now)
	}
}

// Step processes one inbound message. Calls to Step and Tick are serialized by
// the owner of the node.
func (n *Node) Step(message Message, now time.Time) error {
	if _, known := n.memberSet[message.From]; !known || message.To != n.id {
		return ErrUnknownSender
	}

	switch message.Type {
	case MessageRequestVote:
		n.handleRequestVote(message, now)
	case MessageRequestVoteResponse:
		n.handleRequestVoteResponse(message, now)
	case MessageHeartbeat:
		n.handleHeartbeat(message, now)
	case MessageHeartbeatResponse:
		n.handleHeartbeatResponse(message, now)
	default:
		return fmt.Errorf("unknown message type %d", message.Type)
	}
	return nil
}

func (n *Node) startElection(now time.Time) {
	n.cancelCurrentElection()
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.election++
	n.votes = map[NodeID]struct{}{n.id: {}}
	n.electionCtx, n.cancelElection = context.WithCancel(context.Background())
	n.resetElectionDeadline(now)

	if len(n.votes) >= n.majority {
		n.becomeLeader(now)
		return
	}

	request := RequestVoteRequest{
		Term:        n.term,
		CandidateID: n.id,
		LastLog:     n.lastLog,
		Election:    n.election,
	}
	for _, peer := range n.members {
		if peer == n.id {
			continue
		}
		_ = n.transport.Send(n.electionCtx, Message{
			From: n.id, To: peer, Type: MessageRequestVote, RequestVote: request,
		})
	}
}

func (n *Node) handleRequestVote(message Message, now time.Time) {
	request := message.RequestVote
	if request.Term > n.term {
		n.adoptHigherTerm(request.Term)
	}

	granted := false
	if request.Term == n.term && request.CandidateID == message.From &&
		(n.votedFor == "" || n.votedFor == request.CandidateID) &&
		isAtLeastAsUpToDate(request.LastLog, n.lastLog) {
		granted = true
		n.votedFor = request.CandidateID
		n.resetElectionDeadline(now)
	}

	_ = n.transport.Send(context.Background(), Message{
		From: n.id, To: message.From, Type: MessageRequestVoteResponse,
		RequestVoteResponse: RequestVoteResponse{
			Term: n.term, VoteGranted: granted, Election: request.Election,
		},
	})
}

func (n *Node) handleRequestVoteResponse(message Message, now time.Time) {
	response := message.RequestVoteResponse
	if response.Term > n.term {
		n.adoptHigherTerm(response.Term)
		return
	}
	if n.role != Candidate || response.Term != n.term || response.Election != n.election || !response.VoteGranted {
		return
	}

	n.votes[message.From] = struct{}{}
	if len(n.votes) >= n.majority {
		n.becomeLeader(now)
	}
}

func (n *Node) handleHeartbeat(message Message, now time.Time) {
	heartbeat := message.Heartbeat
	accepted := heartbeat.LeaderID == message.From && heartbeat.Term >= n.term
	if heartbeat.Term > n.term {
		n.adoptHigherTerm(heartbeat.Term)
	}
	if accepted {
		if n.role != Follower {
			n.cancelCurrentElection()
			n.role = Follower
			n.votes = nil
		}
		n.resetElectionDeadline(now)
	}

	_ = n.transport.Send(context.Background(), Message{
		From: n.id, To: message.From, Type: MessageHeartbeatResponse,
		HeartbeatResponse: HeartbeatResponse{Term: n.term},
	})
}

func (n *Node) handleHeartbeatResponse(message Message, _ time.Time) {
	if message.HeartbeatResponse.Term > n.term {
		n.adoptHigherTerm(message.HeartbeatResponse.Term)
	}
}

func (n *Node) becomeLeader(now time.Time) {
	n.cancelCurrentElection()
	n.role = Leader
	n.votes = nil
	n.sendHeartbeats()
	n.heartbeatDue = now.Add(n.heartbeatInterval)
}

func (n *Node) adoptHigherTerm(term uint64) {
	n.cancelCurrentElection()
	n.term = term
	n.role = Follower
	n.votedFor = ""
	n.votes = nil
}

func (n *Node) sendHeartbeats() {
	heartbeat := Heartbeat{Term: n.term, LeaderID: n.id}
	for _, peer := range n.members {
		if peer == n.id {
			continue
		}
		_ = n.transport.Send(context.Background(), Message{
			From: n.id, To: peer, Type: MessageHeartbeat, Heartbeat: heartbeat,
		})
	}
}

func (n *Node) cancelCurrentElection() {
	if n.cancelElection != nil {
		n.cancelElection()
		n.cancelElection = nil
		n.electionCtx = nil
	}
}

func (n *Node) resetElectionDeadline(now time.Time) {
	timeout := n.timeouts.ElectionTimeout()
	if timeout <= 0 {
		panic("raft: election timeout must be positive")
	}
	n.electionDeadline = now.Add(timeout)
}

func isAtLeastAsUpToDate(candidate, local LogPosition) bool {
	if candidate.Term != local.Term {
		return candidate.Term > local.Term
	}
	return candidate.Index >= local.Index
}
