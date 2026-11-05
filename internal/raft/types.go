package raft

import (
	"context"
	"errors"
	"time"
)

// NodeID identifies a member of a fixed Raft cluster.
type NodeID string

// Role is a node's current election role.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogPosition describes the last entry known to a node. Log replication is
// intentionally outside this stage; elections need only this position.
type LogPosition struct {
	Index uint64
	Term  uint64
}

type RequestVoteRequest struct {
	Term        uint64
	CandidateID NodeID
	LastLog     LogPosition
	Election    uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
	Election    uint64
}

// Heartbeat is the election-only subset of AppendEntries. Log replication
// will replace this with the complete RPC in a later stage.
type Heartbeat struct {
	Term     uint64
	LeaderID NodeID
}

type HeartbeatResponse struct {
	Term uint64
}

type MessageType uint8

const (
	MessageRequestVote MessageType = iota
	MessageRequestVoteResponse
	MessageHeartbeat
	MessageHeartbeatResponse
)

// Message is the transport-neutral election envelope.
type Message struct {
	From NodeID
	To   NodeID
	Type MessageType

	RequestVote         RequestVoteRequest
	RequestVoteResponse RequestVoteResponse
	Heartbeat           Heartbeat
	HeartbeatResponse   HeartbeatResponse
}

// Transport accepts outbound messages. Implementations must honor ctx: it is
// canceled when the election that produced a request is no longer current.
type Transport interface {
	Send(ctx context.Context, message Message) error
}

// TimeoutSource supplies a fresh randomized timeout for each election timer
// reset. Production code can use RandomTimeout; tests can inject a sequence.
type TimeoutSource interface {
	ElectionTimeout() time.Duration
}

// Int63nSource is the subset of math/rand.Rand used by RandomTimeout.
type Int63nSource interface {
	Int63n(n int64) int64
}

type RandomTimeout struct {
	Min    time.Duration
	Max    time.Duration
	Source Int63nSource
}

func (r RandomTimeout) ElectionTimeout() time.Duration {
	if r.Source == nil || r.Max <= r.Min {
		return r.Min
	}
	return r.Min + time.Duration(r.Source.Int63n(int64(r.Max-r.Min)))
}

type Config struct {
	ID                NodeID
	Members           []NodeID
	LastLog           LogPosition
	HeartbeatInterval time.Duration
	Timeouts          TimeoutSource
	Transport         Transport
}

var (
	ErrInvalidConfig = errors.New("invalid raft configuration")
	ErrUnknownSender = errors.New("message sender is not a cluster member")
)

// State is a read-only snapshot intended for observation and tests.
type State struct {
	ID               NodeID
	Role             Role
	Term             uint64
	VotedFor         NodeID
	LastLog          LogPosition
	Election         uint64
	ElectionDeadline time.Time
	HeartbeatDue     time.Time
	Votes            int
}
