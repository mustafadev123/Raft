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

// LogPosition describes the last entry known to a node for vote freshness.
type LogPosition struct {
	Index uint64
	Term  uint64
}

type Operation uint8

const (
	OperationNoop Operation = iota
	OperationPut
	OperationDelete
	OperationGet
)

// Command is replicated as part of a log entry. ClientID and Sequence define
// the bounded retry identity used by the state machine.
type Command struct {
	Operation Operation
	ClientID  string
	Sequence  uint64
	Key       string
	Value     string
}

type Entry struct {
	Index   uint64
	Term    uint64
	Command Command
}

type Result struct {
	Value string
	Found bool
	Error string
}

// StateMachine applies committed entries and optionally returns a cached
// result for a previously applied client request.
type StateMachine interface {
	Apply(entry Entry) Result
	CachedResult(clientID string, sequence uint64) (Result, bool)
	Snapshot() ([]byte, error)
	ValidateSnapshot(data []byte) error
	RestoreSnapshot(data []byte) error
}

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// PersistentState is the safety-critical state that must survive restart.
// CommitIndex is included so recovery applies only the known committed prefix.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID
	Log         []Entry
	CommitIndex uint64
	Snapshot    Snapshot
}

// StableStore is implemented by the storage package. Save must not return nil
// until the supplied state has reached the implementation's durability
// boundary.
type StableStore interface {
	Load() (state PersistentState, found bool, err error)
	Save(state PersistentState) error
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

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	RequestID    uint64
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
	RequestID     uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	RequestID         uint64
}

type InstallSnapshotResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
	RequestID  uint64
}

type MessageType uint8

const (
	MessageRequestVote MessageType = iota
	MessageRequestVoteResponse
	MessageAppendEntries
	MessageAppendEntriesResponse
	MessageInstallSnapshot
	MessageInstallSnapshotResponse
)

// Message is the transport-neutral election envelope.
type Message struct {
	From NodeID
	To   NodeID
	Type MessageType

	RequestVote             RequestVoteRequest
	RequestVoteResponse     RequestVoteResponse
	AppendEntries           AppendEntriesRequest
	AppendEntriesResponse   AppendEntriesResponse
	InstallSnapshot         InstallSnapshotRequest
	InstallSnapshotResponse InstallSnapshotResponse
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
	HeartbeatInterval time.Duration
	Timeouts          TimeoutSource
	Transport         Transport
	StateMachine      StateMachine
	Store             StableStore
	InitialTerm       uint64
	InitialLog        []Entry
	SnapshotThreshold uint64
	MaxSnapshotBytes  int
}

var (
	ErrInvalidConfig = errors.New("invalid raft configuration")
	ErrUnknownSender = errors.New("message sender is not a cluster member")
)

// State is a read-only snapshot intended for observation and tests.
type State struct {
	ID                NodeID
	Role              Role
	Term              uint64
	VotedFor          NodeID
	LeaderID          NodeID
	LastLog           LogPosition
	Election          uint64
	ElectionDeadline  time.Time
	HeartbeatDue      time.Time
	Votes             int
	CommitIndex       uint64
	LastApplied       uint64
	Log               []Entry
	NextIndex         map[NodeID]uint64
	MatchIndex        map[NodeID]uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

var (
	ErrNotLeader        = errors.New("not raft leader")
	ErrInvalidCommand   = errors.New("invalid client command")
	ErrRequestPending   = errors.New("a different request is already pending for this client")
	ErrStorageFault     = errors.New("raft storage fault")
	ErrSnapshotTooLarge = errors.New("snapshot exceeds configured size limit")
)

// Proposal completes only after its entry is committed and applied, or when
// leadership is lost before completion.
type Proposal struct {
	done   chan struct{}
	result Result
}

func newProposal() *Proposal { return &Proposal{done: make(chan struct{})} }

func completedProposal(result Result) *Proposal {
	p := newProposal()
	p.complete(result)
	return p
}

func (p *Proposal) Done() <-chan struct{} { return p.done }

func (p *Proposal) Result() (Result, bool) {
	select {
	case <-p.done:
		return p.result, true
	default:
		return Result{}, false
	}
}

func (p *Proposal) complete(result Result) {
	select {
	case <-p.done:
		return
	default:
		p.result = result
		close(p.done)
	}
}
