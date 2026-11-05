package raft

import (
	"context"
	"fmt"
	"time"
)

// Node is a caller-serialized Raft state machine. Tick, Step, and Propose must
// be called serially by its owner.
type Node struct {
	id        NodeID
	members   []NodeID
	memberSet map[NodeID]struct{}
	majority  int
	role      Role
	term      uint64
	votedFor  NodeID
	lastLog   LogPosition
	log       []Entry

	timeouts          TimeoutSource
	heartbeatInterval time.Duration
	transport         Transport
	stateMachine      StateMachine
	store             StableStore
	storageErr        error
	electionDeadline  time.Time
	heartbeatDue      time.Time

	election       uint64
	electionCtx    context.Context
	cancelElection context.CancelFunc
	votes          map[NodeID]struct{}

	leadershipCtx    context.Context
	cancelLeadership context.CancelFunc
	nextIndex        map[NodeID]uint64
	matchIndex       map[NodeID]uint64
	lastRequest      map[NodeID]uint64
	requestSequence  uint64
	commitIndex      uint64
	lastApplied      uint64
	pending          map[string]pendingRequest
}

type pendingRequest struct {
	sequence uint64
	proposal *Proposal
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

	logCopy := cloneEntries(config.InitialLog)
	currentTerm := config.InitialTerm
	votedFor := NodeID("")
	commitIndex := uint64(0)
	if config.Store != nil {
		stored, found, err := config.Store.Load()
		if err != nil {
			return nil, fmt.Errorf("load raft state: %w", err)
		}
		if found {
			if len(config.InitialLog) != 0 {
				return nil, fmt.Errorf("%w: InitialLog cannot be combined with stored state", ErrInvalidConfig)
			}
			logCopy = cloneEntries(stored.Log)
			currentTerm = stored.CurrentTerm
			votedFor = stored.VotedFor
			commitIndex = stored.CommitIndex
			if votedFor != "" {
				if _, known := memberSet[votedFor]; !known {
					return nil, fmt.Errorf("%w: stored vote for unknown member %q", ErrInvalidConfig, votedFor)
				}
			}
		}
	}
	for i, entry := range logCopy {
		if entry.Index != uint64(i+1) || entry.Term == 0 || entry.Term > currentTerm {
			return nil, fmt.Errorf("%w: initial log must be contiguous and no newer than current term", ErrInvalidConfig)
		}
	}
	if commitIndex > uint64(len(logCopy)) {
		return nil, fmt.Errorf("%w: stored commit index exceeds log", ErrInvalidConfig)
	}
	lastLog := LogPosition{}
	if len(logCopy) > 0 {
		last := logCopy[len(logCopy)-1]
		lastLog = LogPosition{Index: last.Index, Term: last.Term}
	}

	n := &Node{
		id: config.ID, members: members, memberSet: memberSet,
		majority: len(members)/2 + 1, role: Follower, term: currentTerm, votedFor: votedFor,
		lastLog: lastLog, log: logCopy, commitIndex: commitIndex,
		timeouts: config.Timeouts, heartbeatInterval: config.HeartbeatInterval,
		transport: config.Transport, stateMachine: config.StateMachine, store: config.Store,
		pending: make(map[string]pendingRequest),
	}
	n.resetElectionDeadline(now)
	n.applyCommitted()
	return n, nil
}

func (n *Node) State() State {
	return State{
		ID: n.id, Role: n.role, Term: n.term, VotedFor: n.votedFor, LastLog: n.lastLog,
		Election: n.election, ElectionDeadline: n.electionDeadline, HeartbeatDue: n.heartbeatDue,
		Votes: len(n.votes), CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		Log: cloneEntries(n.log), NextIndex: copyProgress(n.nextIndex), MatchIndex: copyProgress(n.matchIndex),
	}
}

// Propose appends a client operation. Its Proposal completes only after the
// entry is committed and applied by this leader.
func (n *Node) Propose(command Command) (*Proposal, error) {
	if err := n.ensureHealthy(); err != nil {
		return nil, err
	}
	if n.role != Leader {
		return nil, ErrNotLeader
	}
	if command.Operation == OperationNoop || command.ClientID == "" || command.Sequence == 0 {
		return nil, ErrInvalidCommand
	}
	if cached, ok := n.cachedResult(command.ClientID, command.Sequence); ok {
		return completedProposal(cached), nil
	}
	if pending, ok := n.pending[command.ClientID]; ok {
		if pending.sequence == command.Sequence {
			return pending.proposal, nil
		}
		return nil, ErrRequestPending
	}
	proposal := newProposal()
	n.pending[command.ClientID] = pendingRequest{sequence: command.Sequence, proposal: proposal}
	n.appendLocal(command)
	if err := n.persist(); err != nil {
		return nil, err
	}
	n.replicateAll()
	if err := n.advanceCommit(); err != nil {
		return nil, err
	}
	return proposal, nil
}

func (n *Node) Tick(now time.Time) error {
	if err := n.ensureHealthy(); err != nil {
		return err
	}
	if n.role == Leader {
		if !now.Before(n.heartbeatDue) {
			n.replicateAll()
			n.heartbeatDue = now.Add(n.heartbeatInterval)
		}
		return nil
	}
	if !now.Before(n.electionDeadline) {
		return n.startElection(now)
	}
	return nil
}

func (n *Node) Step(message Message, now time.Time) error {
	if err := n.ensureHealthy(); err != nil {
		return err
	}
	if _, known := n.memberSet[message.From]; !known || message.To != n.id {
		return ErrUnknownSender
	}
	switch message.Type {
	case MessageRequestVote:
		return n.handleRequestVote(message, now)
	case MessageRequestVoteResponse:
		return n.handleRequestVoteResponse(message, now)
	case MessageAppendEntries:
		return n.handleAppendEntries(message, now)
	case MessageAppendEntriesResponse:
		return n.handleAppendEntriesResponse(message)
	default:
		return fmt.Errorf("unknown message type %d", message.Type)
	}
}

func (n *Node) startElection(now time.Time) error {
	n.cancelCurrentElection()
	n.cancelCurrentLeadership(Result{Error: ErrNotLeader.Error()})
	n.role = Candidate
	n.term++
	n.votedFor = n.id
	n.election++
	n.votes = map[NodeID]struct{}{n.id: {}}
	n.electionCtx, n.cancelElection = context.WithCancel(context.Background())
	n.resetElectionDeadline(now)
	if err := n.persist(); err != nil {
		return err
	}
	if len(n.votes) >= n.majority {
		return n.becomeLeader(now)
	}
	request := RequestVoteRequest{Term: n.term, CandidateID: n.id, LastLog: n.lastLog, Election: n.election}
	for _, peer := range n.members {
		if peer != n.id {
			_ = n.transport.Send(n.electionCtx, Message{From: n.id, To: peer, Type: MessageRequestVote, RequestVote: request})
		}
	}
	return nil
}

func (n *Node) handleRequestVote(message Message, now time.Time) error {
	request := message.RequestVote
	if request.Term > n.term {
		if err := n.adoptHigherTerm(request.Term); err != nil {
			return err
		}
	}
	granted := false
	if request.Term == n.term && request.CandidateID == message.From &&
		(n.votedFor == "" || n.votedFor == request.CandidateID) && isAtLeastAsUpToDate(request.LastLog, n.lastLog) {
		granted = true
		n.votedFor = request.CandidateID
		n.resetElectionDeadline(now)
		if err := n.persist(); err != nil {
			return err
		}
	}
	_ = n.transport.Send(context.Background(), Message{
		From: n.id, To: message.From, Type: MessageRequestVoteResponse,
		RequestVoteResponse: RequestVoteResponse{Term: n.term, VoteGranted: granted, Election: request.Election},
	})
	return nil
}

func (n *Node) handleRequestVoteResponse(message Message, now time.Time) error {
	response := message.RequestVoteResponse
	if response.Term > n.term {
		return n.adoptHigherTerm(response.Term)
	}
	if n.role != Candidate || response.Term != n.term || response.Election != n.election || !response.VoteGranted {
		return nil
	}
	n.votes[message.From] = struct{}{}
	if len(n.votes) >= n.majority {
		return n.becomeLeader(now)
	}
	return nil
}

func (n *Node) handleAppendEntries(message Message, now time.Time) error {
	request := message.AppendEntries
	if request.Term > n.term {
		if err := n.adoptHigherTerm(request.Term); err != nil {
			return err
		}
	}
	response := AppendEntriesResponse{Term: n.term, RequestID: request.RequestID}
	if request.Term < n.term || request.LeaderID != message.From {
		n.sendAppendResponse(message.From, response)
		return nil
	}
	n.becomeFollowerSameTerm()
	n.resetElectionDeadline(now)

	if request.PrevLogIndex > uint64(len(n.log)) {
		response.ConflictIndex = uint64(len(n.log)) + 1
		n.sendAppendResponse(message.From, response)
		return nil
	}
	if request.PrevLogIndex > 0 && n.termAt(request.PrevLogIndex) != request.PrevLogTerm {
		response.ConflictTerm = n.termAt(request.PrevLogIndex)
		response.ConflictIndex = n.firstIndexOfTerm(response.ConflictTerm)
		n.sendAppendResponse(message.From, response)
		return nil
	}

	persistentChanged := false
	for i, incoming := range request.Entries {
		index := request.PrevLogIndex + uint64(i) + 1
		if incoming.Index != index {
			response.ConflictIndex = index
			n.sendAppendResponse(message.From, response)
			return nil
		}
		if index <= uint64(len(n.log)) && n.termAt(index) != incoming.Term {
			if index <= n.commitIndex {
				response.ConflictIndex = n.commitIndex + 1
				n.sendAppendResponse(message.From, response)
				return nil
			}
			n.log = n.log[:index-1]
			persistentChanged = true
		}
		if index > uint64(len(n.log)) {
			n.log = append(n.log, cloneEntries(request.Entries[i:])...)
			persistentChanged = true
			break
		}
	}
	n.updateLastLog()
	if request.LeaderCommit > n.commitIndex {
		n.commitIndex = min(request.LeaderCommit, uint64(len(n.log)))
		persistentChanged = true
	}
	if persistentChanged {
		if err := n.persist(); err != nil {
			return err
		}
		n.applyCommitted()
	}
	response.Success = true
	response.MatchIndex = request.PrevLogIndex + uint64(len(request.Entries))
	n.sendAppendResponse(message.From, response)
	return nil
}

func (n *Node) handleAppendEntriesResponse(message Message) error {
	response := message.AppendEntriesResponse
	if response.Term > n.term {
		return n.adoptHigherTerm(response.Term)
	}
	if n.role != Leader || response.Term != n.term {
		return nil
	}
	peer := message.From
	if response.Success {
		if response.MatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = response.MatchIndex
			n.nextIndex[peer] = response.MatchIndex + 1
		}
		return n.advanceCommit()
	}
	if response.RequestID != n.lastRequest[peer] {
		return nil
	}
	next := response.ConflictIndex
	if response.ConflictTerm != 0 {
		if last := n.lastIndexOfTerm(response.ConflictTerm); last != 0 {
			next = last + 1
		}
	}
	if next == 0 {
		next = 1
	}
	if ceiling := uint64(len(n.log)) + 1; next > ceiling {
		next = ceiling
	}
	n.nextIndex[peer] = next
	n.sendAppend(peer)
	return nil
}

func (n *Node) becomeLeader(now time.Time) error {
	n.cancelCurrentElection()
	n.role = Leader
	n.votes = nil
	n.leadershipCtx, n.cancelLeadership = context.WithCancel(context.Background())
	n.nextIndex = make(map[NodeID]uint64, len(n.members))
	n.matchIndex = make(map[NodeID]uint64, len(n.members))
	n.lastRequest = make(map[NodeID]uint64, len(n.members))
	for _, peer := range n.members {
		n.nextIndex[peer] = uint64(len(n.log)) + 1
	}
	n.appendLocal(Command{Operation: OperationNoop})
	if err := n.persist(); err != nil {
		return err
	}
	n.replicateAll()
	if err := n.advanceCommit(); err != nil {
		return err
	}
	n.heartbeatDue = now.Add(n.heartbeatInterval)
	return nil
}

func (n *Node) becomeFollowerSameTerm() {
	if n.role != Follower {
		n.cancelCurrentElection()
		n.cancelCurrentLeadership(Result{Error: ErrNotLeader.Error()})
		n.role = Follower
		n.votes = nil
	}
}

func (n *Node) adoptHigherTerm(term uint64) error {
	n.cancelCurrentElection()
	n.cancelCurrentLeadership(Result{Error: ErrNotLeader.Error()})
	n.term = term
	n.role = Follower
	n.votedFor = ""
	n.votes = nil
	return n.persist()
}

func (n *Node) appendLocal(command Command) Entry {
	entry := Entry{Index: uint64(len(n.log)) + 1, Term: n.term, Command: command}
	n.log = append(n.log, entry)
	n.lastLog = LogPosition{Index: entry.Index, Term: entry.Term}
	if n.matchIndex != nil {
		n.matchIndex[n.id] = entry.Index
		n.nextIndex[n.id] = entry.Index + 1
	}
	return entry
}

func (n *Node) replicateAll() {
	for _, peer := range n.members {
		if peer != n.id {
			n.sendAppend(peer)
		}
	}
}

func (n *Node) sendAppend(peer NodeID) {
	next := n.nextIndex[peer]
	if next == 0 {
		next = uint64(len(n.log)) + 1
	}
	prev := next - 1
	n.requestSequence++
	requestID := n.requestSequence
	n.lastRequest[peer] = requestID
	request := AppendEntriesRequest{
		Term: n.term, LeaderID: n.id, PrevLogIndex: prev, PrevLogTerm: n.termAt(prev),
		Entries: cloneEntries(n.log[prev:]), LeaderCommit: n.commitIndex, RequestID: requestID,
	}
	ctx := n.leadershipCtx
	if ctx == nil {
		ctx = context.Background()
	}
	_ = n.transport.Send(ctx, Message{From: n.id, To: peer, Type: MessageAppendEntries, AppendEntries: request})
}

func (n *Node) sendAppendResponse(peer NodeID, response AppendEntriesResponse) {
	_ = n.transport.Send(context.Background(), Message{
		From: n.id, To: peer, Type: MessageAppendEntriesResponse, AppendEntriesResponse: response,
	})
}

func (n *Node) advanceCommit() error {
	if n.role != Leader {
		return nil
	}
	for index := uint64(len(n.log)); index > n.commitIndex; index-- {
		if n.termAt(index) != n.term {
			continue
		}
		matched := 0
		for _, peer := range n.members {
			if n.matchIndex[peer] >= index {
				matched++
			}
		}
		if matched >= n.majority {
			n.commitIndex = index
			if err := n.persist(); err != nil {
				return err
			}
			n.applyCommitted()
			n.replicateAll()
			return nil
		}
	}
	return nil
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		result := Result{}
		if n.stateMachine != nil {
			result = n.stateMachine.Apply(entry)
		}
		command := entry.Command
		if command.Operation == OperationNoop || command.ClientID == "" {
			continue
		}
		if pending, ok := n.pending[command.ClientID]; ok && pending.sequence == command.Sequence {
			pending.proposal.complete(result)
			delete(n.pending, command.ClientID)
		}
	}
}

func (n *Node) cachedResult(clientID string, sequence uint64) (Result, bool) {
	if n.stateMachine == nil {
		return Result{}, false
	}
	return n.stateMachine.CachedResult(clientID, sequence)
}

func (n *Node) cancelCurrentElection() {
	if n.cancelElection != nil {
		n.cancelElection()
		n.cancelElection = nil
		n.electionCtx = nil
	}
}

func (n *Node) cancelCurrentLeadership(result Result) {
	if n.cancelLeadership != nil {
		n.cancelLeadership()
		n.cancelLeadership = nil
		n.leadershipCtx = nil
	}
	for clientID, pending := range n.pending {
		pending.proposal.complete(result)
		delete(n.pending, clientID)
	}
}

func (n *Node) ensureHealthy() error {
	return n.storageErr
}

func (n *Node) persist() error {
	if n.store == nil {
		return nil
	}
	state := PersistentState{
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		Log:         cloneEntries(n.log),
		CommitIndex: n.commitIndex,
	}
	if err := n.store.Save(state); err != nil {
		n.storageErr = fmt.Errorf("%w: %v", ErrStorageFault, err)
		n.cancelCurrentElection()
		n.cancelCurrentLeadership(Result{Error: n.storageErr.Error()})
		return n.storageErr
	}
	return nil
}

func (n *Node) resetElectionDeadline(now time.Time) {
	timeout := n.timeouts.ElectionTimeout()
	if timeout <= 0 {
		panic("raft: election timeout must be positive")
	}
	n.electionDeadline = now.Add(timeout)
}

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 || index > uint64(len(n.log)) {
		return 0
	}
	return n.log[index-1].Term
}

func (n *Node) firstIndexOfTerm(term uint64) uint64 {
	for _, entry := range n.log {
		if entry.Term == term {
			return entry.Index
		}
	}
	return 0
}

func (n *Node) lastIndexOfTerm(term uint64) uint64 {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			return n.log[i].Index
		}
	}
	return 0
}

func (n *Node) updateLastLog() {
	if len(n.log) == 0 {
		n.lastLog = LogPosition{}
		return
	}
	last := n.log[len(n.log)-1]
	n.lastLog = LogPosition{Index: last.Index, Term: last.Term}
}

func isAtLeastAsUpToDate(candidate, local LogPosition) bool {
	if candidate.Term != local.Term {
		return candidate.Term > local.Term
	}
	return candidate.Index >= local.Index
}

func cloneEntries(entries []Entry) []Entry { return append([]Entry(nil), entries...) }

func copyProgress(source map[NodeID]uint64) map[NodeID]uint64 {
	if source == nil {
		return nil
	}
	result := make(map[NodeID]uint64, len(source))
	for id, index := range source {
		result[id] = index
	}
	return result
}
