package kv

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/yourusername/raftkv/internal/raft"
)

const DefaultDeduplicationClients = 1024

type clientRecord struct {
	sequence     uint64
	result       raft.Result
	appliedIndex uint64
}

// StateMachine is deterministic and intended to be driven only by committed
// Raft entries. The Raft owner serializes calls.
type StateMachine struct {
	values         map[string]string
	clients        map[string]clientRecord
	maxClients     int
	mutationCount  uint64
	appliedIndices []uint64
}

type snapshotImage struct {
	Values  map[string]string         `json:"values"`
	Clients map[string]snapshotClient `json:"clients"`
}

type snapshotClient struct {
	Sequence     uint64      `json:"sequence"`
	Result       raft.Result `json:"result"`
	AppliedIndex uint64      `json:"appliedIndex"`
}

func New(maxDeduplicationClients int) *StateMachine {
	if maxDeduplicationClients <= 0 {
		maxDeduplicationClients = DefaultDeduplicationClients
	}
	return &StateMachine{
		values:     make(map[string]string),
		clients:    make(map[string]clientRecord),
		maxClients: maxDeduplicationClients,
	}
}

func (s *StateMachine) Apply(entry raft.Entry) raft.Result {
	command := entry.Command
	if command.Operation == raft.OperationNoop {
		s.appliedIndices = append(s.appliedIndices, entry.Index)
		return raft.Result{}
	}
	if cached, ok := s.CachedResult(command.ClientID, command.Sequence); ok {
		s.appliedIndices = append(s.appliedIndices, entry.Index)
		return cached
	}

	result := raft.Result{}
	previous, knownClient := s.clients[command.ClientID]
	switch {
	case command.ClientID == "" || command.Sequence == 0:
		result.Error = "missing client request identity"
	case knownClient && command.Sequence != previous.sequence+1:
		result.Error = fmt.Sprintf("request sequence %d does not follow %d", command.Sequence, previous.sequence)
	case !knownClient && command.Sequence != 1:
		result.Error = fmt.Sprintf("first request sequence must be 1, got %d", command.Sequence)
	default:
		result = s.applyCommand(command)
		s.clients[command.ClientID] = clientRecord{
			sequence: command.Sequence, result: result, appliedIndex: entry.Index,
		}
		s.evictIfNeeded()
	}
	s.appliedIndices = append(s.appliedIndices, entry.Index)
	return result
}

func (s *StateMachine) applyCommand(command raft.Command) raft.Result {
	switch command.Operation {
	case raft.OperationPut:
		previous, found := s.values[command.Key]
		s.values[command.Key] = command.Value
		s.mutationCount++
		return raft.Result{Value: previous, Found: found}
	case raft.OperationDelete:
		previous, found := s.values[command.Key]
		if found {
			delete(s.values, command.Key)
			s.mutationCount++
		}
		return raft.Result{Value: previous, Found: found}
	case raft.OperationGet:
		value, found := s.values[command.Key]
		return raft.Result{Value: value, Found: found}
	default:
		return raft.Result{Error: "unknown operation"}
	}
}

func (s *StateMachine) CachedResult(clientID string, sequence uint64) (raft.Result, bool) {
	record, ok := s.clients[clientID]
	if !ok || record.sequence != sequence {
		return raft.Result{}, false
	}
	return record.result, true
}

func (s *StateMachine) Value(key string) (string, bool) {
	value, ok := s.values[key]
	return value, ok
}

func (s *StateMachine) MutationCount() uint64 { return s.mutationCount }

func (s *StateMachine) AppliedIndices() []uint64 {
	return append([]uint64(nil), s.appliedIndices...)
}

func (s *StateMachine) Snapshot() ([]byte, error) {
	image := snapshotImage{
		Values:  make(map[string]string, len(s.values)),
		Clients: make(map[string]snapshotClient, len(s.clients)),
	}
	for key, value := range s.values {
		image.Values[key] = value
	}
	for id, record := range s.clients {
		image.Clients[id] = snapshotClient{
			Sequence: record.sequence, Result: record.result, AppliedIndex: record.appliedIndex,
		}
	}
	return json.Marshal(image)
}

func (s *StateMachine) ValidateSnapshot(data []byte) error {
	_, err := decodeSnapshot(data)
	return err
}

func (s *StateMachine) RestoreSnapshot(data []byte) error {
	image, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	values := make(map[string]string, len(image.Values))
	for key, value := range image.Values {
		values[key] = value
	}
	clients := make(map[string]clientRecord, len(image.Clients))
	for id, record := range image.Clients {
		clients[id] = clientRecord{sequence: record.Sequence, result: record.Result, appliedIndex: record.AppliedIndex}
	}
	s.values = values
	s.clients = clients
	s.mutationCount = 0
	s.appliedIndices = nil
	return nil
}

func decodeSnapshot(data []byte) (snapshotImage, error) {
	var image snapshotImage
	if err := json.Unmarshal(data, &image); err != nil {
		return snapshotImage{}, fmt.Errorf("decode KV snapshot: %w", err)
	}
	if image.Values == nil || image.Clients == nil {
		return snapshotImage{}, fmt.Errorf("decode KV snapshot: missing values or clients")
	}
	for id, record := range image.Clients {
		if id == "" || record.Sequence == 0 {
			return snapshotImage{}, fmt.Errorf("decode KV snapshot: invalid client record")
		}
	}
	return image, nil
}

func (s *StateMachine) evictIfNeeded() {
	if len(s.clients) <= s.maxClients {
		return
	}
	ids := make([]string, 0, len(s.clients))
	for id := range s.clients {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	evict := ids[0]
	for _, id := range ids[1:] {
		if s.clients[id].appliedIndex < s.clients[evict].appliedIndex {
			evict = id
		}
	}
	delete(s.clients, evict)
}
