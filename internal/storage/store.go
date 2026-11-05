package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/yourusername/raftkv/internal/raft"
)

const formatVersion = 1

var ErrCorrupt = errors.New("corrupt raft state")

type CrashPoint string

const (
	BeforeTempWrite CrashPoint = "before-temp-write"
	AfterTempWrite  CrashPoint = "after-temp-write"
	AfterTempSync   CrashPoint = "after-temp-sync"
	AfterReplace    CrashPoint = "after-atomic-replace"
)

type envelope struct {
	Version  int             `json:"version"`
	Payload  json.RawMessage `json:"payload"`
	Checksum string          `json:"sha256"`
}

// FileStore rewrites a checksummed complete-state file on every Save. Hook is
// intended for deterministic crash/failure injection in tests.
type FileStore struct {
	Path string
	Hook func(CrashPoint) error
}

func (s *FileStore) Load() (raft.PersistentState, bool, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.PersistentState{}, false, nil
	}
	if err != nil {
		return raft.PersistentState{}, false, fmt.Errorf("read raft state: %w", err)
	}
	var wrapper envelope
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return raft.PersistentState{}, false, fmt.Errorf("%w: decode envelope: %v", ErrCorrupt, err)
	}
	if wrapper.Version != formatVersion {
		return raft.PersistentState{}, false, fmt.Errorf("%w: unsupported version %d", ErrCorrupt, wrapper.Version)
	}
	sum := sha256.Sum256(wrapper.Payload)
	if wrapper.Checksum != hex.EncodeToString(sum[:]) {
		return raft.PersistentState{}, false, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	var state raft.PersistentState
	if err := json.Unmarshal(wrapper.Payload, &state); err != nil {
		return raft.PersistentState{}, false, fmt.Errorf("%w: decode payload: %v", ErrCorrupt, err)
	}
	if err := validate(state); err != nil {
		return raft.PersistentState{}, false, err
	}
	state.Log = cloneLog(state.Log)
	return state, true, nil
}

func (s *FileStore) Save(state raft.PersistentState) error {
	if s.Path == "" {
		return errors.New("raft state path is empty")
	}
	if err := validate(state); err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode raft state: %w", err)
	}
	sum := sha256.Sum256(payload)
	data, err := json.Marshal(envelope{
		Version: formatVersion, Payload: payload, Checksum: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return fmt.Errorf("encode raft envelope: %w", err)
	}
	data = append(data, '\n')

	directory := filepath.Dir(s.Path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create raft state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary raft state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := s.inject(BeforeTempWrite); err != nil {
		temporary.Close()
		return err
	}
	written, err := temporary.Write(data)
	if err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary raft state: %w", err)
	}
	if written != len(data) {
		temporary.Close()
		return fmt.Errorf("write temporary raft state: short write %d of %d bytes", written, len(data))
	}
	if err := s.inject(AfterTempWrite); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync temporary raft state: %w", err)
	}
	if err := s.inject(AfterTempSync); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary raft state: %w", err)
	}
	if err := atomicReplace(temporaryPath, s.Path); err != nil {
		return fmt.Errorf("replace raft state: %w", err)
	}
	if err := s.inject(AfterReplace); err != nil {
		return err
	}
	return nil
}

func (s *FileStore) inject(point CrashPoint) error {
	if s.Hook == nil {
		return nil
	}
	if err := s.Hook(point); err != nil {
		return fmt.Errorf("storage crash point %s: %w", point, err)
	}
	return nil
}

func validate(state raft.PersistentState) error {
	snapshot := state.Snapshot
	if snapshot.LastIncludedIndex == 0 {
		if snapshot.LastIncludedTerm != 0 || len(snapshot.Data) != 0 {
			return fmt.Errorf("%w: invalid empty snapshot boundary", ErrCorrupt)
		}
	} else if snapshot.LastIncludedTerm == 0 || snapshot.LastIncludedTerm > state.CurrentTerm || len(snapshot.Data) == 0 {
		return fmt.Errorf("%w: invalid snapshot boundary", ErrCorrupt)
	}
	lastIndex := snapshot.LastIncludedIndex + uint64(len(state.Log))
	if state.CommitIndex < snapshot.LastIncludedIndex || state.CommitIndex > lastIndex {
		return fmt.Errorf("%w: commit index %d outside snapshot/log range %d..%d", ErrCorrupt, state.CommitIndex, snapshot.LastIncludedIndex, lastIndex)
	}
	for i, entry := range state.Log {
		wantIndex := snapshot.LastIncludedIndex + uint64(i) + 1
		if entry.Index != wantIndex {
			return fmt.Errorf("%w: log index %d at position %d", ErrCorrupt, entry.Index, wantIndex)
		}
		if entry.Term == 0 || entry.Term > state.CurrentTerm {
			return fmt.Errorf("%w: invalid term %d at log index %d for current term %d", ErrCorrupt, entry.Term, entry.Index, state.CurrentTerm)
		}
	}
	return nil
}

func cloneLog(log []raft.Entry) []raft.Entry { return append([]raft.Entry(nil), log...) }

// MemoryStore is a durable-across-object-restart test store with injectable
// Save failures.
type MemoryStore struct {
	mu       sync.Mutex
	state    raft.PersistentState
	found    bool
	nextFail error
}

func (s *MemoryStore) Load() (raft.PersistentState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state
	state.Log = cloneLog(state.Log)
	return state, s.found, nil
}

func (s *MemoryStore) Save(state raft.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextFail != nil {
		err := s.nextFail
		s.nextFail = nil
		return err
	}
	if err := validate(state); err != nil {
		return err
	}
	state.Log = cloneLog(state.Log)
	s.state = state
	s.found = true
	return nil
}

func (s *MemoryStore) FailNextSave(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextFail = err
}
