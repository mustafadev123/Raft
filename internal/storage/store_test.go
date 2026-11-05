package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yourusername/raftkv/internal/raft"
)

func testState(term uint64, value string) raft.PersistentState {
	return raft.PersistentState{
		CurrentTerm: term,
		Log: []raft.Entry{{
			Index: 1, Term: term,
			Command: raft.Command{Operation: raft.OperationPut, ClientID: "client", Sequence: 1, Key: "k", Value: value},
		}},
		CommitIndex: 1,
	}
}

func TestFileStoreRejectsCorruptAndIncompleteState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")
	store := &FileStore{Path: path}
	if err := store.Save(testState(1, "valid")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"payload":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load error = %v, want ErrCorrupt", err)
	}
}

func TestFileStoreCrashPointsKeepACompleteGeneration(t *testing.T) {
	for _, point := range []CrashPoint{BeforeTempWrite, AfterTempWrite, AfterTempSync, AfterReplace} {
		t.Run(string(point), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "raft-state.json")
			store := &FileStore{Path: path}
			oldState := testState(1, "old")
			if err := store.Save(oldState); err != nil {
				t.Fatal(err)
			}
			store.Hook = func(got CrashPoint) error {
				if got == point {
					return errors.New("simulated crash")
				}
				return nil
			}
			newState := testState(2, "new")
			if err := store.Save(newState); err == nil {
				t.Fatal("Save succeeded at injected crash point")
			}
			store.Hook = nil
			recovered, found, err := store.Load()
			if err != nil || !found {
				t.Fatalf("Load = found %v, err %v", found, err)
			}
			want := oldState
			if point == AfterReplace {
				want = newState
			}
			if recovered.CurrentTerm != want.CurrentTerm || recovered.Log[0].Command.Value != want.Log[0].Command.Value {
				t.Fatalf("recovered %+v, want complete term %d generation", recovered, want.CurrentTerm)
			}
		})
	}
}

func TestFileStoreRejectsInvalidCommitIndex(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "raft-state.json")}
	state := testState(1, "value")
	state.CommitIndex = 2
	if err := store.Save(state); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Save error = %v, want ErrCorrupt", err)
	}
}
