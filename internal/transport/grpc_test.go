package transport

import (
	"io"
	"log/slog"
	"testing"

	"github.com/yourusername/raftkv/internal/raft"
)

func TestDuplicateRPCResponseWaitersAreNotOverwritten(t *testing.T) {
	transport := NewGRPC("b", nil, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	first, releaseFirst := transport.CaptureVote("a", 7)
	defer releaseFirst()
	second, releaseSecond := transport.CaptureVote("a", 7)
	defer releaseSecond()

	for _, granted := range []bool{true, false} {
		err := transport.Send(nil, raft.Message{
			From: "b", To: "a", Type: raft.MessageRequestVoteResponse,
			RequestVoteResponse: raft.RequestVoteResponse{Term: 7, Election: 7, VoteGranted: granted},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if response := <-first; !response.VoteGranted {
		t.Fatal("first duplicate waiter did not receive first response")
	}
	if response := <-second; response.VoteGranted {
		t.Fatal("second duplicate waiter did not receive second response")
	}
}
