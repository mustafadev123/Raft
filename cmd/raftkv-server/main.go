// Command raftkv-server runs one statically configured raftkv node.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	raftkvv1 "github.com/yourusername/raftkv/api/raftkv/v1"
	"github.com/yourusername/raftkv/internal/kv"
	"github.com/yourusername/raftkv/internal/raft"
	serverruntime "github.com/yourusername/raftkv/internal/server"
	"github.com/yourusername/raftkv/internal/storage"
	peertransport "github.com/yourusername/raftkv/internal/transport"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id            = flag.String("id", "", "node ID (required)")
		listenAddress = flag.String("listen", "127.0.0.1:7001", "loopback listen address")
		peerList      = flag.String("peers", "", "comma-separated id=address static membership")
		dataDirectory = flag.String("data-dir", "", "node data directory (required)")
		electionMin   = flag.Duration("election-min", 800*time.Millisecond, "minimum election timeout")
		electionMax   = flag.Duration("election-max", 1400*time.Millisecond, "maximum election timeout")
		heartbeat     = flag.Duration("heartbeat", 150*time.Millisecond, "leader heartbeat interval")
		rpcTimeout    = flag.Duration("rpc-timeout", 500*time.Millisecond, "peer RPC deadline")
		stopFile      = flag.String("stop-file", "", "optional local file whose creation requests graceful shutdown")
		snapshotEvery = flag.Uint64("snapshot-threshold", 100, "compact after this many newly applied entries; 0 disables")
		maxSnapshot   = flag.Int("max-snapshot-bytes", 2<<20, "maximum unary snapshot payload size")
	)
	flag.Parse()
	if *id == "" || *dataDirectory == "" {
		return fmt.Errorf("-id and -data-dir are required")
	}
	peers, err := parsePeers(*peerList)
	if err != nil {
		return err
	}
	if len(peers) != 3 && len(peers) != 5 {
		return fmt.Errorf("static membership must contain 3 or 5 nodes")
	}
	if peers[raft.NodeID(*id)] != *listenAddress {
		return fmt.Errorf("-listen must match this node's address in -peers")
	}
	members := make([]raft.NodeID, 0, len(peers))
	for member := range peers {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("node", *id)
	seedBytes := make([]byte, 8)
	if _, err := cryptorand.Read(seedBytes); err != nil {
		return fmt.Errorf("seed election randomness: %w", err)
	}
	random := rand.New(rand.NewSource(int64(binary.LittleEndian.Uint64(seedBytes))))
	transport := peertransport.NewGRPC(raft.NodeID(*id), peers, *rpcTimeout, logger)
	machine := kv.New(kv.DefaultDeduplicationClients)
	store := &storage.FileStore{Path: filepath.Join(*dataDirectory, "raft-state.json")}
	node, err := raft.NewNode(raft.Config{
		ID: raft.NodeID(*id), Members: members, HeartbeatInterval: *heartbeat,
		Timeouts:  raft.RandomTimeout{Min: *electionMin, Max: *electionMax, Source: random},
		Transport: transport, StateMachine: machine, Store: store,
		SnapshotThreshold: *snapshotEvery, MaxSnapshotBytes: *maxSnapshot,
	}, time.Now())
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}
	runtime := serverruntime.NewRuntime(node, logger)
	rootContext, cancelRuntime := context.WithCancel(context.Background())
	go runtime.Run(rootContext)
	transport.SetReceiver(runtime.Step)

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		cancelRuntime()
		return fmt.Errorf("listen on %s: %w", *listenAddress, err)
	}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(*maxSnapshot + (64 << 10)))
	raftkvv1.RegisterRaftPeerServer(grpcServer, &serverruntime.PeerService{Runtime: runtime, Responses: transport})
	raftkvv1.RegisterKeyValueServer(grpcServer, &serverruntime.KeyValueService{Runtime: runtime, PeerAddresses: peers, MaxWait: 5 * time.Second})
	serveError := make(chan error, 1)
	go func() { serveError <- grpcServer.Serve(listener) }()
	logger.Info("node listening", "listen", *listenAddress, "members", members, "data_dir", *dataDirectory)
	stopRequest := make(chan struct{}, 1)
	if *stopFile != "" {
		_ = os.Remove(*stopFile)
		go watchStopFile(rootContext, *stopFile, stopRequest)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case signalValue := <-signals:
		logger.Info("shutdown requested", "signal", signalValue.String())
	case <-stopRequest:
		logger.Info("shutdown requested", "source", "stop-file")
	case err := <-serveError:
		cancelRuntime()
		_ = transport.Close()
		return fmt.Errorf("gRPC serve: %w", err)
	}

	gracefulDone := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(gracefulDone) }()
	select {
	case <-gracefulDone:
	case <-time.After(5 * time.Second):
		logger.Warn("graceful shutdown timed out; forcing stop")
		grpcServer.Stop()
		<-gracefulDone
	}
	cancelRuntime()
	_ = transport.Close()
	runtime.Wait()
	logger.Info("node stopped")
	return nil
}

func watchStopFile(ctx context.Context, path string, requested chan<- struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				select {
				case requested <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

func parsePeers(value string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	for _, item := range strings.Split(value, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer %q; want id=address", item)
		}
		peers[raft.NodeID(parts[0])] = parts[1]
	}
	return peers, nil
}
