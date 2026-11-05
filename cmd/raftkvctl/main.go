// Command raftkvctl issues retry-safe client requests to a local cluster.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	raftkvv1 "github.com/yourusername/raftkv/api/raftkv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type connectionPool struct{ connections map[string]*grpc.ClientConn }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addressesFlag := flag.String("addr", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated node addresses")
	clientIDFlag := flag.String("client-id", "", "stable unique client ID; generated if omitted")
	sequence := flag.Uint64("seq", 1, "monotonic request sequence")
	totalTimeout := flag.Duration("timeout", 8*time.Second, "total retry deadline")
	attemptTimeout := flag.Duration("attempt-timeout", 1500*time.Millisecond, "per-RPC deadline")
	flag.Parse()
	arguments := flag.Args()
	if len(arguments) < 2 {
		return fmt.Errorf("usage: raftkvctl [flags] put KEY VALUE | get KEY | delete KEY")
	}
	operation, key := strings.ToLower(arguments[0]), arguments[1]
	value := ""
	if operation == "put" {
		if len(arguments) != 3 {
			return fmt.Errorf("put requires KEY VALUE")
		}
		value = arguments[2]
	} else if operation != "get" && operation != "delete" {
		return fmt.Errorf("unknown operation %q", operation)
	}
	clientID := *clientIDFlag
	if clientID == "" {
		generated := make([]byte, 16)
		if _, err := rand.Read(generated); err != nil {
			return err
		}
		clientID = hex.EncodeToString(generated)
	}
	addresses := uniqueAddresses(strings.Split(*addressesFlag, ","))
	pool := &connectionPool{connections: make(map[string]*grpc.ClientConn)}
	defer pool.close()
	ctx, cancel := context.WithTimeout(context.Background(), *totalTimeout)
	defer cancel()
	response, unknown, err := executeWithRetry(ctx, pool, addresses, operation, clientID, *sequence, key, value, *attemptTimeout)
	if err != nil {
		if unknown {
			return fmt.Errorf("%w; outcome is unknown—retry with -client-id %s -seq %d", err, clientID, *sequence)
		}
		return err
	}
	output := map[string]any{
		"status": response.GetStatus().String(), "found": response.GetFound(), "value": response.GetValue(),
		"node_id": response.GetNodeId(), "term": response.GetTerm(), "client_id": clientID, "sequence": *sequence,
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}

func executeWithRetry(ctx context.Context, pool *connectionPool, addresses []string, operation, clientID string, sequence uint64, key, value string, attemptTimeout time.Duration) (*raftkvv1.ClientResponse, bool, error) {
	unknown := false
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, unknown, err
		}
		address := addresses[attempt%len(addresses)]
		connection, err := pool.get(address)
		if err != nil {
			return nil, unknown, err
		}
		attemptContext, cancel := context.WithTimeout(ctx, attemptTimeout)
		response, callErr := call(attemptContext, raftkvv1.NewKeyValueClient(connection), operation, clientID, sequence, key, value)
		cancel()
		if callErr == nil {
			if response.GetStatus() == raftkvv1.ClientStatus_CLIENT_STATUS_OK {
				return response, unknown, nil
			}
			if response.GetStatus() == raftkvv1.ClientStatus_CLIENT_STATUS_NOT_LEADER {
				if hint := response.GetLeaderHint(); hint != "" {
					addresses = prependUnique(hint, addresses)
				}
			} else {
				return nil, unknown, errors.New(response.GetError())
			}
		} else {
			code := status.Code(callErr)
			if code == codes.DeadlineExceeded || code == codes.Canceled {
				unknown = true
			} else if code != codes.Unavailable {
				return nil, unknown, callErr
			}
		}
		select {
		case <-time.After(75 * time.Millisecond):
		case <-ctx.Done():
			return nil, unknown, ctx.Err()
		}
	}
}

func call(ctx context.Context, client raftkvv1.KeyValueClient, operation, clientID string, sequence uint64, key, value string) (*raftkvv1.ClientResponse, error) {
	switch operation {
	case "put":
		return client.Put(ctx, &raftkvv1.PutRequest{ClientId: clientID, Sequence: sequence, Key: key, Value: value})
	case "get":
		return client.Get(ctx, &raftkvv1.GetRequest{ClientId: clientID, Sequence: sequence, Key: key})
	case "delete":
		return client.Delete(ctx, &raftkvv1.DeleteRequest{ClientId: clientID, Sequence: sequence, Key: key})
	default:
		return nil, fmt.Errorf("unknown operation %q", operation)
	}
}

func (p *connectionPool) get(address string) (*grpc.ClientConn, error) {
	if connection := p.connections[address]; connection != nil {
		return connection, nil
	}
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	p.connections[address] = connection
	return connection, nil
}

func (p *connectionPool) close() {
	for _, connection := range p.connections {
		_ = connection.Close()
	}
}

func uniqueAddresses(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result = prependUnique(value, result)
		}
	}
	if len(result) == 0 {
		return []string{"127.0.0.1:7001"}
	}
	return result
}

func prependUnique(value string, values []string) []string {
	result := []string{value}
	for _, existing := range values {
		if existing != value {
			result = append(result, existing)
		}
	}
	return result
}
