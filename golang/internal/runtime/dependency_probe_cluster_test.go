package runtime

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

// fakeClusterNode is a minimal RESP2 Redis Cluster node. It answers only the
// commands the readiness probe and the cluster client's topology load send.
type fakeClusterNode struct {
	id       string
	listener net.Listener
	mu       sync.Mutex
	settings map[string]string
	replicas []*fakeClusterNode
	slots    [2]int
	topology *[]*fakeClusterNode
	// stalled nodes still serve the connection handshake and topology, but
	// never answer a per-node probe command, as a hung node would.
	stalled atomic.Bool
}

func newFakeClusterNode(t *testing.T, id string, topology *[]*fakeClusterNode) *fakeClusterNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node := &fakeClusterNode{
		id:       id,
		listener: listener,
		settings: map[string]string{"maxmemory-policy": "noeviction", "appendonly": "yes", "save": "60 1"},
		topology: topology,
	}
	t.Cleanup(func() { _ = listener.Close() })
	go node.serve()
	return node
}

func (node *fakeClusterNode) addr() string { return node.listener.Addr().String() }

func (node *fakeClusterNode) set(name, value string) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.settings[name] = value
}

func (node *fakeClusterNode) serve() {
	for {
		conn, err := node.listener.Accept()
		if err != nil {
			return
		}
		go node.handle(conn)
	}
}

func (node *fakeClusterNode) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		if command := strings.ToUpper(strings.Join(args, " ")); node.stalled.Load() && (command == "CLUSTER MYID" || strings.HasPrefix(command, "CONFIG GET")) {
			_, _ = io.Copy(io.Discard, conn)
			return
		}
		if _, err := io.WriteString(conn, node.reply(args)); err != nil {
			return
		}
	}
}

func (node *fakeClusterNode) reply(args []string) string {
	command := strings.ToUpper(strings.Join(args[:min(2, len(args))], " "))
	switch {
	case command == "PING":
		return "+PONG\r\n"
	case strings.HasPrefix(command, "TIME"):
		return respArray(respBulk(strconv.FormatInt(time.Now().Unix(), 10)), respBulk("0"))
	case command == "CLUSTER MYID":
		return respBulk(node.id)
	case command == "CLUSTER SLOTS":
		var ranges []string
		for _, master := range *node.topology {
			entries := []string{respInteger(master.slots[0]), respInteger(master.slots[1])}
			for _, member := range append([]*fakeClusterNode{master}, master.replicas...) {
				host, port, _ := net.SplitHostPort(member.addr())
				portNumber, _ := strconv.Atoi(port)
				entries = append(entries, respArray(respBulk(host), respInteger(portNumber), respBulk(member.id)))
			}
			ranges = append(ranges, respArray(entries...))
		}
		return respArray(ranges...)
	case command == "CONFIG GET" && len(args) == 3:
		node.mu.Lock()
		defer node.mu.Unlock()
		return respArray(respBulk(args[2]), respBulk(node.settings[args[2]]))
	case command == "SCRIPT EXISTS":
		return "*1\r\n:1\r\n"
	default:
		return "-ERR unknown command\r\n"
	}
}

func respInteger(value int) string { return ":" + strconv.Itoa(value) + "\r\n" }

func respBulk(value string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value) }

func respArray(items ...string) string {
	return "*" + strconv.Itoa(len(items)) + "\r\n" + strings.Join(items, "")
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("unexpected RESP line %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return nil, err
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args = append(args, string(value[:size]))
	}
	return args, nil
}

// TestRedisDependencyProbeChecksReplicasOnClusterClient runs the probe through
// a real cluster client against fake nodes, so replica discovery uses the
// client's own topology and the slot owner's CLUSTER SLOTS reply.
func TestRedisDependencyProbeChecksReplicasOnClusterClient(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(owner *fakeClusterNode, others []*fakeClusterNode)
		want   ProbeStatus
		// fast marks cases that must not wait on a stalled node.
		fast bool
	}{
		{name: "compliant", want: ProbeStatusReady},
		{
			name: "slot owner replica evicts",
			mutate: func(owner *fakeClusterNode, _ []*fakeClusterNode) {
				owner.replicas[1].set("maxmemory-policy", "allkeys-lru")
			},
			want: ProbeStatusPolicy,
		},
		{
			name:   "slot owner replica without aof",
			mutate: func(owner *fakeClusterNode, _ []*fakeClusterNode) { owner.replicas[0].set("appendonly", "no") },
			want:   ProbeStatusPolicy,
		},
		{
			name: "other primary replica evicts",
			mutate: func(_ *fakeClusterNode, others []*fakeClusterNode) {
				for _, other := range others {
					other.replicas[0].set("maxmemory-policy", "allkeys-lru")
				}
			},
			want: ProbeStatusReady,
		},
		{
			// Replicas of other primaries are never contacted, so a hung
			// one cannot consume the probe deadline.
			name: "other primary replica stalled",
			mutate: func(_ *fakeClusterNode, others []*fakeClusterNode) {
				for _, other := range others {
					other.replicas[0].stalled.Store(true)
				}
			},
			want: ProbeStatusReady,
			fast: true,
		},
		{
			name:   "slot owner replica unreachable",
			mutate: func(owner *fakeClusterNode, _ []*fakeClusterNode) { _ = owner.replicas[0].listener.Close() },
			want:   ProbeStatusUnavailable,
		},
		{
			// Redis omits a replica it has marked as failed from CLUSTER
			// SLOTS, so readiness recovers once the cluster fails the node.
			name: "failed slot owner replica omitted",
			mutate: func(owner *fakeClusterNode, _ []*fakeClusterNode) {
				_ = owner.replicas[0].listener.Close()
				owner.replicas = owner.replicas[1:]
			},
			want: ProbeStatusReady,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var topology []*fakeClusterNode
			bounds := [][2]int{{0, 5460}, {5461, 10922}, {10923, 16383}}
			for i, bound := range bounds {
				master := newFakeClusterNode(t, fmt.Sprintf("master%d", i), &topology)
				master.slots = bound
				for j := 0; j < 2; j++ {
					master.replicas = append(master.replicas, newFakeClusterNode(t, fmt.Sprintf("replica%d-%d", i, j), &topology))
				}
				topology = append(topology, master)
			}
			cluster := redisclient.NewClusterClient(&redisclient.ClusterOptions{Addrs: []string{topology[0].addr()}, MaxRedirects: -1, MaxRetries: -1, Protocol: 2, DisableIdentity: true, ReadTimeout: 5 * time.Second})
			t.Cleanup(func() { _ = cluster.Close() })
			value := testRedisProbeConfig()
			value.KeyPrefix = "worker"
			value.AdmissionHashTag = "budget"
			value.AdmissionMode = "lua"
			value.AdmissionDigest = redisstore.AdmissionLuaDigest()
			probe, err := NewRedisDependencyProbe(cluster, value)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ownerClient, err := cluster.MasterForKey(ctx, redisAdmissionSlotKey(value))
			if err != nil {
				t.Fatal(err)
			}
			var owner *fakeClusterNode
			var others []*fakeClusterNode
			for _, master := range topology {
				if master.addr() == ownerClient.Options().Addr {
					owner = master
				} else {
					others = append(others, master)
				}
			}
			if owner == nil {
				t.Fatalf("slot owner %s is not a fake node", ownerClient.Options().Addr)
			}
			if test.mutate != nil {
				test.mutate(owner, others)
			}
			for attempt := 0; attempt < 3; attempt++ {
				started := time.Now()
				if result := probe.Probe(ctx); result.Status != test.want {
					t.Fatalf("attempt %d result = %#v, want %s", attempt, result, test.want)
				}
				if elapsed := time.Since(started); test.fast && elapsed > 2*time.Second {
					t.Fatalf("attempt %d took %s waiting on a stalled node", attempt, elapsed)
				}
			}
		})
	}
}
