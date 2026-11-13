package runtime

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// fakeAWSRequestStores serves the DynamoDB and S3 calls the real AWS provider
// makes to open and probe cloud request storage. DescribeTable reports status
// and Query fails with queryError when it is set.
type fakeAWSRequestStores struct {
	status, queryError        atomic.Value
	describeCalls, queryCalls atomic.Int32
}

func newFakeAWSRequestStores(t *testing.T) *fakeAWSRequestStores {
	t.Helper()
	stores := &fakeAWSRequestStores{}
	stores.status.Store("ACTIVE")
	stores.queryError.Store("")
	server := httptest.NewServer(http.HandlerFunc(stores.serve(t)))
	t.Cleanup(server.Close)
	t.Setenv("AWS_ENDPOINT_URL_DYNAMODB", server.URL)
	t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/absent")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	return stores
}

func (stores *fakeAWSRequestStores) serve(t *testing.T) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if target := r.Header.Get("X-Amz-Target"); target != "" {
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			switch target {
			case "DynamoDB_20120810.DescribeTable":
				stores.describeCalls.Add(1)
				writeDynamoDB(w, http.StatusOK, `{"Table":{"TableName":"physical-table","TableStatus":"`+stores.status.Load().(string)+`","TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/physical-table","TableId":"00000000-0000-0000-0000-000000000000",`+
					`"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}]}}`)
			case "DynamoDB_20120810.Query":
				stores.queryCalls.Add(1)
				if kind := stores.queryError.Load().(string); kind != "" {
					writeDynamoDB(w, http.StatusBadRequest, `{"__type":"com.amazonaws.dynamodb.v20120810#`+kind+`","message":"table is not accessible"}`)
					return
				}
				writeDynamoDB(w, http.StatusOK, `{"Items":[],"Count":0,"ScannedCount":0}`)
			default:
				t.Errorf("unexpected DynamoDB call %q", target)
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}
		// S3 uses path-style requests against an IP endpoint.
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/payload-bucket":
			w.Header().Set("X-Amz-Bucket-Region", "us-east-1")
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/payload-bucket/"):
			// The readiness payload is never written; a missing key is ready.
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
		default:
			t.Errorf("unexpected S3 call %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}
}

// writeDynamoDB sends the CRC32 header the SDK verifies on DynamoDB responses.
func writeDynamoDB(w http.ResponseWriter, status int, body string) {
	w.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE([]byte(body))), 10))
	w.WriteHeader(status)
	io.WriteString(w, body)
}

// A single-region DynamoDB table reports UPDATING during UpdateTable changes
// such as autoscaling, billing mode, encryption or streams, and stays usable
// for item reads and writes. The worker must keep advertising readiness and
// keep polling Temporal throughout, well past the consecutive-failure budget
// that would pause it if the probe treated UPDATING as unavailable.
func TestRuntimeKeepsPollingWhileCloudRequestTableIsUpdating(t *testing.T) {
	stores := newFakeAWSRequestStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repository, err := cloudstate.Open(ctx, cloudstate.Config{
		Provider:     map[string]any{"type": "aws", "aws": map[string]any{"region": "us-east-1"}, "key_value_stores": map[string]any{"requests": "physical-table"}, "blob_stores": map[string]any{"payloads": "payload-bucket"}},
		RequestTable: "requests", PayloadStore: "payloads", Namespace: "requests-v1",
	}, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatalf("open cloud request storage: %v", err)
	}

	controller := &monitoringWorker{}
	var closed atomic.Bool
	options := testRuntimeOptions(t, &testWorker{}, &closed)
	options.WorkerFactory = func(_ client.Client, _ string, _ worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
		return controller, &testRegistry{}, nil
	}
	options.DependencyProbes = []DependencyProbe{cloudRequestProbe(repository)}
	runtime, err := New(ctx, runtimeConfig(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(); err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("sandbox does not permit loopback listeners: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if !runtime.Health.Ready() || controller.starts.Load() != 1 {
		t.Fatalf("initial state ready=%v starts=%d", runtime.Health.Ready(), controller.starts.Load())
	}

	stores.status.Store("UPDATING")
	describes, queries := stores.describeCalls.Load(), stores.queryCalls.Load()
	rounds := pauseAfterProbeFailures + 2
	for round := 1; round <= rounds; round++ {
		if err := runtime.syncDependencyReadiness(ctx); err != nil {
			t.Fatal(err)
		}
		if !runtime.Health.Ready() || controller.stops.Load() != 0 || controller.starts.Load() != 1 {
			t.Fatalf("UPDATING probe round %d: ready=%v starts=%d stops=%d", round, runtime.Health.Ready(), controller.starts.Load(), controller.stops.Load())
		}
	}
	// Every round must have reached the UPDATING table, not a cached result.
	if got := stores.describeCalls.Load() - describes; got != int32(rounds) {
		t.Fatalf("DescribeTable calls during UPDATING = %d, want %d", got, rounds)
	}
	if got := stores.queryCalls.Load() - queries; got != int32(rounds) {
		t.Fatalf("Query calls during UPDATING = %d, want %d", got, rounds)
	}

	// The same rounds against an archived table do pause the worker, so the
	// assertions above are not satisfied by a probe that never fails.
	stores.status.Store("ARCHIVED")
	stores.queryError.Store("ResourceNotFoundException")
	for round := 1; round <= pauseAfterProbeFailures; round++ {
		if err := runtime.syncDependencyReadiness(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitForRuntime(t, func() bool { return controller.stops.Load() >= 1 && !runtime.Health.Ready() })
}
