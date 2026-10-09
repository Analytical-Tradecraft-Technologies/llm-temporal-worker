//go:build composeliveintegration

package temporal_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	liveRecoveryWorkflowName              = "llmtw.integration.temporal-recovery.v1"
	liveRecoveryWorkflowTaskExecutionSize = 2
	liveSharedBudgetLimit                 = pricing.MicroUSD(100)
	liveHeartbeatTimeout                  = 2 * time.Second
	liveHeartbeatKeepaliveInterval        = 500 * time.Millisecond
	liveSuccessfulProviderHold            = liveHeartbeatTimeout + time.Second
)

func TestLiveRedisKeyPrefixFollowsComposeNamespace(t *testing.T) {
	t.Setenv("LLMTW_REDIS_KEY_PREFIX", "")
	if got, want := liveRedisKeyPrefix(), "llmtw"; got != want {
		t.Fatalf("default live Redis key prefix = %q, want %q", got, want)
	}
	t.Setenv("LLMTW_REDIS_KEY_PREFIX", "tenant-a.v1")
	if got, want := liveRedisKeyPrefix(), "tenant-a.v1"; got != want {
		t.Fatalf("configured live Redis key prefix = %q, want %q", got, want)
	}
}

func TestLiveRecoveryWorkerOptionsUseTemporalSupportedMinimum(t *testing.T) {
	options := liveRecoveryWorkerOptions("construction")
	// One panics in the Temporal SDK and zero silently selects its large default.
	if got, want := options.MaxConcurrentWorkflowTaskExecutionSize, 2; got != want {
		t.Fatalf("workflow task execution size = %d, want Temporal-supported bounded value %d", got, want)
	}
	if got, want := options.MaxHeartbeatThrottleInterval, liveHeartbeatKeepaliveInterval; got != want {
		t.Fatalf("heartbeat throttle interval = %s, want keepalive cadence %s", got, want)
	}

	workflowClient, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: "default"})
	if err != nil {
		t.Fatalf("create lazy Temporal client: %v", err)
	}
	t.Cleanup(workflowClient.Close)
	if valueWorker := worker.New(workflowClient, "llmtw-live-recovery-construction", options); valueWorker == nil {
		t.Fatal("construct live recovery worker: got nil")
	}
}

// TestTemporalRecoveryWithSharedRedis uses the real Temporal development
// server and the Redis instance from the worker Compose profile. The adapter
// is deliberately in-process and content-free: it records a possible write,
// waits for the first worker to stop, and then reports only the conservative
// ambiguous outcome. The replacement replica must never replay that provider
// call or reserve the shared budget a second time.
func TestTemporalRecoveryWithSharedRedis(t *testing.T) {
	temporalAddress := os.Getenv("LLMTW_TEMPORAL_ADDRESS")
	redisAddress := os.Getenv("LLMTW_REDIS_ADDR")
	redisUsername := os.Getenv("LLMTW_REDIS_USERNAME")
	redisPassword := os.Getenv("LLMTW_REDIS_PASSWORD")
	redisKeyPrefix := liveRedisKeyPrefix()
	if temporalAddress == "" || redisAddress == "" || redisUsername == "" || redisPassword == "" {
		t.Skip("make compose-live-integration supplies the local Temporal and authenticated Redis addresses")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	workflowClient, err := client.Dial(client.Options{HostPort: temporalAddress, Namespace: "default"})
	if err != nil {
		t.Fatalf("dial Temporal: %v", err)
	}
	t.Cleanup(func() { workflowClient.Close() })

	queue := fmt.Sprintf("llmtw-live-recovery-%d", time.Now().UnixNano())
	redisClient := redisclient.NewClient(&redisclient.Options{Addr: redisAddress, Username: redisUsername, Password: redisPassword})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Compose Redis: %v", err)
	}
	admissionStore, err := redisstore.NewAdmissionStore(redisstore.AdmissionOptions{
		Client:          redisClient,
		Mode:            redisstore.AdmissionModeFunction,
		FunctionVersion: redisstore.AdmissionFunctionVersion,
		Keys: redisstore.KeyOptions{
			Prefix:    redisKeyPrefix,
			HashTag:   "admission",
			KeySecret: liveKeySecret(queue),
		},
		Clock:          time.Now,
		MaxRecordBytes: 256 << 10,
	})
	if err != nil {
		t.Fatalf("create shared Redis admission store: %v", err)
	}

	adapter := newAcceptedThenStoppedAdapter()
	results := &liveResultStore{}
	firstEngine := newLiveRecoveryEngine(t, admissionStore, results, adapter)
	secondEngine := newLiveRecoveryEngine(t, admissionStore, results, adapter)
	first := newLiveRecoveryWorker(t, workflowClient, queue, "first", firstEngine)
	firstStarted := false
	defer func() {
		if firstStarted {
			first.Stop()
		}
	}()
	if err := first.Start(); err != nil {
		t.Fatalf("start first Temporal worker: %v", err)
	}
	firstStarted = true

	payload := activity.GenerateRequest{APIVersion: activity.APIVersion, Request: liveRecoveryRequest()}
	firstRun, err := workflowClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("llmtw-live-first-%d", time.Now().UnixNano()),
		TaskQueue:                queue,
		WorkflowRunTimeout:       30 * time.Second,
		WorkflowTaskTimeout:      5 * time.Second,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, liveRecoveryWorkflowName, payload)
	if err != nil {
		t.Fatalf("start first recovery workflow: %v", err)
	}
	if err := adapter.WaitAccepted(ctx); err != nil {
		t.Fatal(err)
	}

	// Start the replacement before stopping the accepted attempt. This is the
	// rolling two-replica shape operators use during a worker restart; after
	// Stop returns the second replica is the only eligible poller.
	second := newLiveRecoveryWorker(t, workflowClient, queue, "second", secondEngine)
	secondStarted := false
	defer func() {
		if secondStarted {
			second.Stop()
		}
	}()
	if err := second.Start(); err != nil {
		t.Fatalf("start replacement Temporal worker: %v", err)
	}
	secondStarted = true

	first.Stop()
	firstStarted = false
	firstDetails := waitForAmbiguousWorkflow(t, ctx, firstRun)
	if firstDetails.OperationID == "" {
		t.Fatal("ambiguous Activity error did not contain an operation ID")
	}
	operation, err := admissionStore.Get(ctx, firstDetails.OperationID)
	if err != nil {
		t.Fatalf("load shared admission operation: %v", err)
	}
	if operation.State != admission.StateAmbiguous || operation.Attempt.Dispatch != admission.Ambiguous {
		t.Fatalf("operation after stopped accepted dispatch = %#v, want ambiguous terminal state", operation)
	}
	if operation.ReservedMicroUSD <= 0 || operation.ReservedMicroUSD > liveSharedBudgetLimit || len(operation.Reservations) != 1 {
		t.Fatalf("shared budget reservation = %#v, want one bounded reservation", operation)
	}

	// A new Temporal workflow with the same request is an existing-operation
	// replay against the replacement replica. It must receive the same safe
	// ambiguity rather than invoke the adapter or create another reservation.
	secondRun, err := workflowClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("llmtw-live-replay-%d", time.Now().UnixNano()),
		TaskQueue:                queue,
		WorkflowRunTimeout:       30 * time.Second,
		WorkflowTaskTimeout:      5 * time.Second,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, liveRecoveryWorkflowName, payload)
	if err != nil {
		t.Fatalf("start replay workflow: %v", err)
	}
	secondDetails := waitForAmbiguousWorkflow(t, ctx, secondRun)
	if secondDetails.Code != string(provider.CodeAmbiguousDispatch) || secondDetails.Phase != string(provider.PhaseAdmission) || secondDetails.Dispatch != string(provider.DispatchAmbiguous) {
		t.Fatalf("replayed ambiguous Activity details = %#v, want ambiguous admission refusal", secondDetails)
	}
	replayedOperation, err := admissionStore.Get(ctx, firstDetails.OperationID)
	if err != nil {
		t.Fatalf("reload shared admission operation after replay: %v", err)
	}
	if replayedOperation.State != admission.StateAmbiguous || replayedOperation.Attempt.Dispatch != admission.Ambiguous || replayedOperation.ResultRef != nil {
		t.Fatalf("operation after ambiguous replay = %#v, want unchanged ambiguous operation without result", replayedOperation)
	}
	if replayedOperation.ReservedMicroUSD != operation.ReservedMicroUSD || replayedOperation.IncurredMicroUSD != operation.IncurredMicroUSD || replayedOperation.FinalMicroUSD != operation.FinalMicroUSD {
		t.Fatalf("operation financial facts after ambiguous replay = %#v, want %#v", replayedOperation, operation)
	}
	if len(replayedOperation.Reservations) != len(operation.Reservations) {
		t.Fatalf("reservation count after ambiguous replay = %d, want %d", len(replayedOperation.Reservations), len(operation.Reservations))
	}
	for index := range operation.Reservations {
		if replayedOperation.Reservations[index] != operation.Reservations[index] {
			t.Fatalf("reservation %d after ambiguous replay = %#v, want %#v", index, replayedOperation.Reservations[index], operation.Reservations[index])
		}
	}
	if calls := adapter.Calls(); calls != 1 {
		t.Fatalf("accepted provider dispatches across two replicas = %d, want one", calls)
	}
	if puts := results.Puts(); puts != 0 {
		t.Fatalf("ambiguous operation wrote %d completed results", puts)
	}
}

// TestTemporalKeepaliveCompletesLongOneShotProviderCall proves the
// provider-wait heartbeat lifecycle against the real Compose Temporal service.
// The in-process, content-free adapter deliberately holds one successful
// Adapter.Invoke call longer than the Activity heartbeat timeout. The Activity
// must complete from that one call rather than timing out and retrying it.
func TestTemporalKeepaliveCompletesLongOneShotProviderCall(t *testing.T) {
	temporalAddress := os.Getenv("LLMTW_TEMPORAL_ADDRESS")
	redisAddress := os.Getenv("LLMTW_REDIS_ADDR")
	redisUsername := os.Getenv("LLMTW_REDIS_USERNAME")
	redisPassword := os.Getenv("LLMTW_REDIS_PASSWORD")
	redisKeyPrefix := liveRedisKeyPrefix()
	if temporalAddress == "" || redisAddress == "" || redisUsername == "" || redisPassword == "" {
		t.Skip("make compose-live-integration supplies the local Temporal and authenticated Redis addresses")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	workflowClient, err := client.Dial(client.Options{HostPort: temporalAddress, Namespace: "default"})
	if err != nil {
		t.Fatalf("dial Temporal: %v", err)
	}
	t.Cleanup(func() { workflowClient.Close() })

	queue := fmt.Sprintf("llmtw-live-keepalive-%d", time.Now().UnixNano())
	redisClient := redisclient.NewClient(&redisclient.Options{Addr: redisAddress, Username: redisUsername, Password: redisPassword})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Compose Redis: %v", err)
	}
	admissionStore, err := redisstore.NewAdmissionStore(redisstore.AdmissionOptions{
		Client:          redisClient,
		Mode:            redisstore.AdmissionModeFunction,
		FunctionVersion: redisstore.AdmissionFunctionVersion,
		Keys: redisstore.KeyOptions{
			Prefix:    redisKeyPrefix,
			HashTag:   "admission",
			KeySecret: liveKeySecret(queue),
		},
		Clock:          time.Now,
		MaxRecordBytes: 256 << 10,
	})
	if err != nil {
		t.Fatalf("create shared Redis admission store: %v", err)
	}

	adapter := newDelayedSuccessfulAdapter(liveSuccessfulProviderHold)
	results := &liveSuccessfulResultStore{}
	value := newLiveRecoveryEngine(t, admissionStore, results, adapter)
	valueWorker := newLiveRecoveryWorker(t, workflowClient, queue, "keepalive", value)
	started := false
	defer func() {
		if started {
			valueWorker.Stop()
		}
	}()
	if err := valueWorker.Start(); err != nil {
		t.Fatalf("start Temporal worker: %v", err)
	}
	started = true

	payload := activity.GenerateRequest{APIVersion: activity.APIVersion, Request: liveKeepaliveRequest()}
	run, err := workflowClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("llmtw-live-keepalive-%d", time.Now().UnixNano()),
		TaskQueue:                queue,
		WorkflowRunTimeout:       30 * time.Second,
		WorkflowTaskTimeout:      5 * time.Second,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, liveRecoveryWorkflowName, payload)
	if err != nil {
		t.Fatalf("start keepalive workflow: %v", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("long one-shot keepalive workflow: %v", err)
	}
	if calls := adapter.Calls(); calls != 1 {
		t.Fatalf("provider calls across completed Activity = %d, want one", calls)
	}
	if elapsed := adapter.SuccessfulCallElapsed(); elapsed <= liveHeartbeatTimeout {
		t.Fatalf("successful provider call ran for %s, want longer than heartbeat timeout %s", elapsed, liveHeartbeatTimeout)
	}
	if puts := results.Puts(); puts != 1 {
		t.Fatalf("completed provider call wrote %d results, want one", puts)
	}
}

func liveRecoveryWorkflow(ctx workflow.Context, payload activity.GenerateRequest) error {
	options := workflow.ActivityOptions{
		StartToCloseTimeout:    15 * time.Second,
		ScheduleToCloseTimeout: 25 * time.Second,
		HeartbeatTimeout:       liveHeartbeatTimeout,
		WaitForCancellation:    true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        200 * time.Millisecond,
			MaximumInterval:        200 * time.Millisecond,
			BackoffCoefficient:     1,
			MaximumAttempts:        2,
			NonRetryableErrorTypes: []string{activity.ErrorTypeAmbiguous, activity.ErrorTypeOperationConflict},
		},
	}
	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), activity.LegacyGenerateActivityName, payload).Get(ctx, nil)
}

func newLiveRecoveryWorker(t *testing.T, workflowClient client.Client, queue, identity string, value *engine.Engine) worker.Worker {
	t.Helper()
	valueWorker := worker.New(workflowClient, queue, liveRecoveryWorkerOptions(identity))
	valueWorker.RegisterWorkflowWithOptions(liveRecoveryWorkflow, workflow.RegisterOptions{Name: liveRecoveryWorkflowName})
	(&activity.Activities{Engine: value, Heartbeater: &activity.TemporalHeartbeater{}, HeartbeatKeepaliveInterval: liveHeartbeatKeepaliveInterval}).Register(valueWorker)
	return valueWorker
}

func liveRecoveryWorkerOptions(identity string) worker.Options {
	return worker.Options{
		Identity:                           "llmtw-live-recovery-" + identity,
		MaxConcurrentActivityExecutionSize: 1,
		// Temporal needs two slots to alternate sticky and regular queue polling.
		MaxConcurrentWorkflowTaskExecutionSize: liveRecoveryWorkflowTaskExecutionSize,
		// RecordHeartbeat is throttled by the SDK, independently of the Activity
		// keepalive ticker. Its default is 80% of the heartbeat timeout, leaving
		// only 400ms of scheduling/RPC margin under this fixture's 2s deadline.
		// Bound the network cadence too, while retaining the short deadline and
		// the provider call that runs longer than it.
		MaxHeartbeatThrottleInterval: liveHeartbeatKeepaliveInterval,
		WorkerStopTimeout:            5 * time.Second,
	}
}

func newLiveRecoveryEngine(t *testing.T, admissions admission.AdmissionStore, results engine.ResultStore, adapter provider.Adapter) *engine.Engine {
	t.Helper()
	classes := []llm.ServiceClass{llm.ServiceClassStandard}
	routes, err := routing.CompileCatalog("live-recovery-routes-v1", map[string]routing.Model{
		"live-recovery-model": {Routes: []routing.Route{{
			ID: "live-recovery-route", EndpointID: "live-recovery-endpoint", Provider: "content-free-fixture",
			Family: string(provider.FamilyOpenAIResponses), Region: "local", AccountRegion: "local",
			Model: "content-free-model", ModelLineage: "content-free-model", Classes: classes,
			ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"},
			PriceVersion:  "live-recovery-prices-v1", PriceAvailable: true,
			Capabilities: routing.CapabilitySet{Version: "live-recovery-capabilities-v1", Features: map[routing.Feature]routing.Capability{
				routing.FeatureText: {State: routing.CapabilityNative},
			}},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	priceCatalog, err := pricing.CompileUSD("live-recovery-prices-v1", []pricing.Entry{{
		Provider: "content-free-fixture", Family: string(provider.FamilyOpenAIResponses), EndpointID: "live-recovery-endpoint",
		Region: "local", Model: "content-free-model", ProviderTier: "standard", Version: "live-recovery-prices-v1",
		Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.000001"), OutputPerMillion: pricing.MustDecimalUSD("1")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := engine.New(engine.Dependencies{
		Snapshots: engine.StaticSnapshot{Value: engine.Snapshot{
			Version: "live-recovery-snapshot-v1", Routes: routes, Prices: pricing.NewResolver(priceCatalog),
			BudgetPolicies: []budget.Policy{{
				ID:      "live-shared-budget",
				Match:   budget.Matcher{Tenant: "live-tenant", Environment: "development"},
				Windows: []budget.Window{{ID: "minute", Duration: time.Minute, Bucket: time.Minute, Limit: liveSharedBudgetLimit}},
			}},
			RequireBudgetMatch: true, RequirePriceWhenBudgeted: true, Environment: "development",
			ReservationLease: 15 * time.Second, OperationRetention: time.Minute,
		}},
		Planner: routing.DeterministicPlanner{}, Adapters: engine.AdapterMap{"live-recovery-endpoint": adapter},
		Admission: admissions, Results: results, Clock: time.Now, Estimator: budget.Estimator{MaxOutput: 1}, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func liveRecoveryRequest() llm.Request {
	return llm.Request{
		OperationKey: "shared-recovery-operation",
		Context:      llm.RequestContext{Tenant: "live-tenant"},
		Model:        "live-recovery-model",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "content-free live recovery fixture"},
		}}},
	}
}

func liveKeepaliveRequest() llm.Request {
	return llm.Request{
		OperationKey: "shared-keepalive-operation",
		Context:      llm.RequestContext{Tenant: "live-tenant"},
		Model:        "live-recovery-model",
		Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{
			llm.TextPart{Text: "content-free live keepalive fixture"},
		}}},
	}
}

func liveKeySecret(queue string) []byte {
	digest := sha256.Sum256([]byte("llmtw-live-recovery-key:" + queue))
	return digest[:]
}

func liveRedisKeyPrefix() string {
	if value := os.Getenv("LLMTW_REDIS_KEY_PREFIX"); value != "" {
		return value
	}
	return "llmtw"
}

func waitForAmbiguousWorkflow(t *testing.T, ctx context.Context, run client.WorkflowRun) activity.SafeErrorDetails {
	t.Helper()
	err := run.Get(ctx, nil)
	if err == nil {
		t.Fatal("workflow unexpectedly completed after an accepted worker stop")
	}
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatalf("workflow error = %T %v, want application error", err, err)
	}
	if application.Type() != activity.ErrorTypeAmbiguous || !application.NonRetryable() {
		t.Fatalf("workflow application error = type %q non-retryable %t, want %q true", application.Type(), application.NonRetryable(), activity.ErrorTypeAmbiguous)
	}
	var details activity.SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatalf("decode ambiguous error details: %v", err)
	}
	return details
}

type liveResultStore struct {
	mu   sync.Mutex
	puts int
}

func (*liveResultStore) Get(context.Context, string) (llm.Response, error) {
	return llm.Response{}, engine.ErrResultNotFound
}

func (store *liveResultStore) Put(context.Context, string, llm.Response) (state.BlobRef, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.puts++
	return state.BlobRef{}, errors.New("ambiguous recovery fixture must not persist a completed response")
}

func (store *liveResultStore) Puts() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.puts
}

type liveSuccessfulResultStore struct {
	mu     sync.Mutex
	values map[string]llm.Response
	puts   int
}

func (store *liveSuccessfulResultStore) Get(_ context.Context, operationID string) (llm.Response, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.values[operationID]
	if !ok {
		return llm.Response{}, engine.ErrResultNotFound
	}
	return value, nil
}

func (store *liveSuccessfulResultStore) Put(_ context.Context, operationID string, response llm.Response) (state.BlobRef, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.values == nil {
		store.values = make(map[string]llm.Response)
	}
	store.values[operationID] = response
	store.puts++
	return liveResultRef(operationID), nil
}

func (store *liveSuccessfulResultStore) Puts() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.puts
}

func liveResultRef(operationID string) state.BlobRef {
	digest := sha256.Sum256([]byte("live-result:" + operationID))
	return state.BlobRef{Digest: digest, Size: int64(len(operationID)), Media: "application/json"}
}

type acceptedThenStoppedAdapter struct {
	mu       sync.Mutex
	calls    int
	accepted chan struct{}
}

func newAcceptedThenStoppedAdapter() *acceptedThenStoppedAdapter {
	return &acceptedThenStoppedAdapter{accepted: make(chan struct{})}
}

func (*acceptedThenStoppedAdapter) Name() string { return "content-free-live-recovery" }

func (*acceptedThenStoppedAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{Version: "live-recovery-capabilities-v1", Features: map[provider.Feature]provider.Capability{
		provider.FeatureText:      {State: provider.CapabilityNative},
		provider.FeatureStreaming: {State: provider.CapabilityUnsupported},
	}}, nil
}

func (*acceptedThenStoppedAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	return provider.Call{
		EndpointID: input.Query.EndpointID, Family: input.Query.Family, Model: input.Query.Model,
		OperationKey: input.Request.OperationKey, ServiceClass: input.Query.ServiceClass, Metadata: input.Metadata,
	}, nil
}

func (adapter *acceptedThenStoppedAdapter) Invoke(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	adapter.mu.Lock()
	adapter.calls++
	adapter.mu.Unlock()
	select {
	case <-adapter.accepted:
	default:
		close(adapter.accepted)
	}
	select {
	case <-sdkactivity.GetWorkerStopChannel(ctx):
		return provider.Result{}, provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "fixture worker stopped after accepted write")
	case <-ctx.Done():
		return provider.Result{}, provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "fixture activity ended after accepted write")
	}
}

func (adapter *acceptedThenStoppedAdapter) WaitAccepted(ctx context.Context) error {
	select {
	case <-adapter.accepted:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for accepted fixture dispatch: %w", ctx.Err())
	}
}

func (adapter *acceptedThenStoppedAdapter) Calls() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.calls
}

type delayedSuccessfulAdapter struct {
	mu      sync.Mutex
	delay   time.Duration
	calls   int
	elapsed time.Duration
}

func newDelayedSuccessfulAdapter(delay time.Duration) *delayedSuccessfulAdapter {
	return &delayedSuccessfulAdapter{delay: delay}
}

func (*delayedSuccessfulAdapter) Name() string { return "content-free-live-keepalive" }

func (*delayedSuccessfulAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{Version: "live-recovery-capabilities-v1", Features: map[provider.Feature]provider.Capability{
		provider.FeatureText:      {State: provider.CapabilityNative},
		provider.FeatureStreaming: {State: provider.CapabilityUnsupported},
	}}, nil
}

func (*delayedSuccessfulAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	return provider.Call{
		EndpointID: input.Query.EndpointID, Family: input.Query.Family, Model: input.Query.Model,
		OperationKey: input.Request.OperationKey, ServiceClass: input.Query.ServiceClass, Metadata: input.Metadata,
	}, nil
}

func (adapter *delayedSuccessfulAdapter) Invoke(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	adapter.mu.Lock()
	adapter.calls++
	adapter.mu.Unlock()

	started := time.Now()
	timer := time.NewTimer(adapter.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		if err := observer.AfterResponseHeaders(ctx, provider.ResponseMetadata{RequestID: "live-keepalive-provider-request"}); err != nil {
			return provider.Result{}, err
		}
		adapter.mu.Lock()
		adapter.elapsed = time.Since(started)
		adapter.mu.Unlock()
		return provider.Result{Response: llm.Response{
			Status:   llm.ResponseStatusCompleted,
			Usage:    llm.Usage{OutputTokens: 1},
			Provider: llm.ProviderFacts{RequestID: "live-keepalive-provider-request"},
		}}, nil
	case <-ctx.Done():
		return provider.Result{}, ctx.Err()
	}
}

func (adapter *delayedSuccessfulAdapter) Calls() int {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.calls
}

func (adapter *delayedSuccessfulAdapter) SuccessfulCallElapsed() time.Duration {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.elapsed
}
