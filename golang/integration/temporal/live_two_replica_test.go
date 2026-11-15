//go:build composeliveintegration

package temporal_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	redisclient "github.com/redis/go-redis/v9"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.yaml.in/yaml/v4"
)

const (
	twoReplicaWorkflowName    = "llmtw.integration.compose-two-replica.v1"
	twoReplicaBatchSize       = 8
	twoReplicaMaxBatches      = 10
	twoReplicaPollerDeadline  = 60 * time.Second
	twoReplicaActivityTimeout = 20 * time.Second
	twoReplicaOutcomeComplete = "completed"
)

// TestComposeTwoWorkerReplicasShareTaskQueueAndRedis runs against the worker
// Compose profile scaled to two containers (`--scale worker=2`). Both
// replicas use the same checked-in configuration, so they poll the same task
// queue and share the one Redis service and blob volume. The test proves:
//
//   - Temporal reports an Activity poller for each running replica container
//     on that one task queue;
//   - requests dispatched to that queue are started by both replicas and every
//     one reaches a terminal outcome rather than stalling; and
//   - each request is admitted through the shared Redis, leaving exactly one
//     admission operation there, whichever replica ran it.
//
// The local profile's provider mock is Docker-private and fails the
// production egress policy, so no provider call is compiled for it: after
// admission each request ends with a definite, non-dispatched no-route
// refusal rather than model output. Inference with two workers is covered by
// the in-process suites; this test covers the deployed two-container shape.
func TestComposeTwoWorkerReplicasShareTaskQueueAndRedis(t *testing.T) {
	temporalAddress := os.Getenv("LLMTW_TEMPORAL_ADDRESS")
	containers := strings.Fields(os.Getenv("LLMTW_COMPOSE_WORKER_CONTAINERS"))
	if temporalAddress == "" || len(containers) == 0 {
		t.Skip("make compose-live-integration supplies the Temporal address and scaled worker containers")
	}
	if len(containers) != 2 {
		t.Fatalf("worker containers = %d, want two Compose replicas", len(containers))
	}
	settings := readLocalTemporalSettings(t)
	replicaIdentities := make(map[string]string, len(containers))
	for _, container := range containers {
		// Docker names a container's host after its short ID; the worker
		// identity is <identity_prefix>-<hostname>-<pid>.
		if len(container) < 12 {
			t.Fatalf("worker container ID %q is shorter than a Docker short ID", container)
		}
		replicaIdentities[settings.IdentityPrefix+"-"+container[:12]+"-"] = container[:12]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	workflowClient, err := client.Dial(client.Options{HostPort: temporalAddress, Namespace: settings.Namespace})
	if err != nil {
		t.Fatalf("dial Temporal: %v", err)
	}
	t.Cleanup(workflowClient.Close)

	waitForReplicaPollers(t, ctx, workflowClient, settings.TaskQueue, replicaIdentities)

	// The workflow runs in this process on a private queue. It only schedules
	// the Activity onto the replicas' queue; this process registers no
	// Activity, so every Activity task is executed by a container.
	queue := fmt.Sprintf("llmtw-live-two-replica-%d", time.Now().UnixNano())
	orchestrator := worker.New(workflowClient, queue, worker.Options{
		Identity:                               "llmtw-live-two-replica-orchestrator",
		MaxConcurrentWorkflowTaskExecutionSize: liveRecoveryWorkflowTaskExecutionSize,
		WorkerStopTimeout:                      5 * time.Second,
	})
	orchestrator.RegisterWorkflowWithOptions(twoReplicaWorkflow, workflow.RegisterOptions{Name: twoReplicaWorkflowName})
	if err := orchestrator.Start(); err != nil {
		t.Fatalf("start two-replica orchestrator: %v", err)
	}
	t.Cleanup(orchestrator.Stop)

	redisClient := newLiveRedisClient(t, ctx)
	operationPattern := liveRedisKeyPrefix() + ":{admission}:operation:*"
	operationsBefore := countRedisKeys(t, ctx, redisClient, operationPattern)

	startedBy := map[string]int{}
	requests := 0
	for batch := 0; batch < twoReplicaMaxBatches && len(startedBy) < len(replicaIdentities); batch++ {
		input := twoReplicaInput{TaskQueue: settings.TaskQueue}
		for index := 0; index < twoReplicaBatchSize; index++ {
			input.Requests = append(input.Requests, activity.GenerateRequest{
				APIVersion: activity.APIVersion,
				Request: llm.Request{
					OperationKey: fmt.Sprintf("%s-%d-%d", queue, batch, index),
					Context:      llm.RequestContext{Tenant: "local"},
					Model:        "demo-model",
					Input:        []llm.Item{llm.HumanText("content-free two-replica smoke")},
				},
			})
		}
		run, err := workflowClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       fmt.Sprintf("%s-batch-%d", queue, batch),
			TaskQueue:                queue,
			WorkflowExecutionTimeout: time.Minute,
			WorkflowTaskTimeout:      5 * time.Second,
		}, twoReplicaWorkflowName, input)
		if err != nil {
			t.Fatalf("start two-replica batch %d: %v", batch, err)
		}
		var outcomes []twoReplicaOutcome
		if err := run.Get(ctx, &outcomes); err != nil {
			t.Fatalf("two-replica batch %d did not complete: %v", batch, err)
		}
		if len(outcomes) != len(input.Requests) {
			t.Fatalf("two-replica batch %d returned %d outcomes, want %d", batch, len(outcomes), len(input.Requests))
		}
		for index, outcome := range outcomes {
			if !outcome.expected() {
				t.Fatalf("two-replica batch %d request %d outcome = %#v, want completed or the local profile's non-dispatched no-route refusal", batch, index, outcome)
			}
		}
		requests += len(outcomes)
		for _, identity := range activityStartIdentities(t, ctx, workflowClient, run) {
			replica, ok := replicaForIdentity(replicaIdentities, identity)
			if !ok {
				t.Fatalf("Activity started by %q, which is not a Compose worker replica %v", identity, replicaIdentities)
			}
			startedBy[replica]++
		}
	}
	if len(startedBy) != len(replicaIdentities) {
		t.Fatalf("Activities started per replica = %v after %d requests, want both replicas", startedBy, requests)
	}
	if operations := countRedisKeys(t, ctx, redisClient, operationPattern) - operationsBefore; operations != requests {
		t.Fatalf("shared Redis gained %d admission operations for %d requests, want one each", operations, requests)
	}
	t.Logf("%d requests reached a terminal outcome across replicas %v", requests, startedBy)
}

// newLiveRedisClient connects to the Compose Redis from the environment that
// make compose-live-integration supplies and closes it at test cleanup.
func newLiveRedisClient(t *testing.T, ctx context.Context) *redisclient.Client {
	t.Helper()
	address := os.Getenv("LLMTW_REDIS_ADDR")
	username := os.Getenv("LLMTW_REDIS_USERNAME")
	password := os.Getenv("LLMTW_REDIS_PASSWORD")
	if address == "" || username == "" || password == "" {
		t.Skip("make compose-live-integration supplies the authenticated Compose Redis address")
	}
	value := redisclient.NewClient(&redisclient.Options{Addr: address, Username: username, Password: password})
	t.Cleanup(func() { _ = value.Close() })
	if err := value.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Compose Redis: %v", err)
	}
	return value
}

func countRedisKeys(t *testing.T, ctx context.Context, value *redisclient.Client, pattern string) int {
	t.Helper()
	count := 0
	iterator := value.Scan(ctx, 0, pattern, 256).Iterator()
	for iterator.Next(ctx) {
		count++
	}
	if err := iterator.Err(); err != nil {
		t.Fatalf("scan Compose Redis keys: %v", err)
	}
	return count
}

type localTemporalSettings struct {
	Namespace      string `yaml:"namespace"`
	TaskQueue      string `yaml:"task_queue"`
	IdentityPrefix string `yaml:"identity_prefix"`
}

// readLocalTemporalSettings reads the queue and identity that the Compose
// worker replicas run with, so the test cannot drift from the fixture.
func readLocalTemporalSettings(t *testing.T) localTemporalSettings {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "local", "config.yaml"))
	if err != nil {
		t.Fatalf("read local worker configuration: %v", err)
	}
	var document struct {
		Temporal localTemporalSettings `yaml:"temporal"`
	}
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse local worker configuration: %v", err)
	}
	settings := document.Temporal
	if settings.Namespace == "" || settings.TaskQueue == "" || settings.IdentityPrefix == "" {
		t.Fatalf("local worker Temporal settings = %#v, want namespace, task queue and identity prefix", settings)
	}
	return settings
}

// waitForReplicaPollers waits until Temporal reports an Activity poller on
// taskQueue for every replica identity prefix.
func waitForReplicaPollers(t *testing.T, ctx context.Context, workflowClient client.Client, taskQueue string, replicaIdentities map[string]string) {
	t.Helper()
	deadline := time.Now().Add(twoReplicaPollerDeadline)
	var seen []string
	for {
		response, err := workflowClient.DescribeTaskQueue(ctx, taskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		if err != nil {
			t.Fatalf("describe task queue %q: %v", taskQueue, err)
		}
		seen = seen[:0]
		polling := map[string]bool{}
		for _, poller := range response.GetPollers() {
			seen = append(seen, poller.GetIdentity())
			if replica, ok := replicaForIdentity(replicaIdentities, poller.GetIdentity()); ok {
				polling[replica] = true
			}
		}
		if len(polling) == len(replicaIdentities) {
			return
		}
		if time.Now().After(deadline) {
			sort.Strings(seen)
			t.Fatalf("Activity pollers on %q = %v, want one per replica %v", taskQueue, seen, replicaIdentities)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for replica pollers: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func replicaForIdentity(replicaIdentities map[string]string, identity string) (string, bool) {
	for prefix, replica := range replicaIdentities {
		if strings.HasPrefix(identity, prefix) {
			return replica, true
		}
	}
	return "", false
}

// activityStartIdentities returns the worker identity recorded on every
// ActivityTaskStarted event of run.
func activityStartIdentities(t *testing.T, ctx context.Context, workflowClient client.Client, run client.WorkflowRun) []string {
	t.Helper()
	var identities []string
	history := workflowClient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for history.HasNext() {
		event, err := history.Next()
		if err != nil {
			t.Fatalf("read workflow history: %v", err)
		}
		if started := event.GetActivityTaskStartedEventAttributes(); started != nil {
			identities = append(identities, started.GetIdentity())
		}
	}
	return identities
}

type twoReplicaInput struct {
	TaskQueue string                     `json:"task_queue"`
	Requests  []activity.GenerateRequest `json:"requests"`
}

// twoReplicaOutcome is one request's terminal Activity outcome: Type is
// "completed" or the Activity's application error type.
type twoReplicaOutcome struct {
	Type    string                    `json:"type"`
	Details activity.SafeErrorDetails `json:"details"`
}

// expected reports whether the outcome is a success or the refusal the local
// profile gives once admission has reserved in Redis: it compiles no call for
// its Docker-private provider mock, so the request ends with a definite,
// non-dispatched no-route failure and no provider write.
func (outcome twoReplicaOutcome) expected() bool {
	if outcome.Type == twoReplicaOutcomeComplete {
		return true
	}
	return outcome.Type == activity.ErrorTypeInvalidArgument &&
		outcome.Details.Code == string(provider.CodeNoRoute) &&
		outcome.Details.Phase == string(provider.PhaseCompile) &&
		outcome.Details.Dispatch == string(provider.DispatchNotDispatched)
}

// twoReplicaWorkflow schedules every request concurrently on the replicas'
// task queue, once each, and returns each terminal outcome.
func twoReplicaWorkflow(ctx workflow.Context, input twoReplicaInput) ([]twoReplicaOutcome, error) {
	options := workflow.ActivityOptions{
		TaskQueue:              input.TaskQueue,
		StartToCloseTimeout:    twoReplicaActivityTimeout,
		ScheduleToCloseTimeout: twoReplicaActivityTimeout,
		HeartbeatTimeout:       10 * time.Second,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	futures := make([]workflow.Future, len(input.Requests))
	for index, request := range input.Requests {
		futures[index] = workflow.ExecuteActivity(ctx, activity.LegacyGenerateActivityName, request)
	}
	outcomes := make([]twoReplicaOutcome, len(futures))
	for index, future := range futures {
		err := future.Get(ctx, nil)
		var application *temporal.ApplicationError
		switch {
		case err == nil:
			outcomes[index] = twoReplicaOutcome{Type: twoReplicaOutcomeComplete}
		case errors.As(err, &application):
			outcome := twoReplicaOutcome{Type: application.Type()}
			if application.HasDetails() {
				if err := application.Details(&outcome.Details); err != nil {
					return nil, fmt.Errorf("decode request %d Activity error details: %w", index, err)
				}
			}
			outcomes[index] = outcome
		default:
			return nil, fmt.Errorf("request %d did not reach a terminal Activity outcome: %w", index, err)
		}
	}
	return outcomes, nil
}
