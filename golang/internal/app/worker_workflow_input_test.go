package app_test

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	domainactivity "github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/workflows"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// workflowEnvironmentRegistry forwards the worker's workflow registrations to
// a Temporal test environment, so the functions NewWorker registers are the
// ones executed.
type workflowEnvironmentRegistry struct {
	fakeRegistry
	env *testsuite.TestWorkflowEnvironment
}

func (registry *workflowEnvironmentRegistry) RegisterWorkflowWithOptions(fn interface{}, options workflow.RegisterOptions) {
	registry.env.RegisterWorkflowWithOptions(fn, options)
}

func generateWorkflowWire(t *testing.T, text string) json.RawMessage {
	t.Helper()
	data, err := os.ReadFile("../../llm/testdata/v1/generate-root.json")
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	fields["append"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] = text
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// The worker must register workflows that classify an invalid public request
// themselves, using the configured inline payload limit.
func TestWorkerRegisteredWorkflowsRejectInvalidInputAsTypedNonRetryable(t *testing.T) {
	const marker = "SECRET customer text!"
	limits := domainactivity.PayloadLimits{MaxInlineBytes: 4096}
	run := func(t *testing.T, input any) error {
		t.Helper()
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		_, err := app.NewWorker(app.WorkerOptions{
			TaskQueue: "queue-a", MaxConcurrentActivities: 1, MaxConcurrentActivityTaskPolls: 1, GracefulStopTimeout: time.Second,
			Activities: &domainactivity.Activities{PayloadLimits: limits, V1Runtime: registrationRuntime{}},
			Factory: func(client.Client, string, worker.Options) (app.WorkerController, app.WorkerRegistry, error) {
				return &fakeWorker{}, &workflowEnvironmentRegistry{env: env}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		env.ExecuteWorkflow(workflows.GenerateWorkflowName, input)
		if !env.IsWorkflowCompleted() {
			t.Fatal("workflow did not complete")
		}
		return env.GetWorkflowError()
	}
	invalid := map[string]json.RawMessage{
		"unknown field": json.RawMessage(strings.Replace(string(generateWorkflowWire(t, "hi")), "{", `{"transcript":"`+marker+`",`, 1)),
		// Valid and below the default limit: only the configured limit rejects it.
		"over configured limit": generateWorkflowWire(t, marker+strings.Repeat("x", 2*limits.MaxInlineBytes)),
	}
	for name, input := range invalid {
		t.Run(name, func(t *testing.T) {
			err := run(t, input)
			var applicationErr *temporal.ApplicationError
			if !errors.As(err, &applicationErr) {
				t.Fatalf("error = %T %v, want ApplicationError", err, err)
			}
			if applicationErr.Type() != domainactivity.ErrorTypeInvalidArgument || !applicationErr.NonRetryable() || !applicationErr.HasDetails() {
				t.Fatalf("error type=%q nonRetryable=%v (%v)", applicationErr.Type(), applicationErr.NonRetryable(), err)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("error %q echoes caller content", err)
			}
		})
	}
	// A valid request passes the input boundary; no Activity is registered in
	// this environment, so it fails later with a different error.
	err := run(t, generateWorkflowWire(t, "hi"))
	var applicationErr *temporal.ApplicationError
	if err == nil || (errors.As(err, &applicationErr) && applicationErr.Type() == domainactivity.ErrorTypeInvalidArgument) {
		t.Fatalf("valid request was rejected at the input boundary: %v", err)
	}
}
