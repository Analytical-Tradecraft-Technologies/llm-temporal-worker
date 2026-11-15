package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// A call the worker's own HTTP timeout cuts off after the request was sent,
// on a route with no provider job and no idempotency recovery, cannot be
// learned by waiting out the start lease, and a replacement would run under
// the same timeout. It is a terminal, possibly billed deadline failure rather
// than an unknown outcome that buys a new attempt after every lease.
func TestCloudExecutionUnrecoverableDeadlineIsTerminal(t *testing.T) {
	for name, callErr := range map[string]error{
		"classified deadline": provider.NewError(provider.CodeDeadlineExceeded, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request deadline exceeded"),
		"unclassified client timeout": &provider.Error{Code: provider.CodeProviderUnavailable, Phase: provider.PhaseDispatch, Dispatch: provider.DispatchAmbiguous, Retry: provider.RetrySameOperation,
			SafeMessage: "provider request failed before a response was classified", Cause: fmt.Errorf("read body: %w", context.DeadlineExceeded)},
	} {
		t.Run(name, func(t *testing.T) {
			f, budgets := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) { return provider.Result{}, callErr })
			ctx, v := f.generateStep(t, llm.ExecutionFailed)
			if v.Retryable || v.FailureCode != "provider_error" || v.ErrorCode != string(provider.CodeDeadlineExceeded) || v.Dispatch != string(provider.DispatchAmbiguous) {
				t.Fatalf("failure=%+v", v)
			}
			execution := toolArgumentsExecution(t, f, v.RequestID)
			if execution.Stage != cloudstate.ExecutionFailed || execution.Failure == nil || execution.Failure.Retryable || execution.Settlement == nil || !execution.Settled || budgets.calls.Load() != 1 {
				t.Fatalf("execution stage=%s failure=%+v settled=%t reconciles=%d", execution.Stage, execution.Failure, execution.Settled, budgets.calls.Load())
			}
			// The provider may have billed the request: account the reservation.
			requireReservationAccounted(t, execution)
			requireTerminalAfterRecoveryDeadline(t, ctx, f, budgets)
		})
	}
}

// Cancellation, such as a worker shutdown, is not the route's deadline: the
// attempt stays unknown and is recovered or replaced after the start lease.
func TestCloudExecutionCanceledCallStaysUnknown(t *testing.T) {
	f, _ := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) {
		return provider.Result{}, provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request canceled")
	})
	v := f.step(t, context.Background(), f.request, llm.ExecutionPending)
	if execution := toolArgumentsExecution(t, f, v.RequestID); execution.Stage != cloudstate.ExecutionUnknown {
		t.Fatalf("execution stage=%s failure=%+v", execution.Stage, execution.Failure)
	}
}

// A failed attempt is logged with its safe classification and upstream HTTP
// status, so an operator can tell an upstream error from a timeout. Provider
// text never reaches the log.
func TestCloudExecutionLogsProviderAttemptFailure(t *testing.T) {
	f, _ := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) {
		failure := provider.NewError(provider.CodeProviderUnavailable, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "provider failed during generation")
		failure.SafeDetails = map[string]string{"provider": "openrouter", "status": "502"}
		failure.Cause = fmt.Errorf("upstream said: secret prompt text")
		return provider.Result{}, failure
	})
	v, logged, output := generateLoggingAttemptFailure(t, f)
	// The upstream status is saved with the failure and reported in the result.
	if execution := toolArgumentsExecution(t, f, v.RequestID); execution.Failure == nil || execution.Failure.ProviderStatus != 502 || v.ProviderStatus != 502 {
		t.Fatalf("saved failure=%+v result status=%d", execution.Failure, v.ProviderStatus)
	}
	for key, want := range map[string]string{"level": "WARN", "error_code": "provider_unavailable", "phase": "lift", "dispatch": "accepted", "status": "502", "outcome": string(cloudstate.ExecutionFailed)} {
		if logged[key] != want {
			t.Fatalf("log %s = %v, want %s (entry %v)", key, logged[key], want, logged)
		}
	}
	if bytes.Contains(output.Bytes(), []byte("secret prompt")) {
		t.Fatalf("provider text logged: %s", output.String())
	}
}

// A poll that finds no provider job leaves the attempt unknown with no
// provider error, and is still logged so it is diagnosable.
func TestCloudExecutionLogsProviderJobNotFound(t *testing.T) {
	f := newProviderExecutionFixture(t, "generate", true)
	testassert.NoError(t, testassert.Err(f.submit(context.Background())))
	f.now = f.now.Add(time.Minute)
	f.adapter.poll = func(context.Context, provider.Call, string, provider.Observer) (provider.ResumableResult, error) {
		return provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}, nil
	}
	var output bytes.Buffer
	logger := testLogger(t, observability.LogOptions{Format: "json", Output: &output})
	result, err := f.resume(observability.WithLogger(context.Background(), logger))
	if err != nil || result.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
		t.Fatalf("resume = %+v, %v", result.Saved.Execution, err)
	}
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &entry); err != nil {
		t.Fatalf("log %q: %v", output.String(), err)
	}
	for key, want := range map[string]string{"msg": "provider attempt failed", "cause": "provider_job_not_found", "dispatch": "ambiguous", "outcome": string(cloudstate.ExecutionUnknown)} {
		if entry[key] != want {
			t.Fatalf("log %s = %v, want %s (entry %v)", key, entry[key], want, entry)
		}
	}
}

// A response the adapter rejected while lifting is logged with its closed
// cause and finish reason. A detail that is not a short token is dropped.
func TestCloudExecutionLogsLiftCause(t *testing.T) {
	for name, test := range map[string]struct {
		details     map[string]string
		cause, want string
	}{
		"tagged":    {details: map[string]string{"lift_cause": "json_trailing_text", "finish_reason": "tool_calls"}, cause: "json_trailing_text", want: "tool_calls"},
		"untrusted": {details: map[string]string{"lift_cause": "Here is the prompt", "finish_reason": "stop because of https://x"}},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) {
				failure := provider.NewError(provider.CodeProviderInvalidResponse, provider.PhaseLift, provider.DispatchAccepted, provider.RetryNever, "provider JSON response does not satisfy schema")
				failure.SafeDetails = test.details
				return provider.Result{}, failure
			})
			_, logged, _ := generateLoggingAttemptFailure(t, f)
			if cause, _ := logged["cause"].(string); cause != test.cause {
				t.Fatalf("cause = %q, want %q (entry %v)", cause, test.cause, logged)
			}
			if finish, _ := logged["finish_reason"].(string); finish != test.want {
				t.Fatalf("finish_reason = %q, want %q (entry %v)", finish, test.want, logged)
			}
		})
	}
}

// generateLoggingAttemptFailure runs the fixture's Generate step with a JSON
// logger, expecting it to fail, and returns the result, the last "provider
// attempt failed" log entry and the whole log.
func generateLoggingAttemptFailure(t *testing.T, f *boundedCloudFixture) (llm.ExecutionResultV1, map[string]any, *bytes.Buffer) {
	t.Helper()
	var output bytes.Buffer
	logger := testLogger(t, observability.LogOptions{Format: "json", Output: &output})
	v, err := f.runtime.GenerateStepV1(observability.WithLogger(context.Background(), logger), f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	var logged map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(output.Bytes()), []byte("\n")) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) == nil && entry["msg"] == "provider attempt failed" {
			logged = entry
		}
	}
	if logged == nil {
		t.Fatalf("no provider attempt log in %s", output.String())
	}
	return v, logged, &output
}
