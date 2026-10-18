package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
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
			ctx := context.Background()
			v, err := f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if v.Retryable || v.FailureCode != "provider_error" || v.ErrorCode != string(provider.CodeDeadlineExceeded) || v.Dispatch != string(provider.DispatchAmbiguous) {
				t.Fatalf("failure=%+v", v)
			}
			execution := toolArgumentsExecution(t, f, v.RequestID)
			if execution.Stage != cloudstate.ExecutionFailed || execution.Failure == nil || execution.Failure.Retryable || execution.Settlement == nil || !execution.Settled || budgets.calls.Load() != 1 {
				t.Fatalf("execution stage=%s failure=%+v settled=%t reconciles=%d", execution.Stage, execution.Failure, execution.Settled, budgets.calls.Load())
			}
			// The provider may have billed the request: account the reservation.
			for _, event := range execution.Settlement.Events {
				if event.CostStatus != budget.CostUnknown || event.AccountedIncreaseUSD.String() != event.ReservedDecreaseUSD.String() {
					t.Fatalf("settlement event=%+v", event)
				}
			}
			f.now = f.now.Add(16 * time.Minute)
			f.restart(t)
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
			boundedState(t, v, err, llm.ExecutionFailed)
			if f.submits.Load() != 1 || budgets.calls.Load() != 1 {
				t.Fatalf("submits=%d reconciles=%d, want one each", f.submits.Load(), budgets.calls.Load())
			}
		})
	}
}

// Cancellation, such as a worker shutdown, is not the route's deadline: the
// attempt stays unknown and is recovered or replaced after the start lease.
func TestCloudExecutionCanceledCallStaysUnknown(t *testing.T) {
	f, _ := toolArgumentsCloud(t, func(provider.Call) (provider.Result, error) {
		return provider.Result{}, provider.NewError(provider.CodeCanceled, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, "provider request canceled")
	})
	v, err := f.runtime.GenerateStepV1(context.Background(), f.request)
	boundedState(t, v, err, llm.ExecutionPending)
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
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := f.submit(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(time.Minute)
	f.adapter.poll = func(context.Context, provider.Call, string, provider.Observer) (provider.ResumableResult, error) {
		return provider.ResumableResult{State: provider.ResumableNotFound, Dispatch: provider.DispatchAmbiguous}, nil
	}
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
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
