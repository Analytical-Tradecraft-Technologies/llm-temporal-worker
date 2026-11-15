package activity

import (
	"context"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/langfuse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"math/rand/v2"
	"time"
)

const ExportLangfuseActivityName = "llm.ExportLangfuse.v1"

// LangfuseRuntime is optional so an unconfigured sink is a cheap no-op.
type LangfuseRuntime interface {
	ExportLangfuseV1(context.Context, llm.ExecutionReferenceV1) error
}

// langfuseExportFailures maps each classified export error to its metric
// reason, which also names the Temporal error. In order:
//   - A full in-flight limit only means other exports are on the wire. The
//     export workflow waits for nobody and a retry holds no slot while it
//     backs off, so retry after a jittered delay rather than drop the trace
//     (#1459). The jitter spreads a burst that hit the limit together.
//   - A throttled Langfuse is healthy and asked for a slower rate, so the
//     export backs off the same way rather than dropping the trace (#1670).
//   - An open breaker sent nothing and signals a Langfuse outage; it stays a
//     fast-fail so an outage cannot pile retries up behind the cooldown.
//   - Oversized, redirected and rejected exports are never retried.
var langfuseExportFailures = []struct {
	err     error
	reason  string
	backoff bool
}{
	{langfuse.ErrSaturated, "saturated", true},
	{langfuse.ErrThrottled, "throttled", true},
	{langfuse.ErrUnavailable, "unavailable", false},
	{langfuse.ErrOversized, "oversized", false},
	{langfuse.ErrRedirected, "redirected", false},
	{langfuse.ErrRejected, "rejected", false},
}

func (a *Activities) exportLangfuseTemporal(ctx context.Context, input converter.RawValue) error {
	ref, err := decodeV1ActivityInput[llm.ExecutionReferenceV1](ctx, a, input)
	if err != nil {
		return err
	}
	if a == nil {
		return nil
	}
	runtime, ok := a.V1Runtime.(LangfuseRuntime)
	if !ok {
		return nil
	}
	if err := runtime.ExportLangfuseV1(ctx, ref); err != nil {
		for _, failure := range langfuseExportFailures {
			if !errors.Is(err, failure.err) {
				continue
			}
			a.Metrics.RecordLangfuseExportFailure(failure.reason)
			message, kind := "Langfuse export "+failure.reason, "langfuse_export_"+failure.reason
			if failure.backoff {
				return temporal.NewApplicationErrorWithOptions(message, kind, temporal.ApplicationErrorOptions{NextRetryDelay: saturatedRetryDelay(sdkactivity.GetInfo(ctx).Attempt)})
			}
			return temporal.NewNonRetryableApplicationError(message, kind, nil)
		}
		a.Metrics.RecordLangfuseExportFailure("failed")
		return temporal.NewApplicationError("Langfuse export failed", "langfuse_export_failed")
	}
	return nil
}

// saturatedRetryDelay doubles from about one second per attempt up to about
// eight, choosing uniformly within the upper half of each step.
func saturatedRetryDelay(attempt int32) time.Duration {
	step := 8 * time.Second
	if attempt >= 1 && attempt <= 4 {
		step = time.Second << (attempt - 1)
	}
	return step/2 + rand.N(step/2+1)
}
