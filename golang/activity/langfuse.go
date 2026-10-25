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
		// A full in-flight limit only means other exports are on the wire. The
		// export workflow waits for nobody and a retry holds no slot while it
		// backs off, so retry after a jittered delay rather than drop the trace
		// (#1459). The jitter spreads a burst that hit the limit together.
		if errors.Is(err, langfuse.ErrSaturated) {
			a.Metrics.RecordLangfuseExportFailure("saturated")
			return temporal.NewApplicationErrorWithOptions("Langfuse export saturated", "langfuse_export_saturated", temporal.ApplicationErrorOptions{NextRetryDelay: saturatedRetryDelay(sdkactivity.GetInfo(ctx).Attempt)})
		}
		// A throttled Langfuse is healthy and asked for a slower rate, so the
		// export backs off the same way rather than dropping the trace (#1670).
		if errors.Is(err, langfuse.ErrThrottled) {
			a.Metrics.RecordLangfuseExportFailure("throttled")
			return temporal.NewApplicationErrorWithOptions("Langfuse export throttled", "langfuse_export_throttled", temporal.ApplicationErrorOptions{NextRetryDelay: saturatedRetryDelay(sdkactivity.GetInfo(ctx).Attempt)})
		}
		// An open breaker sent nothing and signals a Langfuse outage; it stays
		// a fast-fail so an outage cannot pile retries up behind the cooldown.
		if errors.Is(err, langfuse.ErrUnavailable) {
			a.Metrics.RecordLangfuseExportFailure("unavailable")
			return temporal.NewNonRetryableApplicationError("Langfuse export unavailable", "langfuse_export_unavailable", nil)
		}
		if errors.Is(err, langfuse.ErrOversized) {
			a.Metrics.RecordLangfuseExportFailure("oversized")
			return temporal.NewNonRetryableApplicationError("Langfuse export oversized", "langfuse_export_oversized", nil)
		}
		if errors.Is(err, langfuse.ErrRedirected) {
			a.Metrics.RecordLangfuseExportFailure("redirected")
			return temporal.NewNonRetryableApplicationError("Langfuse export redirected", "langfuse_export_redirected", nil)
		}
		if errors.Is(err, langfuse.ErrRejected) {
			a.Metrics.RecordLangfuseExportFailure("rejected")
			return temporal.NewNonRetryableApplicationError("Langfuse export rejected", "langfuse_export_rejected", nil)
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
