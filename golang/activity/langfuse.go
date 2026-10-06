package activity

import (
	"context"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/langfuse"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
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
		if errors.Is(err, langfuse.ErrRejected) {
			return temporal.NewNonRetryableApplicationError("Langfuse export rejected", "langfuse_export_rejected", nil)
		}
		return temporal.NewApplicationError("Langfuse export failed", "langfuse_export_failed")
	}
	return nil
}
