package activity

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/langfuse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type langfuseRuntimeStub struct {
	v1RuntimeStub
	err error
}

func (s *langfuseRuntimeStub) ExportLangfuseV1(context.Context, llm.ExecutionReferenceV1) error {
	return s.err
}

func TestExportLangfuseRetriesOnlyDeliveryFailures(t *testing.T) {
	payload, err := json.Marshal(llm.ExecutionReferenceV1{RequestID: executionTestID, Context: validGenerateV1Request().Context})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err          error
		kind         string
		nonRetryable bool
	}{
		{langfuse.ErrExport, "langfuse_export_failed", false},
		{langfuse.ErrRejected, "langfuse_export_rejected", true},
		{langfuse.ErrUnavailable, "langfuse_export_unavailable", true},
	} {
		activities := &Activities{V1Runtime: &langfuseRuntimeStub{err: tc.err}}
		var suite testsuite.WorkflowTestSuite
		environment := suite.NewTestActivityEnvironment()
		environment.RegisterActivityWithOptions(activities.exportLangfuseTemporal, sdkactivity.RegisterOptions{Name: ExportLangfuseActivityName})
		_, err := environment.ExecuteActivity(ExportLangfuseActivityName, rawJSONArgument(t, payload))
		var applicationErr *temporal.ApplicationError
		if !errors.As(err, &applicationErr) || applicationErr.Type() != tc.kind || applicationErr.NonRetryable() != tc.nonRetryable {
			t.Fatalf("%v: got %v, want %s nonRetryable=%v", tc.err, err, tc.kind, tc.nonRetryable)
		}
	}
}
