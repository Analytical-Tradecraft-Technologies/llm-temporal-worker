package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	commonpb "go.temporal.io/api/common/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// unescapedRequest encodes a v1 fixture whose first appended text is text the
// way the OCaml client (Yojson.Safe.to_string) does: <, > and & stay literal,
// so the caller's payload is smaller than this worker's re-encoding.
func unescapedRequest(t *testing.T, fixture, text string) converter.RawValue {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(wireFixture(t, fixture), &fields); err != nil {
		t.Fatal(err)
	}
	fields["append"] = []any{map[string]any{"kind": "message", "actor": "human", "content": []any{map[string]any{"kind": "text", "text": text}}}}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(fields); err != nil {
		t.Fatal(err)
	}
	return converter.NewRawValue(&commonpb.Payload{
		Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)},
		Data:     bytes.TrimSuffix(encoded.Bytes(), []byte("\n")),
	})
}

type reencodeRun struct {
	planned, prepared bool
	err               error
}

// runReencoded starts Generate as the worker does: registered by Register and
// encoding every worker-side payload through the bounded converter. The plan
// Activity returns a maximum-length effective parent when the request has a
// parent, the largest envelope the internal child can receive.
func runReencoded(t *testing.T, limits activity.PayloadLimits, input converter.RawValue) reencodeRun {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(activity.BoundedDataConverter(limits))
	Register(env, limits)
	var run reencodeRun
	env.RegisterActivityWithOptions(func(_ context.Context, request llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
		run.planned = true
		if request.Parent == nil {
			return llm.GenerationPlanV1{}, nil
		}
		issued := llm.CheckpointHandle(strings.Repeat("y", llm.CheckpointHandleMaxCodePoints))
		return llm.GenerationPlanV1{CompactBeforeGenerate: true, EffectiveParent: &issued}, nil
	}, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
	env.RegisterActivityWithOptions(func(context.Context, llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
		run.prepared = true
		return nil, temporal.NewNonRetryableApplicationError("prepared", "test_prepared", nil)
	}, sdkactivity.RegisterOptions{Name: activity.PrepareActivityName})
	env.ExecuteWorkflow(GenerateWorkflowName, input)
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	run.err = env.GetWorkflowError()
	return run
}

func TestGenerateRejectsUnescapedHTMLThatReencodesPastTheLimit(t *testing.T) {
	limits := activity.PayloadLimits{MaxInlineBytes: 64 << 10}
	html := "<p>Claim &amp; <b>notes</b></p>"
	for _, fixture := range []string{"generate-root", "generate-delta"} {
		t.Run(fixture, func(t *testing.T) {
			// About 60% of the limit as sent, more than the limit once Go escapes it.
			oversize := unescapedRequest(t, fixture, strings.Repeat(html, limits.MaxInlineBytes*6/10/len(html)))
			if len(oversize.Payload().GetData()) > limits.MaxInlineBytes {
				t.Fatalf("caller payload is %d bytes, want within the %d-byte limit", len(oversize.Payload().GetData()), limits.MaxInlineBytes)
			}
			run := runReencoded(t, limits, oversize)
			if run.planned || run.prepared {
				t.Fatalf("planned=%v prepared=%v, want rejection before any Activity", run.planned, run.prepared)
			}
			assertTypedInvalidInput(t, run.err)

			fits := runReencoded(t, limits, unescapedRequest(t, fixture, strings.Repeat(html, limits.MaxInlineBytes/10/len(html))))
			if !fits.planned || !fits.prepared {
				t.Fatalf("planned=%v prepared=%v err=%v, want a fitting HTML request to reach preparation", fits.planned, fits.prepared, fits.err)
			}
		})
	}
}

// Every request accepted at decode must reach llm.request.prepare.v1: the
// sweep crosses the acceptance boundary for plain and HTML-heavy text, with
// and without a parent handle.
func TestGenerateAcceptedNearLimitRequestsReachPreparation(t *testing.T) {
	limits := activity.PayloadLimits{MaxInlineBytes: 16 << 10}
	for _, fixture := range []string{"generate-root", "generate-delta"} {
		for _, char := range []string{"x", "<"} {
			t.Run(fixture+"/"+char, func(t *testing.T) {
				base := len(unescapedRequest(t, fixture, "").Payload().GetData())
				// Go escapes each "<" to six bytes, so its boundary sits lower.
				width := (limits.MaxInlineBytes - base) / map[string]int{"x": 1, "<": 6}[char]
				accepted, rejected := 0, 0
				for n := width - 1500; n <= width; n += 5 {
					run := runReencoded(t, limits, unescapedRequest(t, fixture, strings.Repeat(char, n)))
					if !run.planned {
						assertTypedInvalidInput(t, run.err)
						rejected++
						continue
					}
					if !run.prepared {
						t.Fatalf("text of %d bytes was accepted but did not reach preparation: %v", n, run.err)
					}
					accepted++
				}
				if accepted == 0 || rejected == 0 {
					t.Fatalf("accepted=%d rejected=%d, want the sweep to cross the boundary", accepted, rejected)
				}
			})
		}
	}
}
