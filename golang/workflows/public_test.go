package workflows

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestPublicGenerateOptionalCompaction(t *testing.T) {
	for _, compact := range []bool{false, true} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		var request llm.GenerateRequestV1
		workflowJSON(t, "generate-root", &request)
		parent := llm.CheckpointHandle("ckp_v1.original")
		request.Parent = &parent
		var generated llm.GenerateResponseV1
		workflowJSON(t, "generate-response", &generated)
		generated.OperationKey = request.OperationKey
		generated.OperationID = testRequestID
		var summary llm.CompactResponseV1
		workflowJSON(t, "compact-response", &summary)
		summary.OperationKey = compactionKey(request)
		summary.OperationID = testRequestID
		planned, compactions, generations := 0, 0, 0
		env.RegisterWorkflowWithOptions(Generate, workflow.RegisterOptions{Name: GenerateWorkflowName})
		env.RegisterWorkflowWithOptions(Compact, workflow.RegisterOptions{Name: CompactWorkflowName})
		registerPlanner(env, func(_ context.Context, input llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
			planned++
			if !reflect.DeepEqual(input, request) {
				t.Error("changed planning input")
			}
			return llm.GenerationPlanV1{CompactBeforeGenerate: compact}, nil
		})
		env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
			if input.Compact != nil {
				compactions++
				if input.Compact.Parent != parent || input.Compact.OperationKey != summary.OperationKey || input.Compact.Cache == nil || input.Compact.Cache.MaxAgeSeconds != 0 {
					t.Error("incorrect compact request")
				}
				return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "compact", State: llm.ExecutionCompleted, Compact: &summary}, nil
			}
			generations++
			if input.OriginalGenerate == nil || !reflect.DeepEqual(input.OriginalGenerate.Parent, request.Parent) {
				t.Error("original public request was not retained")
			}
			expected := request
			if compact {
				next := summary.Checkpoint.Handle
				expected.Parent = &next
			}
			if !reflect.DeepEqual(*input.Generate, expected) {
				t.Error("main changed append/settings or did not use compacted parent")
			}
			// No application tool execution happens at this boundary; return the model
			// response verbatim. The internal workflow validates checkpoint/output.
			return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &generated}, nil
		}, workflow.RegisterOptions{Name: RequestWorkflowName})
		env.ExecuteWorkflow(GenerateWorkflowName, request)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		var got llm.GenerateResponseV1
		if err := env.GetWorkflowResult(&got); err != nil {
			t.Fatal(err)
		}
		expectedCompactions := 0
		if compact {
			expectedCompactions = 1
		}
		if !reflect.DeepEqual(got, generated) || planned != 1 || generations != 1 || compactions != expectedCompactions {
			t.Fatal("unexpected workflow orchestration")
		}
	}
}

func TestPublicGenerateBindingVersionMigration(t *testing.T) {
	for _, version := range []workflow.Version{workflow.DefaultVersion, 1} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		var request llm.GenerateRequestV1
		workflowJSON(t, "generate-root", &request)
		parent := llm.CheckpointHandle("ckp_v1.original")
		request.Parent = &parent
		var summary llm.CompactResponseV1
		workflowJSON(t, "compact-response", &summary)
		summary.OperationKey = compactionKey(request)
		var generated llm.GenerateResponseV1
		workflowJSON(t, "generate-response", &generated)
		generated.OperationKey = request.OperationKey
		generated.OperationID = testRequestID
		compactions := 0
		env.RegisterWorkflowWithOptions(Generate, workflow.RegisterOptions{Name: GenerateWorkflowName})
		registerPlanner(env, func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
			return llm.GenerationPlanV1{CompactBeforeGenerate: true, EffectiveParent: &summary.Checkpoint.Handle}, nil
		})
		env.RegisterWorkflowWithOptions(func(workflow.Context, llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
			compactions++
			return &summary, nil
		}, workflow.RegisterOptions{Name: CompactWorkflowName})
		env.RegisterWorkflowWithOptions(func(_ workflow.Context, input llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
			if input.Generate == nil || input.Generate.Parent == nil || *input.Generate.Parent != summary.Checkpoint.Handle {
				t.Error("wrong effective parent")
			}
			if version == workflow.DefaultVersion && input.OriginalGenerate != nil {
				t.Error("new payload changed old workflow history")
			}
			if version == 1 && (input.OriginalGenerate == nil || !reflect.DeepEqual(input.OriginalGenerate.Parent, request.Parent)) {
				t.Error("missing original request binding")
			}
			return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &generated}, nil
		}, workflow.RegisterOptions{Name: RequestWorkflowName})
		env.OnGetVersion("public-generation-binding-v1", workflow.DefaultVersion, workflow.Version(1)).Return(version)
		env.ExecuteWorkflow(GenerateWorkflowName, request)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		want := 0
		if version == workflow.DefaultVersion {
			want = 1
		}
		if compactions != want {
			t.Fatalf("version %v compacted %d times, want %d", version, compactions, want)
		}
	}
}

func TestPublicWorkflowsUseActualInternalStateMachine(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		submit := activity.GenerateActivityName
		if kind == "compact" {
			submit = activity.CompactActivityName
		}
		f := workflowTest(t, kind, step(activity.PrepareActivityName, llm.ExecutionBudgetRequired), step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired), step(submit, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
		if kind == "generate" {
			f.input.OriginalGenerate = &llm.GenerationOriginV1{Parent: f.input.Generate.Parent}
		}
		// Registered as the worker registers them: all four workflows receive the
		// raw payload, and the caller sends the unchanged v1 wire record.
		f.env.RegisterWorkflowWithOptions(rawWorkflow(activity.PayloadLimits{}, Generate), workflow.RegisterOptions{Name: GenerateWorkflowName})
		f.env.RegisterWorkflowWithOptions(rawWorkflow(activity.PayloadLimits{}, Compact), workflow.RegisterOptions{Name: CompactWorkflowName})
		registerPlanner(f.env, func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
			return llm.GenerationPlanV1{}, nil
		})
		f.env.RegisterDelayedCallback(func() { f.env.CancelWorkflow() }, time.Second)
		name, input := GenerateWorkflowName, wireFixture(t, "generate-root")
		if kind == "compact" {
			name, input = CompactWorkflowName, wireFixture(t, "compact-request")
		}
		f.env.ExecuteWorkflow(name, input)
		if err := f.env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		if len(f.steps) != 0 {
			t.Fatal("public caller abandoned unfinished work")
		}
	}
}

func TestPublicGenerateDoesNotGenerateAfterCompactionFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var request llm.GenerateRequestV1
	workflowJSON(t, "generate-root", &request)
	parent := llm.CheckpointHandle("ckp_v1.original")
	request.Parent = &parent
	env.RegisterWorkflowWithOptions(Generate, workflow.RegisterOptions{Name: GenerateWorkflowName})
	registerPlanner(env, func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
		return llm.GenerationPlanV1{CompactBeforeGenerate: true}, nil
	})
	env.RegisterWorkflowWithOptions(func(workflow.Context, llm.CompactRequestV1) (*llm.CompactResponseV1, error) {
		return nil, temporal.NewNonRetryableApplicationError("summary failed", "incomplete_response", nil)
	}, workflow.RegisterOptions{Name: CompactWorkflowName})
	env.ExecuteWorkflow(GenerateWorkflowName, request)
	if env.GetWorkflowError() == nil {
		t.Fatal("compaction failure accepted")
	}
}

// registerPlanner registers plan as both generation planning Activities: the
// one histories from before resolvedGenerationChange schedule, and the one
// that can also resolve a request, here never finding it complete.
func registerPlanner(env *testsuite.TestWorkflowEnvironment, plan func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error)) {
	env.RegisterActivityWithOptions(plan, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
	env.RegisterActivityWithOptions(func(ctx context.Context, input llm.GenerateRequestV1) (llm.GenerationResolutionV1, error) {
		planned, err := plan(ctx, input)
		return llm.GenerationResolutionV1{Plan: planned}, err
	}, sdkactivity.RegisterOptions{Name: activity.ResolveGenerationActivityName})
}

// resolvedGenerate runs Generate with a resolve Activity that finds the
// request complete. It records the children started and fails the test if
// the request workflow or the old plan Activity runs.
func resolvedGenerate(t *testing.T, alter func(*llm.ExecutionResultV1), version workflow.Version) (*testsuite.TestWorkflowEnvironment, llm.GenerateRequestV1, llm.GenerateResponseV1, *[]llm.ExecutionReferenceV1) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var request llm.GenerateRequestV1
	workflowJSON(t, "generate-root", &request)
	request.Cache = &llm.CachePolicyV1{}
	var generated llm.GenerateResponseV1
	workflowJSON(t, "generate-response", &generated)
	generated.OperationKey, generated.OperationID = request.OperationKey, testRequestID
	Register(env, activity.PayloadLimits{})
	env.OnGetVersion(resolvedGenerationChange, workflow.DefaultVersion, workflow.Version(1)).Return(version)
	env.RegisterActivityWithOptions(func(_ context.Context, input llm.GenerateRequestV1) (llm.GenerationResolutionV1, error) {
		if !reflect.DeepEqual(input, request) {
			t.Error("changed resolution input")
		}
		result := llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &generated, Capability: testCapability}
		if alter != nil {
			alter(&result)
		}
		return llm.GenerationResolutionV1{Result: &result}, nil
	}, sdkactivity.RegisterOptions{Name: activity.ResolveGenerationActivityName})
	env.RegisterActivityWithOptions(func(context.Context, llm.GenerateRequestV1) (llm.GenerationPlanV1, error) {
		return llm.GenerationPlanV1{}, nil
	}, sdkactivity.RegisterOptions{Name: activity.PlanGenerationActivityName})
	env.RegisterActivityWithOptions(func(_ context.Context, input llm.PrepareExecutionV1) (*llm.ExecutionResultV1, error) {
		return &llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &generated, Capability: testCapability}, nil
	}, sdkactivity.RegisterOptions{Name: activity.PrepareActivityName})
	env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	exports := &[]llm.ExecutionReferenceV1{}
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		if info.WorkflowType.Name != LangfuseExportWorkflowName {
			if version != workflow.DefaultVersion {
				t.Errorf("resolved request started %s", info.WorkflowType.Name)
			}
			return
		}
		var ref llm.ExecutionReferenceV1
		if err := args.Get(&ref); err != nil {
			t.Error(err)
		}
		*exports = append(*exports, ref)
	})
	env.ExecuteWorkflow(GenerateWorkflowName, request)
	return env, request, generated, exports
}

// A request that planning finds complete, such as an exact cache hit,
// returns without the request workflow and starts the same Langfuse export
// the request workflow would have.
func TestPublicGenerateResolvedSkipsRequestWorkflow(t *testing.T) {
	env, request, generated, exports := resolvedGenerate(t, nil, 1)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var got llm.GenerateResponseV1
	if err := env.GetWorkflowResult(&got); err != nil {
		t.Fatal(err)
	}
	want := []llm.ExecutionReferenceV1{{RequestID: testRequestID, Context: request.Context, Capability: testCapability}}
	if !reflect.DeepEqual(got, generated) || !reflect.DeepEqual(*exports, want) {
		t.Fatalf("result or export changed: exports %+v", *exports)
	}
}

// A resolved result is checked exactly as the request workflow's result is.
func TestPublicGenerateRejectsMismatchedResolution(t *testing.T) {
	for name, alter := range map[string]func(*llm.ExecutionResultV1){
		"operation key":  func(r *llm.ExecutionResultV1) { r.Generate.OperationKey = "other" },
		"operation id":   func(r *llm.ExecutionResultV1) { r.Generate.OperationID = "other" },
		"no capability":  func(r *llm.ExecutionResultV1) { r.Capability = "" },
		"compact result": func(r *llm.ExecutionResultV1) { r.Kind = "compact" },
	} {
		t.Run(name, func(t *testing.T) {
			env, _, _, exports := resolvedGenerate(t, alter, 1)
			if env.GetWorkflowError() == nil || len(*exports) != 0 {
				t.Fatal("mismatched resolution accepted")
			}
		})
	}
}

// A history from before resolvedGenerationChange keeps scheduling the plan
// Activity and the request workflow.
func TestPublicGenerateBeforeResolutionKeepsRequestWorkflow(t *testing.T) {
	env, _, generated, _ := resolvedGenerate(t, nil, workflow.DefaultVersion)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var got llm.GenerateResponseV1
	if err := env.GetWorkflowResult(&got); err != nil || !reflect.DeepEqual(got, generated) {
		t.Fatalf("result changed: %v", err)
	}
}
