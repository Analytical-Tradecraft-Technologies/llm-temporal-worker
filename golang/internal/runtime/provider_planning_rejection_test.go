package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func rejectedSelection(t *testing.T, err error, code provider.Code, phase provider.Phase) map[string]string {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Phase != phase || mapped.Dispatch != provider.DispatchNotDispatched ||
		mapped.Retry != provider.RetryNever || mapped.Cause != nil || mapped.SafeMessage != "provider planning failed" {
		t.Fatalf("selection error = %#v, want %s at %s", err, code, phase)
	}
	for key, value := range mapped.SafeDetails {
		if strings.Contains(key+value, "sensitive") {
			t.Fatalf("selection details leaked content: %v", mapped.SafeDetails)
		}
	}
	return mapped.SafeDetails
}

// TestCloudExecutionReportsUnsupportedCapabilityForRejectedRoutes drives the
// composed cloud runtime with each real adapter that cannot keep instruction
// levels apart. A strict request that mixes them has a configured route, so
// the failure must say the request is unsupported and name the route.
func TestCloudExecutionReportsUnsupportedCapabilityForRejectedRoutes(t *testing.T) {
	for _, route := range hierarchyRoutes() {
		t.Run(route.name, func(t *testing.T) {
			f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
				route.configure(b)
				b.prices(t, []pricing.Entry{b.entry})
			})
			f.cap.Adapters = engine.AdapterMap{"endpoint": hierarchyInvokeAdapter{Adapter: route.adapter(t), invoke: f.adapter.invoke}}
			f.restart(t)
			f.request.SettingsPatch.Instructions.Set = &[]llm.Instruction{
				{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "sensitive policy"}, hierarchyApplicationInstruction}
			var logs bytes.Buffer
			logger, err := observability.NewLogger(observability.LogOptions{Output: &logs})
			if err != nil {
				t.Fatal(err)
			}
			ctx := observability.WithLogger(context.Background(), logger)
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
			if err == nil {
				boundedState(t, v, err, llm.ExecutionBudgetRequired)
				_, err = f.runtime.AcquireBudgetV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context})
			}
			details := rejectedSelection(t, err, provider.CodeUnsupportedCapability, provider.PhaseCompile)
			// The fixture's priority class has no route; the compile rejection
			// of the standard fallback explains the code and comes first.
			if details["rejected_routes"] != "2" || details["route_1"] != "route" || details["reason_1"] != "route_compile_rejected" || details["reason_2"] != routing.RejectClass {
				t.Fatalf("details = %v", details)
			}
			if f.submits.Load() != 0 {
				t.Fatal("rejected request reached the provider")
			}
			line := logs.String()
			if strings.Count(line, "\n") != 2 || !strings.Contains(line, `"route_id":"route"`) || !strings.Contains(line, `"cause":"route_compile_rejected"`) ||
				!strings.Contains(line, `"error_code":"unsupported_capability"`) || strings.Contains(line, "sensitive") || strings.Contains(line, "instruction") {
				t.Fatalf("rejection log = %q", line)
			}
		})
	}
}

func TestProviderPlanningClassifiesExhaustedSelection(t *testing.T) {
	unsupported := func(provider.CompileInput) (provider.Call, error) {
		return provider.Call{}, provider.NewError(provider.CodeUnsupportedCapability, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "structured_output: sensitive compiler error")
	}
	for _, test := range []struct {
		name      string
		configure func(*planningSource, *planningAdapter, *llm.GenerateRequestV1)
		code      provider.Code
		phase     provider.Phase
		details   map[string]string
	}{
		{name: "compiler names the feature", configure: func(_ *planningSource, adapter *planningAdapter, _ *llm.GenerateRequestV1) {
			adapter.compile = unsupported
		}, code: provider.CodeUnsupportedCapability, phase: provider.PhaseCompile,
			details: map[string]string{"rejected_routes": "1", "route_1": "route", "reason_1": rejectCompile + ":structured_output"}},
		{name: "compiler message is not a feature", configure: func(_ *planningSource, adapter *planningAdapter, _ *llm.GenerateRequestV1) {
			adapter.compile = func(provider.CompileInput) (provider.Call, error) {
				return provider.Call{}, provider.NewError(provider.CodeInvalidArgument, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, "sensitive: compiler error")
			}
		}, code: provider.CodeUnsupportedCapability, phase: provider.PhaseCompile,
			details: map[string]string{"rejected_routes": "1", "route_1": "route", "reason_1": rejectCompile}},
		{name: "planner rejects the capability", configure: func(source *planningSource, _ *planningAdapter, _ *llm.GenerateRequestV1) {
			source.value.Routes.Models["alias"].Routes[0].Capabilities.Features[routing.FeatureText] = routing.Capability{State: routing.CapabilityUnsupported}
		}, code: provider.CodeUnsupportedCapability, phase: provider.PhasePlan,
			details: map[string]string{"rejected_routes": "1", "route_1": "route", "reason_1": routing.RejectCapability + ":text"}},
		{name: "adapter is unavailable", configure: func(_ *planningSource, adapter *planningAdapter, _ *llm.GenerateRequestV1) {
			adapter.compile = func(provider.CompileInput) (provider.Call, error) {
				return provider.Call{}, errors.New("sensitive failure")
			}
		}, code: provider.CodeNoRoute, phase: provider.PhaseCompile,
			details: map[string]string{"rejected_routes": "1", "route_1": "route", "reason_1": rejectAdapterUnavailable}},
		{name: "route is disabled", configure: func(source *planningSource, _ *planningAdapter, _ *llm.GenerateRequestV1) {
			source.value.Health.Routes = map[string]routing.RouteHealth{"route": {Open: true}}
		}, code: provider.CodeNoRoute, phase: provider.PhasePlan,
			details: map[string]string{"rejected_routes": "1", "route_1": "route", "reason_1": routing.RejectHealth}},
		{name: "model has no route", configure: func(_ *planningSource, _ *planningAdapter, request *llm.GenerateRequestV1) {
			request.SettingsPatch.Model.Set = preparationPointer("unknown")
		}, code: provider.CodeNoRoute, phase: provider.PhasePlan},
	} {
		t.Run(test.name, func(t *testing.T) {
			capabilities, source, adapter, request, _, _ := planningFixture()
			request.SettingsPatch.ServiceClass.Set = preparationPointer(llm.ServiceClassStandard)
			request.SettingsPatch.ServiceClassFallbacks.Set = preparationPointer([]llm.ServiceClass{})
			test.configure(source, adapter, &request)
			planning, err := capabilities.NewProviderPlanning(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = planning.Generate(context.Background(), prepared)
			details := rejectedSelection(t, err, test.code, test.phase)
			if len(details) != len(test.details) {
				t.Fatalf("details = %v, want %v", details, test.details)
			}
			for key, want := range test.details {
				if details[key] != want {
					t.Fatalf("details = %v, want %v", details, test.details)
				}
			}
		})
	}
}

// TestProviderPlanningBoundsRejectionDetails keeps a wide catalog from growing
// the error, and puts the rejections that explain the code first.
func TestProviderPlanningBoundsRejectionDetails(t *testing.T) {
	rejections := []planningRejection{{RouteID: "tenant", Reason: routing.RejectTenant}}
	for index := 0; index < 2*maxPlanningRejectionDetails; index++ {
		rejections = append(rejections, planningRejection{RouteID: "route", Reason: rejectCompile, Feature: "image"})
	}
	details := rejectedSelection(t, selectionError(context.Background(), rejections, false, provider.PhaseCompile), provider.CodeUnsupportedCapability, provider.PhaseCompile)
	if len(details) != 1+2*maxPlanningRejectionDetails || details["rejected_routes"] != "9" || details["reason_1"] != rejectCompile+":image" {
		t.Fatalf("details = %v", details)
	}
}
