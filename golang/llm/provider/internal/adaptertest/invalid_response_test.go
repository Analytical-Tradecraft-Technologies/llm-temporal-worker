package adaptertest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type fatalRecorder struct {
	testing.TB
	failed bool
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatalf(string, ...any) { r.failed = true }

func TestInvalidResponseAssertionsCheckCodeDispatchAndPhase(t *testing.T) {
	invalid := func(phase provider.Phase, dispatch provider.DispatchCertainty) error {
		return fmt.Errorf("wrapped: %w", &provider.Error{Code: provider.CodeProviderInvalidResponse, Phase: phase, Dispatch: dispatch})
	}
	for _, test := range []struct {
		name               string
		err                error
		wantAcceptedFailed bool
		wantLiftFailed     bool
	}{
		{name: "accepted lift", err: invalid(provider.PhaseLift, provider.DispatchAccepted)},
		{name: "accepted dispatch", err: invalid(provider.PhaseDispatch, provider.DispatchAccepted), wantLiftFailed: true},
		{name: "rejected", err: invalid(provider.PhaseLift, provider.DispatchRejected), wantAcceptedFailed: true, wantLiftFailed: true},
		{name: "other code", err: &provider.Error{Code: provider.CodeInvalidArgument, Phase: provider.PhaseLift, Dispatch: provider.DispatchAccepted}, wantAcceptedFailed: true, wantLiftFailed: true},
		{name: "not a provider error", err: errors.New("plain"), wantAcceptedFailed: true, wantLiftFailed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			accepted, lift := &fatalRecorder{TB: t}, &fatalRecorder{TB: t}
			RequireAcceptedInvalidResponse(accepted, test.err, test.name)
			RequireLiftInvalidResponse(lift, test.err, test.name)
			if accepted.failed != test.wantAcceptedFailed || lift.failed != test.wantLiftFailed {
				t.Fatalf("accepted failed = %v, lift failed = %v; want %v, %v", accepted.failed, lift.failed, test.wantAcceptedFailed, test.wantLiftFailed)
			}
		})
	}
}
