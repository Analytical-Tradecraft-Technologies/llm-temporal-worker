package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestPollSleepMapsOnlyContextStops(t *testing.T) {
	sleepReturning := func(err error) func(context.Context, time.Duration) error {
		return func(context.Context, time.Duration) error { return err }
	}
	for _, test := range []struct {
		name  string
		sleep error
		code  provider.Code
	}{
		{name: "canceled", sleep: context.Canceled, code: provider.CodeCanceled},
		{name: "deadline", sleep: context.DeadlineExceeded, code: provider.CodeDeadlineExceeded},
	} {
		err := pollSleep(context.Background(), sleepReturning(test.sleep), time.Second)
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != test.code || !errors.Is(mapped.Cause, test.sleep) {
			t.Fatalf("%s: pollSleep = %v", test.name, err)
		}
	}
	other := errors.New("sleep failed")
	if err := pollSleep(context.Background(), sleepReturning(other), time.Second); err != other {
		t.Fatalf("other sleep error = %v", err)
	}
	if err := pollSleep(context.Background(), sleepReturning(nil), time.Second); err != nil {
		t.Fatalf("completed sleep = %v", err)
	}
}

func TestHeartbeatFailedAndWithOperationID(t *testing.T) {
	cause := errors.New("beat")
	failure := withOperationID(heartbeatFailed(provider.DispatchAccepted, cause), "op-1")
	if failure.Code != provider.CodeStateUnavailable || failure.Phase != provider.PhaseFinalize || failure.Dispatch != provider.DispatchAccepted ||
		failure.Retry != provider.RetrySameOperation || failure.SafeMessage != "progress heartbeat failed" || failure.Cause != cause || failure.OperationID != "op-1" {
		t.Fatalf("failure = %+v", failure)
	}
}
