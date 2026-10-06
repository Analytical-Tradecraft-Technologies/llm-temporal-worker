package provider

import (
	"errors"
	"testing"
)

func TestCallRecoveredReportsAPanicInsteadOfUnwinding(t *testing.T) {
	result, panicked, err := CallRecovered(func() (*int, error) { panic("slice bounds out of range") })
	if result != nil || err != nil || panicked != "slice bounds out of range" {
		t.Fatalf("CallRecovered() = %v, %v, %v; want the recovered panic", result, panicked, err)
	}
	value := 7
	result, panicked, err = CallRecovered(func() (*int, error) { return &value, errors.New("plain") })
	if result != &value || panicked != nil || err == nil {
		t.Fatalf("CallRecovered() without a panic = %v, %v, %v", result, panicked, err)
	}
}

func TestSDKPanicErrorClassifiesByWhetherAResponseArrived(t *testing.T) {
	received := SDKPanicError("op-1", "boom", true)
	if received.Code != CodeProviderInvalidResponse || received.Phase != PhaseLift || received.Dispatch != DispatchAccepted || received.Retry != RetryNever || received.OperationID != "op-1" {
		t.Fatalf("panic after the response = %#v, want accepted invalid response", received)
	}
	unsent := SDKPanicError("op-2", "boom", false)
	if unsent.Dispatch != DispatchAmbiguous || unsent.Retry != RetryNever || unsent.OperationID != "op-2" {
		t.Fatalf("panic before the response = %#v, want ambiguous and never retried", unsent)
	}
}
