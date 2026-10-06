package provider

import (
	"errors"
	"net/http"
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

func TestDispatchProbeClassifiesAPanicByHowFarTheCallGot(t *testing.T) {
	respond := func(status int) func(*http.Request) (*http.Response, error) {
		return func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: status}, nil }
	}
	request, _ := http.NewRequest(http.MethodPost, "https://provider.example/v1", nil)
	for _, test := range []struct {
		name     string
		send     func(*DispatchProbe)
		code     Code
		dispatch DispatchCertainty
		retry    RetryDisposition
	}{
		{name: "before the transport", send: func(*DispatchProbe) {}, code: CodeInternal, dispatch: DispatchNotDispatched, retry: RetryNextRoute},
		{name: "sent without a response", send: func(probe *DispatchProbe) {
			_, _ = probe.Middleware(request, func(*http.Request) (*http.Response, error) { return nil, errors.New("reset") })
		}, code: CodeInternal, dispatch: DispatchAmbiguous, retry: RetryNever},
		{name: "success response", send: func(probe *DispatchProbe) { _, _ = probe.Middleware(request, respond(200)) }, code: CodeProviderInvalidResponse, dispatch: DispatchAccepted, retry: RetryNever},
		{name: "redirect", send: func(probe *DispatchProbe) { _, _ = probe.Middleware(request, respond(307)) }, code: CodeProviderUnavailable, dispatch: DispatchAmbiguous, retry: RetryNever},
		{name: "rate limited", send: func(probe *DispatchProbe) { _, _ = probe.Middleware(request, respond(429)) }, code: CodeProviderRateLimited, dispatch: DispatchRejected, retry: RetryAfter},
		{name: "server error", send: func(probe *DispatchProbe) { _, _ = probe.Middleware(request, respond(503)) }, code: CodeProviderUnavailable, dispatch: DispatchRejected, retry: RetrySameOperation},
		{name: "bad request", send: func(probe *DispatchProbe) { _, _ = probe.Middleware(request, respond(400)) }, code: CodeInvalidArgument, dispatch: DispatchRejected, retry: RetryNever},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := &DispatchProbe{}
			test.send(probe)
			mapped := probe.PanicError("op-1", "boom")
			if mapped.Code != test.code || mapped.Dispatch != test.dispatch || mapped.Retry != test.retry || mapped.OperationID != "op-1" {
				t.Fatalf("PanicError() = %#v, want %s/%s/%s", mapped, test.code, test.dispatch, test.retry)
			}
		})
	}
}
