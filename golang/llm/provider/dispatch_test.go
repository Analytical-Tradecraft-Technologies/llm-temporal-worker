package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
)

type dispatchTestObserver struct {
	beforeErr, afterErr error
	progress            []Progress
}

func (observer *dispatchTestObserver) BeforePossibleWrite(context.Context) error {
	return observer.beforeErr
}

func (observer *dispatchTestObserver) AfterResponseHeaders(context.Context, ResponseMetadata) error {
	return observer.afterErr
}

func (observer *dispatchTestObserver) OnProgress(_ context.Context, progress Progress) {
	observer.progress = append(observer.progress, progress)
}

func dispatchTestTarget(credentialUnsent bool) DispatchTarget {
	return DispatchTarget{EndpointID: "ep-1", Provider: "test/profile", CredentialUnsent: credentialUnsent, MapError: func(err error, provider string) *Error {
		mapped := NewUnclassifiedDispatchError(err, provider)
		mapped.SafeMessage = "mapped"
		return mapped
	}}
}

func TestStartDispatchChecksTheContextBeforeTheParams(t *testing.T) {
	call := Call{Family: FamilyOpenAIChat, EndpointID: "ep-1", SDKParams: 7}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := StartDispatch[int](canceled, call, FamilyOpenAIChat, "ep-1"); err == nil || err.(*Error).Code != CodeCanceled {
		t.Fatalf("StartDispatch(canceled) error = %v, want a canceled dispatch", err)
	}
	if got, err := StartDispatch[int](context.Background(), call, FamilyOpenAIChat, "ep-1"); err != nil || got != 7 {
		t.Fatalf("StartDispatch() = %d, %v; want 7", got, err)
	}
	if _, err := StartDispatch[int](context.Background(), call, FamilyOpenAIChat, "ep-2"); err == nil {
		t.Fatal("StartDispatch() accepted another endpoint's call")
	}
}

func TestBeginDispatchRunsTheBeforePossibleWriteHook(t *testing.T) {
	dispatch, err := BeginDispatch(context.Background(), nil, dispatchTestTarget(false))
	if err != nil || dispatch.Observer == nil || dispatch.Context == nil {
		t.Fatalf("BeginDispatch(nil observer) = %#v, %v; want a NopObserver dispatch", dispatch, err)
	}
	_, err = BeginDispatch(context.Background(), &dispatchTestObserver{beforeErr: errors.New("stop")}, dispatchTestTarget(false))
	var mapped *Error
	if !errors.As(err, &mapped) || mapped.Dispatch != DispatchNotDispatched || mapped.Code != CodeInternal {
		t.Fatalf("BeginDispatch(rejecting observer) error = %#v, want a not-dispatched observer error", err)
	}
}

func TestDispatchSDKErrorClassifiesByEvidence(t *testing.T) {
	credentials := fmt.Errorf("sign: %w", ErrProviderCredentials)
	for _, test := range []struct {
		name             string
		err              error
		sent             bool
		credentialUnsent bool
		message          string
		dispatch         DispatchCertainty
		provider         string
	}{
		{name: "egress denial", err: fmt.Errorf("dial: %w", ErrProviderEgressDenied), sent: true, message: "provider egress policy denied request", dispatch: DispatchNotDispatched},
		{name: "unsent", err: errors.New("resolve credentials"), message: "", dispatch: DispatchNotDispatched, provider: "test/profile"},
		{name: "sent", err: errors.New("reset"), sent: true, message: "mapped", dispatch: DispatchAmbiguous, provider: "test/profile"},
		{name: "sent credential error", err: credentials, sent: true, message: "mapped", dispatch: DispatchAmbiguous, provider: "test/profile"},
		{name: "credential error inside the probe", err: credentials, sent: true, credentialUnsent: true, dispatch: DispatchNotDispatched, provider: "test/profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dispatch, err := BeginDispatch(context.Background(), nil, dispatchTestTarget(test.credentialUnsent))
			if err != nil {
				t.Fatal(err)
			}
			var mapped *Error
			if !errors.As(dispatch.SDKError(test.err, test.sent), &mapped) {
				t.Fatal("SDKError() did not return a provider error")
			}
			if mapped.Dispatch != test.dispatch || mapped.SafeDetails["endpoint"] != "ep-1" || mapped.SafeDetails["provider"] != test.provider {
				t.Fatalf("SDKError() = %#v, want %s with endpoint ep-1 and provider %q", mapped, test.dispatch, test.provider)
			}
			if test.message != "" && mapped.SafeMessage != test.message {
				t.Fatalf("SDKError() message = %q, want %q", mapped.SafeMessage, test.message)
			}
		})
	}
}

func TestSendHTTPClassifiesPanicsRedirectsAndErrors(t *testing.T) {
	dispatch, err := BeginDispatch(context.Background(), nil, dispatchTestTarget(false))
	if err != nil {
		t.Fatal(err)
	}
	respond := func(status int) func(*DispatchProbe, **http.Response) (string, error) {
		return func(_ *DispatchProbe, raw **http.Response) (string, error) {
			*raw = &http.Response{StatusCode: status}
			return "body", nil
		}
	}
	response, raw, err := SendHTTP(dispatch, "op-1", respond(http.StatusOK))
	if err != nil || response != "body" || raw == nil || raw.StatusCode != http.StatusOK {
		t.Fatalf("SendHTTP(200) = %q, %v, %v; want the response", response, raw, err)
	}
	for _, test := range []struct {
		name     string
		send     func(*DispatchProbe, **http.Response) (string, error)
		dispatch DispatchCertainty
	}{
		{name: "panic before the transport", send: func(*DispatchProbe, **http.Response) (string, error) { panic("boom") }, dispatch: DispatchNotDispatched},
		{name: "redirect", send: respond(http.StatusTemporaryRedirect), dispatch: DispatchAmbiguous},
		{name: "unsent error", send: func(*DispatchProbe, **http.Response) (string, error) { return "", errors.New("sign") }, dispatch: DispatchNotDispatched},
	} {
		t.Run(test.name, func(t *testing.T) {
			response, raw, err := SendHTTP(dispatch, "op-1", test.send)
			var mapped *Error
			if response != "" || raw != nil || !errors.As(err, &mapped) || mapped.Dispatch != test.dispatch || mapped.SafeDetails["endpoint"] != "ep-1" {
				t.Fatalf("SendHTTP() = %q, %v, %#v; want a %s error for ep-1", response, raw, err, test.dispatch)
			}
		})
	}
}

func TestDispatchAfterResponseHeadersAndLiftProgress(t *testing.T) {
	observer := &dispatchTestObserver{}
	dispatch, err := BeginDispatch(context.Background(), observer, dispatchTestTarget(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.AfterResponseHeaders(ResponseMetadata{ResponseID: "resp-1"}); err != nil {
		t.Fatalf("AfterResponseHeaders() = %v, want nil", err)
	}
	dispatch.LiftProgress(3)
	if len(observer.progress) != 1 || observer.progress[0] != (Progress{Phase: string(PhaseLift), OutputItems: 3}) {
		t.Fatalf("LiftProgress() reported %#v", observer.progress)
	}
	observer.afterErr = errors.New("stop")
	var mapped *Error
	if !errors.As(dispatch.AfterResponseHeaders(ResponseMetadata{ResponseID: "resp-1", RequestID: "req-1"}), &mapped) ||
		mapped.Dispatch != DispatchAccepted || mapped.Provider.ResponseID != "resp-1" || mapped.Provider.RequestID != "req-1" {
		t.Fatalf("AfterResponseHeaders(rejecting observer) = %#v, want an accepted error with the response IDs", mapped)
	}
}
