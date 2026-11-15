package provider

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestHTTPResponseMetadataReadsFirstRequestIDHeader(t *testing.T) {
	if got := HTTPResponseMetadata("resp", "tier", nil, "x-request-id"); got != (ResponseMetadata{ResponseID: "resp", ProviderTier: "tier"}) {
		t.Fatalf("HTTPResponseMetadata(nil raw) = %+v", got)
	}
	raw := &http.Response{StatusCode: 200, Header: http.Header{}}
	raw.Header.Set("second", "req-2")
	raw.Header.Set("third", "req-3")
	got := HTTPResponseMetadata("resp", "tier", raw, "first", "second", "third")
	if got != (ResponseMetadata{ResponseID: "resp", ProviderTier: "tier", Status: 200, RequestID: "req-2"}) {
		t.Fatalf("HTTPResponseMetadata = %+v", got)
	}
}

func TestDispatchLiftRunsHooksThenLift(t *testing.T) {
	observer := &dispatchTestObserver{}
	dispatch, err := BeginDispatch(context.Background(), observer, DispatchTarget{EndpointID: "ep"})
	testassert.NoError(t, err)
	result, err := dispatch.Lift(ResponseMetadata{ResponseID: "resp"}, 2, func() (llm.Response, error) {
		return llm.Response{OperationKey: "op"}, nil
	})
	if err != nil || result.Response.OperationKey != "op" || len(observer.progress) != 1 || observer.progress[0].OutputItems != 2 {
		t.Fatalf("Lift = %+v, %v, progress %+v", result, err, observer.progress)
	}
	liftErr := errors.New("lift failed")
	testassert.ErrorIs(t, testassert.Err(dispatch.Lift(ResponseMetadata{}, 0, func() (llm.Response, error) { return llm.Response{}, liftErr })), liftErr, "Lift(failing lift)")
	observer.afterErr = errors.New("rejected")
	lifted := false
	_, err = dispatch.Lift(ResponseMetadata{ResponseID: "resp", RequestID: "req"}, 0, func() (llm.Response, error) { lifted = true; return llm.Response{}, nil })
	var mapped *Error
	if !errors.As(err, &mapped) || mapped.Dispatch != DispatchAccepted || mapped.Provider.RequestID != "req" || lifted {
		t.Fatalf("Lift(rejected headers) = %v, lifted %v", err, lifted)
	}
}

func TestSDKParamsAsAcceptsValueAndNonNilPointer(t *testing.T) {
	value := 7
	for _, test := range []struct {
		params any
		want   int
		ok     bool
	}{{7, 7, true}, {&value, 7, true}, {(*int)(nil), 0, false}, {"7", 0, false}, {nil, 0, false}} {
		if got, ok := SDKParamsAs[int](Call{SDKParams: test.params}); got != test.want || ok != test.ok {
			t.Errorf("SDKParamsAs(%#v) = %d, %v; want %d, %v", test.params, got, ok, test.want, test.ok)
		}
	}
}
