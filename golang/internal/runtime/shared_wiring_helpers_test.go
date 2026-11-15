package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestQueryFilterAs(t *testing.T) {
	request := control.QueryRequest{Filter: control.CreditStatusQuery{Page: control.QueryPage{Size: 4}}}
	query, err := queryFilterAs[control.CreditStatusQuery](request, "credit status")
	if err != nil || query.Page.Size != 4 {
		t.Fatalf("queryFilterAs = %#v, %v", query, err)
	}
	if _, err := queryFilterAs[control.BudgetStatusQuery](request, "budget status"); err == nil || err.Error() != "budget status filter has unexpected type" {
		t.Fatalf("mismatched filter error = %v", err)
	}
}

func TestClaimedPosition(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if horizon, after := claimedPosition(nil, now); !horizon.Equal(now) || after != "" {
		t.Fatalf("first page = %v, %q", horizon, after)
	}
	claims := &control.BoundCursorClaims{Horizon: now.Add(-time.Hour), Position: "route-b"}
	if horizon, after := claimedPosition(claims, now); !horizon.Equal(claims.Horizon) || after != "route-b" {
		t.Fatalf("continuation = %v, %q", horizon, after)
	}
}

func TestProviderEgressConnectError(t *testing.T) {
	live := context.Background()
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	caller, cancelCaller := context.WithCancel(context.Background())
	cancelCaller()

	testassert.ErrorIs(t, providerEgressConnectError(live, live, &providerEgressCallState{callerContext: caller}, "connection_failed"), context.Canceled, "caller cancellation")
	if err := providerEgressConnectError(live, expired, nil, "connection_failed"); err == nil || err.Error() != preDispatchProviderFailure("connection_timeout").Error() {
		t.Fatalf("connect timeout error = %v", err)
	}
	if err := providerEgressConnectError(live, live, nil, ""); err != nil {
		t.Fatalf("continuable attempt error = %v, want nil", err)
	}
	if err := providerEgressConnectError(live, live, nil, "dns_resolution_failed"); err == nil || err.Error() != preDispatchProviderFailure("dns_resolution_failed").Error() {
		t.Fatalf("reason error = %v", err)
	}
}

func TestMarshalHandoff(t *testing.T) {
	input, data, err := marshalHandoff(map[string]int{"a": 1}, []string{"b"})
	if err != nil || string(input) != `{"a":1}` || string(data) != `["b"]` {
		t.Fatalf("marshalHandoff = %s, %s, %v", input, data, err)
	}
	for _, tc := range []struct {
		name              string
		request, response any
		dispatch          provider.DispatchCertainty
	}{
		{name: "request", request: func() {}, response: "ok", dispatch: provider.DispatchNotDispatched},
		{name: "response", request: "ok", response: func() {}, dispatch: provider.DispatchAccepted},
	} {
		_, _, err := marshalHandoff(tc.request, tc.response)
		testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeInvalidArgument, Dispatch: tc.dispatch}, fmt.Sprintf("%s encoding error", tc.name))
	}
}

func TestSnapshotV1RuntimeCapabilitiesWrapsSentinel(t *testing.T) {
	sentinel := errors.New("fixture composition")
	if _, err := snapshotV1RuntimeCapabilities(nil, nil, nil, sentinel); !errors.Is(err, sentinel) || err.Error() != "fixture composition: context is nil" {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := snapshotV1RuntimeCapabilities(context.Background(), nil, nil, sentinel); !errors.Is(err, sentinel) || err.Error() != "fixture composition: configuration snapshot is nil" {
		t.Fatalf("nil snapshot error = %v", err)
	}
}
