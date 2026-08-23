package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestProviderEgressPolicyRejectsBlockedAddresses(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"ipv4 loopback":        "127.0.0.1",
		"ipv4 private":         "10.0.0.1",
		"ipv4 link local":      "169.254.169.254",
		"ipv4 multicast":       "224.0.0.1",
		"ipv4 unspecified":     "0.0.0.0",
		"ipv4 carrier grade":   "100.64.0.1",
		"azure metadata":       "168.63.129.16",
		"ipv6 loopback":        "::1",
		"ipv6 private":         "fd00:ec2::254",
		"ipv6 link local":      "fe80::1",
		"ipv6 multicast":       "ff02::1",
		"ipv6 unspecified":     "::",
		"ipv4 mapped loopback": "::ffff:127.0.0.1",
	}
	for name, address := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			policy, err := newProviderEgressPolicy(providerEgressEndpoint(), &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(address)}}}, nil, time.Second)
			if err != nil {
				t.Fatalf("newProviderEgressPolicy() error = %v", err)
			}
			_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
			if !errors.Is(err, ErrProviderEgressDenied) {
				t.Fatalf("DialContext() error = %v, want ErrProviderEgressDenied", err)
			}
			if strings.Contains(err.Error(), address) {
				t.Fatalf("DialContext() leaked blocked address: %q", err)
			}
		})
	}
}

func TestBlockedProviderAddressRejectsIPv6SiteLocalRange(t *testing.T) {
	cases := map[string]struct {
		address string
		blocked bool
	}{
		"first site local": {address: "fec0::1", blocked: true},
		"last site local":  {address: "feff:ffff::1", blocked: true},
		"public IPv6":      {address: "2001:4860:4860::8888", blocked: false},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			if got := blockedProviderAddress(netip.MustParseAddr(test.address)); got != test.blocked {
				t.Fatalf("blockedProviderAddress(%s) = %t, want %t", test.address, got, test.blocked)
			}
		})
	}
}

func TestProviderEgressPolicyDialsResolvedPublicAddressAndRechecksConnection(t *testing.T) {
	t.Parallel()

	const publicAddress = "8.8.8.8"
	var dialed string
	policy, err := newProviderEgressPolicy(providerEgressEndpoint(), &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(publicAddress)}}}, func(_ context.Context, _ string, address string) (net.Conn, error) {
		dialed = address
		return newEgressTestConn(net.ParseIP(publicAddress)), nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := policy.DialContext(context.Background(), "tcp", "provider.example:443")
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	firstConnection := connection
	t.Cleanup(func() { _ = firstConnection.Close() })
	if got, want := dialed, "8.8.8.8:443"; got != want {
		t.Fatalf("dial target = %q, want resolved address %q", got, want)
	}

	policy, err = newProviderEgressPolicy(providerEgressEndpoint(), &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(publicAddress)}}}, func(context.Context, string, string) (net.Conn, error) {
		return newEgressTestConn(net.ParseIP("169.254.169.254")), nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	connection, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	if connection != nil {
		_ = connection.Close()
		t.Fatal("DialContext() returned a connection after its remote address changed")
	}
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("DialContext() error = %v, want ErrProviderEgressDenied", err)
	}
	if strings.Contains(err.Error(), "169.254.169.254") {
		t.Fatalf("DialContext() leaked rechecked address: %q", err)
	}
}

func TestProviderEgressPolicyUsesOnlyConfiguredBasePort(t *testing.T) {
	t.Parallel()

	endpoint := providerEgressEndpoint()
	endpoint.BaseURL = "https://provider.example:8443/v1"
	policy, err := newProviderEgressPolicy(endpoint, &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}}, func(_ context.Context, _ string, address string) (net.Conn, error) {
		if got, want := address, "8.8.8.8:8443"; got != want {
			t.Fatalf("dial target = %q, want %q", got, want)
		}
		return newEgressTestConn(net.ParseIP("8.8.8.8")), nil
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("DialContext() unconfigured port error = %v, want ErrProviderEgressDenied", err)
	}
	connection, err := policy.DialContext(context.Background(), "tcp", "provider.example:8443")
	if err != nil {
		t.Fatalf("DialContext() configured port error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
}

func TestProviderEgressPolicyBoundsDNSResolution(t *testing.T) {
	t.Parallel()

	resolver := &blockingEgressResolver{started: make(chan struct{})}
	policy, err := newProviderEgressPolicy(providerEgressEndpoint(), resolver, nil, 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	if !errors.Is(err, provider.ErrProviderPreDispatch) {
		t.Fatalf("DialContext() error = %v, want ErrProviderPreDispatch", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("DNS resolution exceeded the bounded connect timeout: %s", elapsed)
	}
	select {
	case <-resolver.started:
	default:
		t.Fatal("resolver was not called")
	}
}

func TestProviderEgressTransportRecordsCallerDeadlineBeforeDispatch(t *testing.T) {
	resolver := &blockingEgressResolver{started: make(chan struct{})}
	client, err := newProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint(), resolver, nil, config.DefaultProviderResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	deadlineContext, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	ctx, outcome := provider.WithEgressOutcome(deadlineContext)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", strings.NewReader("prompt-secret"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Transport.RoundTrip(request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context.DeadlineExceeded", err)
	}
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("recorded policy denial = %v, want nil for caller deadline", denial)
	}
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	if mapped == nil || mapped.Code != provider.CodeDeadlineExceeded || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("classified caller deadline = %#v, want non-retryable not-dispatched deadline", mapped)
	}
	select {
	case <-resolver.started:
	default:
		t.Fatal("resolver was not called")
	}
}

func TestProviderEgressRoundTripperClassifiesCallerCancellationBeforeGotConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, outcome := provider.WithEgressOutcome(ctx)
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{"provider.example": {"443": {}}}},
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			cancel()
			return nil, request.Context().Err()
		}),
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context.Canceled", err)
	}
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	if mapped == nil || mapped.Code != provider.CodeCanceled || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("classified caller cancellation = %#v, want non-retryable not-dispatched cancellation", mapped)
	}
}

func TestProviderEgressRoundTripperClassifiesClientDeadlineBeforeGotConnAsAvailability(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{"provider.example": {"443": {}}}},
		next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}),
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want context.DeadlineExceeded", err)
	}
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	if mapped == nil || mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("classified client deadline = %#v, want retryable not-dispatched availability", mapped)
	}
}

func TestProviderEgressRoundTripperClassifiesTLSFailureBeforeGotConnAsAvailability(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{"provider.example": {"443": {}}}},
		next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("tls handshake failed")
		}),
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, provider.ErrProviderPreDispatch) {
		t.Fatalf("RoundTrip() error = %v, want ErrProviderPreDispatch", err)
	}
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	if mapped == nil || mapped.Code != provider.CodeProviderUnavailable || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("classified TLS failure = %#v, want retryable not-dispatched availability", mapped)
	}
}

func TestProviderEgressRoundTripperDoesNotLetLatePreflightPolicyOverwriteCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, outcome := provider.WithEgressOutcome(ctx)
	var finish func(error)
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{"provider.example": {"443": {}}}},
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			state := providerEgressCallStateFromContext(request.Context())
			if state == nil {
				t.Fatal("RoundTrip request is missing egress call state")
			}
			finish = state.beginPreflight()
			cancel()
			return nil, request.Context().Err()
		}),
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context.Canceled", err)
	}
	if finish == nil {
		t.Fatal("RoundTrip did not start a preflight")
	}
	finish(deniedProviderEgress("unsafe_address"))
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	if mapped == nil || mapped.Code != provider.CodeCanceled || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("late preflight overwrote caller cancellation: %#v", mapped)
	}
	if errors.Is(mapped, provider.ErrProviderEgressDenied) {
		t.Fatalf("late preflight policy leaked into caller cancellation: %v", mapped)
	}
}

func TestProviderEgressRoundTripperDoesNotClassifyCanceledRequestAfterConnectionAcquired(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			state := providerEgressCallStateFromContext(request.Context())
			if state == nil {
				t.Fatal("RoundTrip request is missing egress call state")
			}
			finish := state.beginPreflight()
			defer func() { finish(request.Context().Err()) }()
			trace := httptrace.ContextClientTrace(request.Context())
			if trace == nil || trace.GotConn == nil {
				t.Fatal("RoundTrip request is missing GotConn trace")
			}
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			cancel()
			return nil, request.Context().Err()
		}),
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context.Canceled", err)
	}
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("recorded denial = %v, want nil after a connection was acquired", denial)
	}
}

func TestProviderEgressRoundTripperRecordsCompletedPreflightDenialAfterCancellation(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			state := providerEgressCallStateFromContext(request.Context())
			if state == nil {
				t.Fatal("RoundTrip request is missing egress call state")
			}
			finish := state.beginPreflight()
			cancel()
			finish(deniedProviderEgress("unsafe_address"))
			return nil, request.Context().Err()
		}),
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context.Canceled", err)
	}
	if !errors.Is(outcome.Denial(), provider.ErrProviderEgressDenied) {
		t.Fatalf("recorded denial = %v, want ErrProviderEgressDenied", outcome.Denial())
	}
}

func TestProviderEgressRoundTripperDoesNotReturnEgressDenialAfterConnectionAcquired(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			state := providerEgressCallStateFromContext(request.Context())
			if state == nil {
				t.Fatal("RoundTrip request is missing egress call state")
			}
			finish := state.beginPreflight()
			trace := httptrace.ContextClientTrace(request.Context())
			if trace == nil || trace.GotConn == nil {
				t.Fatal("RoundTrip request is missing GotConn trace")
			}
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			denial := deniedProviderEgress("connection_failed")
			finish(denial)
			return nil, denial
		}),
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = guard.RoundTrip(request)
	if errors.Is(err, provider.ErrProviderEgressDenied) {
		t.Fatalf("RoundTrip() error = %v, want an ambiguous error without ErrProviderEgressDenied", err)
	}
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("recorded denial = %v, want nil after a connection was acquired", denial)
	}
}

func TestProviderEgressRoundTripperRejectsDeclaredOversizedResponseAndClosesBody(t *testing.T) {
	t.Parallel()

	body := &trackingResponseBody{Reader: strings.NewReader("oversized-provider-payload")}
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: 5,
				Body:          body,
			}, nil
		}),
		maxResponseBytes: 4,
	}
	request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := guard.RoundTrip(request)
	if response != nil {
		t.Fatalf("RoundTrip() response = %#v, want nil", response)
	}
	if !errors.Is(err, provider.ErrProviderResponseTooLarge) {
		t.Fatalf("RoundTrip() error = %v, want ErrProviderResponseTooLarge", err)
	}
	if got, want := err.Error(), "provider response exceeded configured byte limit"; got != want {
		t.Fatalf("RoundTrip() error = %q, want %q", got, want)
	}
	if !body.closed {
		t.Fatal("oversized response body was not closed")
	}
}

func TestProviderEgressRoundTripperBoundsUnknownLengthResponseBody(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusOK, http.StatusTooManyRequests} {
		status := status
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			upstream := &trackingResponseBody{Reader: strings.NewReader("12345")}
			guard := &providerEgressRoundTripper{
				policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
					"provider.example": {"443": {}},
				}},
				next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: status, ContentLength: -1, Body: upstream}, nil
				}),
				maxResponseBytes: 4,
			}
			request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}

			response, err := guard.RoundTrip(request)
			if err != nil {
				t.Fatalf("RoundTrip() error = %v", err)
			}
			body, readErr := io.ReadAll(response.Body)
			if got, want := string(body), "1234"; got != want {
				t.Fatalf("bounded body = %q, want %q", got, want)
			}
			if !errors.Is(readErr, provider.ErrProviderResponseTooLarge) {
				t.Fatalf("body read error = %v, want ErrProviderResponseTooLarge", readErr)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatalf("close bounded body: %v", err)
			}
			if !upstream.closed {
				t.Fatal("bounded body Close did not close upstream body")
			}
		})
	}
}

func TestProviderEgressRoundTripperBoundsBodyWhenDeclaredLengthIsTooSmall(t *testing.T) {
	t.Parallel()

	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: 1,
				Body:          io.NopCloser(strings.NewReader("12345")),
			}, nil
		}),
		maxResponseBytes: 4,
	}
	request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := guard.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	body, readErr := io.ReadAll(response.Body)
	if got, want := string(body), "1234"; got != want {
		t.Fatalf("bounded body = %q, want %q", got, want)
	}
	if !errors.Is(readErr, provider.ErrProviderResponseTooLarge) {
		t.Fatalf("body read error = %v, want ErrProviderResponseTooLarge", readErr)
	}
}

func TestProviderEgressRoundTripperPreservesBoundedStreamingResponse(t *testing.T) {
	t.Parallel()

	payload := "data: ok\n\n"
	upstream := &trackingResponseBody{Reader: &fragmentedReader{data: []byte(payload), maximum: 2}}
	guard := &providerEgressRoundTripper{
		policy: &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{
			"provider.example": {"443": {}},
		}},
		next: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: -1,
				Header:        http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:          upstream,
			}, nil
		}),
		maxResponseBytes: int64(len(payload)),
	}
	request, err := http.NewRequest(http.MethodPost, "https://provider.example/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := guard.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("read bounded streaming body: %v", readErr)
	}
	if got := string(body); got != payload {
		t.Fatalf("streaming body = %q, want %q", got, payload)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close bounded streaming body: %v", err)
	}
	if !upstream.closed {
		t.Fatal("bounded streaming Close did not close upstream body")
	}
}

func TestProviderEgressTransportDoesNotPoisonCanceledRequestAfterIdleConnectionWins(t *testing.T) {
	firstPartial := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondObserved := make(chan struct{})
	releaseSecond := make(chan struct{})
	var firstOnce, secondOnce, releaseFirstOnce, releaseSecondOnce sync.Once

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{
		Listener:    listener,
		EnableHTTP2: false,
		Config: &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.ProtoMajor != 1 {
				t.Errorf("request protocol = HTTP/%d, want HTTP/1", request.ProtoMajor)
			}
			switch request.URL.Path {
			case "/first":
				response.Header().Set("Content-Length", "2")
				_, _ = io.WriteString(response, "a")
				if flusher, ok := response.(http.Flusher); ok {
					flusher.Flush()
				}
				firstOnce.Do(func() { close(firstPartial) })
				<-releaseFirst
				_, _ = io.WriteString(response, "b")
			case "/second":
				secondOnce.Do(func() { close(secondObserved) })
				<-releaseSecond
				response.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(response, request)
			}
		})},
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	t.Cleanup(func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) })
	t.Cleanup(func() { releaseSecondOnce.Do(func() { close(releaseSecond) }) })

	base := server.Client()
	baseTransport, ok := base.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("TLS mock transport = %T, want *http.Transport", base.Transport)
	}
	trustedTransport := baseTransport.Clone()
	trustedTransport.ForceAttemptHTTP2 = false
	trustedTransport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	base = &http.Client{Transport: trustedTransport}

	endpoint := providerEgressEndpoint()
	endpoint.BaseURL = "https://example.com/v1"
	endpoint.OutboundHosts = []string{"example.com"}
	const publicAddress = "8.8.8.8"
	secondDialStarted := make(chan struct{})
	secondDialDone := make(chan struct{})
	var dialCount atomic.Int32
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		switch dialCount.Add(1) {
		case 1:
			connection, dialErr := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			if dialErr != nil {
				return nil, dialErr
			}
			return egressTestConn{Conn: connection, remote: &net.TCPAddr{IP: net.ParseIP(publicAddress), Port: 443}}, nil
		case 2:
			close(secondDialStarted)
			defer close(secondDialDone)
			<-ctx.Done()
			return nil, ctx.Err()
		default:
			return nil, errors.New("unexpected extra egress dial")
		}
	}
	client, err := newProviderEgressHTTPClient(base, endpoint, &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(publicAddress)}}}, dial, config.DefaultProviderResponseBytes)
	if err != nil {
		t.Fatal(err)
	}

	firstRequest, err := http.NewRequest(http.MethodGet, "https://example.com/first", nil)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse, err := client.Transport.RoundTrip(firstRequest)
	if err != nil {
		t.Fatalf("first RoundTrip() error = %v", err)
	}
	t.Cleanup(func() { _ = firstResponse.Body.Close() })
	select {
	case <-firstPartial:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start the first response")
	}
	firstRead := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(firstResponse.Body)
		firstRead <- readErr
	}()

	secondContext, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondContext, outcome := provider.WithEgressOutcome(secondContext)
	secondRequest, err := http.NewRequestWithContext(secondContext, http.MethodPost, "https://example.com/second", nil)
	if err != nil {
		t.Fatal(err)
	}
	secondResult := make(chan error, 1)
	go func() {
		response, roundTripErr := client.Transport.RoundTrip(secondRequest)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		secondResult <- roundTripErr
	}()
	select {
	case <-secondDialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second request did not begin its detached dial")
	}

	releaseFirstOnce.Do(func() { close(releaseFirst) })
	select {
	case readErr := <-firstRead:
		if readErr != nil {
			t.Fatalf("read first response body: %v", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first response did not complete")
	}
	if err := firstResponse.Body.Close(); err != nil {
		t.Fatalf("close first response body: %v", err)
	}
	select {
	case <-secondObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("idle connection did not satisfy the second request")
	}

	cancelSecond()
	select {
	case roundTripErr := <-secondResult:
		if !errors.Is(roundTripErr, context.Canceled) {
			t.Fatalf("second RoundTrip() error = %v, want context.Canceled", roundTripErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled second request did not return")
	}
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("outcome before detached dial exits = %v, want nil", denial)
	}

	select {
	case <-secondDialDone:
	case <-time.After(2 * time.Second):
		t.Fatal("detached dial did not finish after request cancellation")
	}
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("outcome after detached dial exits = %v, want nil", denial)
	}
	releaseSecondOnce.Do(func() { close(releaseSecond) })
}

func TestLocalProviderMockPolicyRemainsFailClosed(t *testing.T) {
	t.Parallel()

	endpoint := localProviderMockEndpoint(t)
	policy, err := newProviderEgressPolicy(endpoint, &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("172.20.0.10")}}}, nil, time.Second)
	if err != nil {
		t.Fatalf("newProviderEgressPolicy() error = %v", err)
	}
	_, err = policy.DialContext(context.Background(), "tcp", "provider-mock:8081")
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("local provider mock DialContext() error = %v, want ErrProviderEgressDenied", err)
	}
	if strings.Contains(err.Error(), "172.20.0.10") {
		t.Fatalf("local provider mock error leaked Docker address: %q", err)
	}
}

func TestProviderEgressTransportRejectsArbitraryRequestURL(t *testing.T) {
	t.Parallel()

	resolver := &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}}
	client, err := newProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint(), resolver, nil, config.DefaultProviderResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://attacker.example/continuation-secret", strings.NewReader("prompt-secret"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer api-key-secret")
	_, err = client.Transport.RoundTrip(request)
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("RoundTrip() error = %v, want ErrProviderEgressDenied", err)
	}
	for _, secret := range []string{"attacker.example", "continuation-secret", "prompt-secret", "api-key-secret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("RoundTrip() leaked %q in %q", secret, err)
		}
	}
	if resolver.lookups != 0 {
		t.Fatalf("resolver calls = %d, want no lookup for an unconfigured host", resolver.lookups)
	}

	hostOverride, err := http.NewRequest(http.MethodGet, "https://provider.example/v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	hostOverride.Host = "attacker.example"
	_, err = client.Transport.RoundTrip(hostOverride)
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("RoundTrip() host override error = %v, want ErrProviderEgressDenied", err)
	}
	if strings.Contains(err.Error(), "attacker.example") {
		t.Fatalf("RoundTrip() leaked host override in %q", err)
	}
	if resolver.lookups != 0 {
		t.Fatalf("resolver calls = %d, want no lookup for a host override", resolver.lookups)
	}
}

func TestProviderEgressTransportDisablesRedirectsAndKeepsTLS(t *testing.T) {
	t.Parallel()

	redirects := 0
	server := newLoopbackTLSServer(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		redirects++
		if request.URL.Path == "/redirect" {
			http.Redirect(response, request, "https://provider.example/final", http.StatusFound)
			return
		}
		_, _ = io.WriteString(response, "ok")
	}))

	base := server.Client()
	transport, ok := base.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("TLS mock transport = %T, want *http.Transport", base.Transport)
	}
	trustedTransport := transport.Clone()
	if trustedTransport.TLSClientConfig == nil {
		t.Fatal("TLS mock transport is missing trusted test roots")
	}
	base = &http.Client{Transport: trustedTransport}
	endpoint := providerEgressEndpoint()
	endpoint.BaseURL = "https://example.com/v1"
	endpoint.OutboundHosts = []string{"example.com"}

	client, err := newProviderEgressHTTPClient(base, endpoint, &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}}, func(ctx context.Context, network, address string) (net.Conn, error) {
		connection, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		if err != nil {
			return nil, err
		}
		return egressTestConn{Conn: connection, remote: &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 443}}, nil
	}, config.DefaultProviderResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Get("https://example.com/redirect")
	if err != nil {
		t.Fatalf("GET through TLS mock: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	if got, want := response.StatusCode, http.StatusFound; got != want {
		t.Fatalf("response status = %d, want redirect response %d", got, want)
	}
	if redirects != 1 {
		t.Fatalf("TLS mock requests = %d, want 1 without redirect follow", redirects)
	}
}

func TestProviderEgressTransportRejectsInsecureTLSVerification(t *testing.T) {
	t.Parallel()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // regression test for rejecting unsafe caller transport.
	_, err := newProviderEgressHTTPClient(&http.Client{Transport: transport}, providerEgressEndpoint(), nil, nil, config.DefaultProviderResponseBytes)
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("newProviderEgressHTTPClient() error = %v, want ErrProviderEgressDenied", err)
	}
	if got, want := err.Error(), "provider egress blocked: invalid_transport"; got != want {
		t.Fatalf("newProviderEgressHTTPClient() error = %q, want %q", got, want)
	}
}

func TestCloneProviderTransportClearsCallerAlternateProtocol(t *testing.T) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{
		"caller-protocol": func(string, *tls.Conn) http.RoundTripper {
			return roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("caller alternate protocol should not run")
			})
		},
	}
	cloned, err := cloneProviderTransport(&http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("cloneProviderTransport() error = %v", err)
	}
	if cloned.TLSNextProto != nil {
		t.Fatalf("cloneProviderTransport() retained caller alternate protocols: %#v", cloned.TLSNextProto)
	}
}

func TestProviderEgressTransportRejectsTLSHostnameOverride(t *testing.T) {
	t.Parallel()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{ServerName: "cohosted.example"}
	_, err := newProviderEgressHTTPClient(&http.Client{Transport: transport}, providerEgressEndpoint(), nil, nil, config.DefaultProviderResponseBytes)
	if !errors.Is(err, ErrProviderEgressDenied) {
		t.Fatalf("newProviderEgressHTTPClient() error = %v, want ErrProviderEgressDenied", err)
	}
	if got, want := err.Error(), "provider egress blocked: invalid_transport"; got != want {
		t.Fatalf("newProviderEgressHTTPClient() error = %q, want %q", got, want)
	}
}

func TestProviderEgressTransportBoundsConnectAndReadTimeouts(t *testing.T) {
	t.Parallel()

	endpoint := providerEgressEndpoint()
	endpoint.Timeout = config.Duration(20 * time.Second)
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	baseTransport.Proxy = http.ProxyFromEnvironment
	baseTransport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("must not bypass egress dial policy")
	}
	baseTransport.DialTLS = func(string, string) (net.Conn, error) { return nil, errors.New("must not bypass egress dial policy") }
	client, err := newProviderEgressHTTPClient(&http.Client{Transport: baseTransport}, endpoint, &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}}, nil, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := client.Timeout, 20*time.Second; got != want {
		t.Fatalf("client timeout = %s, want %s", got, want)
	}
	guard, ok := client.Transport.(*providerEgressRoundTripper)
	if !ok {
		t.Fatalf("client transport = %T, want provider egress guard", client.Transport)
	}
	if got, want := guard.responseByteLimit(), int64(8<<20); got != want {
		t.Fatalf("response byte limit = %d, want %d", got, want)
	}
	transport, ok := guard.next.(*http.Transport)
	if !ok {
		t.Fatalf("guard next transport = %T, want *http.Transport", guard.next)
	}
	if got, want := transport.TLSHandshakeTimeout, 10*time.Second; got != want {
		t.Fatalf("TLS handshake timeout = %s, want %s", got, want)
	}
	if got, want := transport.ResponseHeaderTimeout, 20*time.Second; got != want {
		t.Fatalf("response header timeout = %s, want %s", got, want)
	}
	if transport.Proxy != nil || transport.DialTLSContext != nil || transport.DialTLS != nil {
		t.Fatal("egress transport retained a proxy or TLS dial bypass")
	}
}

func TestProviderEgressHTTPClientRejectsUnsafeResponseLimit(t *testing.T) {
	t.Parallel()

	for name, limit := range map[string]int64{
		"negative":         -1,
		"zero":             0,
		"above safety cap": config.MaxProviderResponseBytes + 1,
	} {
		name, limit := name, limit
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := newProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint(), nil, nil, limit)
			if !errors.Is(err, ErrProviderEgressDenied) {
				t.Fatalf("newProviderEgressHTTPClient(%d) error = %v, want ErrProviderEgressDenied", limit, err)
			}
			if got, want := err.Error(), "provider egress blocked: invalid_policy"; got != want {
				t.Fatalf("newProviderEgressHTTPClient(%d) error = %q, want %q", limit, got, want)
			}
		})
	}
}

func TestNewProviderEgressHTTPClientUsesProductionPolicy(t *testing.T) {
	client, err := NewProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint())
	if err != nil {
		t.Fatalf("NewProviderEgressHTTPClient() error = %v", err)
	}
	if _, ok := client.Transport.(*providerEgressRoundTripper); !ok {
		t.Fatalf("client transport = %T, want production provider egress guard", client.Transport)
	}
	if client.CheckRedirect == nil {
		t.Fatal("client does not retain the production redirect guard")
	}
}

func providerEgressEndpoint() config.EndpointConfig {
	return config.EndpointConfig{
		BaseURL:       "https://provider.example/v1",
		OutboundHosts: []string{"PROVIDER.EXAMPLE."},
		Timeout:       config.Duration(30 * time.Second),
	}
}

type egressTestResolver struct {
	addresses []net.IPAddr
	lookups   int
}

func (resolver *egressTestResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	resolver.lookups++
	return append([]net.IPAddr(nil), resolver.addresses...), nil
}

type blockingEgressResolver struct {
	started chan struct{}
}

func (resolver *blockingEgressResolver) LookupIPAddr(ctx context.Context, _ string) ([]net.IPAddr, error) {
	close(resolver.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

type egressTestConn struct {
	net.Conn
	remote net.Addr
}

func (connection egressTestConn) RemoteAddr() net.Addr { return connection.remote }

type trackingResponseBody struct {
	io.Reader
	closed bool
}

type fragmentedReader struct {
	data    []byte
	maximum int
}

func (reader *fragmentedReader) Read(buffer []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	maximum := min(len(buffer), reader.maximum, len(reader.data))
	read := copy(buffer[:maximum], reader.data[:maximum])
	reader.data = reader.data[read:]
	return read, nil
}

func (body *trackingResponseBody) Close() error {
	body.closed = true
	return nil
}

func newEgressTestConn(remote net.IP) net.Conn {
	client, server := net.Pipe()
	go func() { _ = server.Close() }()
	return egressTestConn{Conn: client, remote: &net.TCPAddr{IP: remote, Port: 443}}
}

func newLoopbackTLSServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func localProviderMockEndpoint(t *testing.T) config.EndpointConfig {
	t.Helper()
	_, source, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(source), "..", "..", "deploy", "local", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(data)
	if err != nil {
		t.Fatalf("load local config: %v", err)
	}
	endpoint, ok := loaded.Endpoints["provider-mock"]
	if !ok {
		t.Fatal("local config is missing provider-mock endpoint")
	}
	return endpoint
}
