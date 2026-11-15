package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/modelsync"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

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
		"ietf protocol":        "192.0.0.8",
		"nat64 metadata":       "64:ff9b::a9fe:a9fe",
		"nat64 private":        "64:ff9b::a00:1",
		"nat64 local use":      "64:ff9b:1::a00:1",
		"6to4 private":         "2002:a00:1::1",
		"teredo":               "2001:0:4136:e378:8000:63bf:3fff:fdd2",
		"ipv4 compatible":      "::7f00:1",
	}
	for name, address := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			policy, err := newProviderEgressPolicy(providerEgressEndpoint(), &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(address)}}}, nil, time.Second)
			testassert.NoError(t, err, "newProviderEgressPolicy() error")
			_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
			testassert.ErrorIs(t, err, ErrProviderEgressDenied, "DialContext()")
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
	testassert.NoError(t, err)
	connection, err := policy.DialContext(context.Background(), "tcp", "provider.example:443")
	testassert.NoError(t, err, "DialContext() error")
	firstConnection := connection
	t.Cleanup(func() { _ = firstConnection.Close() })
	if got, want := dialed, "8.8.8.8:443"; got != want {
		t.Fatalf("dial target = %q, want resolved address %q", got, want)
	}

	policy, err = newProviderEgressPolicy(providerEgressEndpoint(), &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP(publicAddress)}}}, func(context.Context, string, string) (net.Conn, error) {
		return newEgressTestConn(net.ParseIP("169.254.169.254")), nil
	}, time.Second)
	testassert.NoError(t, err)
	connection, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	if connection != nil {
		_ = connection.Close()
		t.Fatal("DialContext() returned a connection after its remote address changed")
	}
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "DialContext()")
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
	testassert.NoError(t, err)
	_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "DialContext() unconfigured port")
	connection, err := policy.DialContext(context.Background(), "tcp", "provider.example:8443")
	testassert.NoError(t, err, "DialContext() configured port error")
	t.Cleanup(func() { _ = connection.Close() })
}

func TestProviderEgressPolicyBoundsDNSResolution(t *testing.T) {
	t.Parallel()

	resolver := &blockingEgressResolver{started: make(chan struct{})}
	policy, err := newProviderEgressPolicy(providerEgressEndpoint(), resolver, nil, 25*time.Millisecond)
	testassert.NoError(t, err)
	start := time.Now()
	_, err = policy.DialContext(context.Background(), "tcp", "provider.example:443")
	testassert.ErrorIs(t, err, provider.ErrProviderPreDispatch, "DialContext()")
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
	testassert.NoError(t, err)
	deadlineContext, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	ctx, outcome := provider.WithEgressOutcome(deadlineContext)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", strings.NewReader("prompt-secret"))
	testassert.NoError(t, err)
	_, err = client.Transport.RoundTrip(request)
	testassert.ErrorIs(t, err, context.DeadlineExceeded, "RoundTrip()")
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("recorded policy denial = %v, want nil for caller deadline", denial)
	}
	requireNotDispatched(t, provider.ClassifyEgressOutcome(outcome, err), provider.CodeDeadlineExceeded, provider.RetryNever, "caller deadline")
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
	_, err := roundTripGuard(t, ctx, 0, func(request *http.Request) (*http.Response, error) {
		cancel()
		return nil, request.Context().Err()
	})
	testassert.ErrorIs(t, err, context.Canceled, "RoundTrip()")
	requireNotDispatched(t, provider.ClassifyEgressOutcome(outcome, err), provider.CodeCanceled, provider.RetryNever, "caller cancellation")
}

func TestProviderEgressRoundTripperClassifiesClientDeadlineBeforeGotConnAsAvailability(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	_, err := roundTripGuard(t, ctx, 0, func(*http.Request) (*http.Response, error) {
		return nil, context.DeadlineExceeded
	})
	testassert.ErrorIs(t, err, context.DeadlineExceeded, "RoundTrip()")
	requireNotDispatched(t, provider.ClassifyEgressOutcome(outcome, err), provider.CodeProviderUnavailable, provider.RetryNextRoute, "client deadline")
}

func TestProviderEgressRoundTripperClassifiesTLSFailureBeforeGotConnAsAvailability(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	_, err := roundTripGuard(t, ctx, 0, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("tls handshake failed")
	})
	testassert.ErrorIs(t, err, provider.ErrProviderPreDispatch, "RoundTrip()")
	requireNotDispatched(t, provider.ClassifyEgressOutcome(outcome, err), provider.CodeProviderUnavailable, provider.RetryNextRoute, "TLS failure")
}

func TestProviderEgressRoundTripperDoesNotLetLatePreflightPolicyOverwriteCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, outcome := provider.WithEgressOutcome(ctx)
	var finish func(error)
	_, err := roundTripGuard(t, ctx, 0, func(request *http.Request) (*http.Response, error) {
		state := requireEgressCallState(t, request)
		finish = state.beginPreflight()
		cancel()
		return nil, request.Context().Err()
	})
	testassert.ErrorIs(t, err, context.Canceled, "RoundTrip()")
	if finish == nil {
		t.Fatal("RoundTrip did not start a preflight")
	}
	finish(deniedProviderEgress("unsafe_address"))
	mapped := provider.ClassifyEgressOutcome(outcome, err)
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: provider.CodeCanceled, Dispatch: provider.DispatchNotDispatched, Retry: provider.RetryNever}, "late preflight overwrote caller cancellation")
	testassert.ErrorIsNot(t, mapped, provider.ErrProviderEgressDenied, "late preflight policy leaked into caller cancellation")
}

func TestProviderEgressRoundTripperDoesNotClassifyCanceledRequestAfterConnectionAcquired(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, err := roundTripGuard(t, ctx, 0, func(request *http.Request) (*http.Response, error) {
		state := requireEgressCallState(t, request)
		finish := state.beginPreflight()
		defer func() { finish(request.Context().Err()) }()
		gotReusedConn(t, request)
		cancel()
		return nil, request.Context().Err()
	})
	testassert.ErrorIs(t, err, context.Canceled, "RoundTrip()")
	if denial := outcome.Denial(); denial != nil {
		t.Fatalf("recorded denial = %v, want nil after a connection was acquired", denial)
	}
}

func TestProviderEgressRoundTripperRecordsCompletedPreflightDenialAfterCancellation(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, err := roundTripGuard(t, ctx, 0, func(request *http.Request) (*http.Response, error) {
		state := requireEgressCallState(t, request)
		finish := state.beginPreflight()
		cancel()
		finish(deniedProviderEgress("unsafe_address"))
		return nil, request.Context().Err()
	})
	testassert.ErrorIs(t, err, context.Canceled, "RoundTrip()")
	testassert.ErrorIs(t, outcome.Denial(), provider.ErrProviderEgressDenied, "recorded denial")
}

func TestProviderEgressRoundTripperDoesNotReturnEgressDenialAfterConnectionAcquired(t *testing.T) {
	ctx, outcome := provider.WithEgressOutcome(context.Background())
	_, err := roundTripGuard(t, ctx, 0, func(request *http.Request) (*http.Response, error) {
		state := requireEgressCallState(t, request)
		finish := state.beginPreflight()
		gotReusedConn(t, request)
		denial := deniedProviderEgress("connection_failed")
		finish(denial)
		return nil, denial
	})
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
	response, err := roundTripGuard(t, context.Background(), 4, func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 5,
			Body:          body,
		}, nil
	})
	if response != nil {
		t.Fatalf("RoundTrip() response = %#v, want nil", response)
	}
	testassert.ErrorIs(t, err, provider.ErrProviderResponseTooLarge, "RoundTrip()")
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
			response, err := roundTripGuard(t, context.Background(), 4, func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, ContentLength: -1, Body: upstream}, nil
			})
			testassert.NoError(t, err, "RoundTrip() error")
			body, readErr := io.ReadAll(response.Body)
			if got, want := string(body), "1234"; got != want {
				t.Fatalf("bounded body = %q, want %q", got, want)
			}
			testassert.ErrorIs(t, readErr, provider.ErrProviderResponseTooLarge, "body read")
			testassert.NoError(t, response.Body.Close(), "close bounded body")
			if !upstream.closed {
				t.Fatal("bounded body Close did not close upstream body")
			}
		})
	}
}

func TestProviderEgressRoundTripperBoundsBodyWhenDeclaredLengthIsTooSmall(t *testing.T) {
	t.Parallel()

	response, err := roundTripGuard(t, context.Background(), 4, func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 1,
			Body:          io.NopCloser(strings.NewReader("12345")),
		}, nil
	})
	testassert.NoError(t, err)
	t.Cleanup(func() { _ = response.Body.Close() })
	body, readErr := io.ReadAll(response.Body)
	if got, want := string(body), "1234"; got != want {
		t.Fatalf("bounded body = %q, want %q", got, want)
	}
	testassert.ErrorIs(t, readErr, provider.ErrProviderResponseTooLarge, "body read")
}

func TestProviderEgressRoundTripperPreservesBoundedStreamingResponse(t *testing.T) {
	t.Parallel()

	payload := "data: ok\n\n"
	upstream := &trackingResponseBody{Reader: &fragmentedReader{data: []byte(payload), maximum: 2}}
	response, err := roundTripGuard(t, context.Background(), int64(len(payload)), func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: -1,
			Header:        http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:          upstream,
		}, nil
	})
	testassert.NoError(t, err)
	body, readErr := io.ReadAll(response.Body)
	testassert.NoError(t, readErr, "read bounded streaming body")
	if got := string(body); got != payload {
		t.Fatalf("streaming body = %q, want %q", got, payload)
	}
	testassert.NoError(t, response.Body.Close(), "close bounded streaming body")
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

	listener := testkit.LoopbackListener(t)
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
	testassert.NoError(t, err)

	firstRequest, err := http.NewRequest(http.MethodGet, "https://example.com/first", nil)
	testassert.NoError(t, err)
	firstResponse, err := client.Transport.RoundTrip(firstRequest)
	testassert.NoError(t, err, "first RoundTrip() error")
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
	testassert.NoError(t, err)
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
		testassert.NoError(t, readErr, "read first response body")
	case <-time.After(2 * time.Second):
		t.Fatal("first response did not complete")
	}
	testassert.NoError(t, firstResponse.Body.Close(), "close first response body")
	select {
	case <-secondObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("idle connection did not satisfy the second request")
	}

	cancelSecond()
	select {
	case roundTripErr := <-secondResult:
		testassert.ErrorIs(t, roundTripErr, context.Canceled, "second RoundTrip()")
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
	testassert.NoError(t, err, "newProviderEgressPolicy() error")
	_, err = policy.DialContext(context.Background(), "tcp", "provider-mock:8081")
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "local provider mock DialContext()")
	if strings.Contains(err.Error(), "172.20.0.10") {
		t.Fatalf("local provider mock error leaked Docker address: %q", err)
	}
}

func TestProviderEgressTransportRejectsArbitraryRequestURL(t *testing.T) {
	t.Parallel()

	resolver := &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}}
	client, err := newProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint(), resolver, nil, config.DefaultProviderResponseBytes)
	testassert.NoError(t, err)
	request, err := http.NewRequest(http.MethodPost, "https://attacker.example/continuation-secret", strings.NewReader("prompt-secret"))
	testassert.NoError(t, err)
	request.Header.Set("Authorization", "Bearer api-key-secret")
	_, err = client.Transport.RoundTrip(request)
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "RoundTrip()")
	for _, secret := range []string{"attacker.example", "continuation-secret", "prompt-secret", "api-key-secret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("RoundTrip() leaked %q in %q", secret, err)
		}
	}
	if resolver.lookups != 0 {
		t.Fatalf("resolver calls = %d, want no lookup for an unconfigured host", resolver.lookups)
	}

	hostOverride, err := http.NewRequest(http.MethodGet, "https://provider.example/v1", nil)
	testassert.NoError(t, err)
	hostOverride.Host = "attacker.example"
	_, err = client.Transport.RoundTrip(hostOverride)
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "RoundTrip() host override")
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
	server := testkit.LoopbackTLSServer(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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
	testassert.NoError(t, err)
	response, err := client.Get("https://example.com/redirect")
	testassert.NoError(t, err, "GET through TLS mock")
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
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "newProviderEgressHTTPClient()")
	if got, want := err.Error(), "provider egress blocked: invalid_transport"; got != want {
		t.Fatalf("newProviderEgressHTTPClient() error = %q, want %q", got, want)
	}
}

func TestCloneProviderTransportClearsCallerAlternateProtocol(t *testing.T) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{
		"caller-protocol": func(string, *tls.Conn) http.RoundTripper {
			return testkit.RoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("caller alternate protocol should not run")
			})
		},
	}
	cloned, err := cloneProviderTransport(&http.Client{Transport: transport})
	testassert.NoError(t, err, "cloneProviderTransport() error")
	if cloned.TLSNextProto != nil {
		t.Fatalf("cloneProviderTransport() retained caller alternate protocols: %#v", cloned.TLSNextProto)
	}
}

func TestProviderEgressTransportRejectsTLSHostnameOverride(t *testing.T) {
	t.Parallel()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{ServerName: "cohosted.example"}
	_, err := newProviderEgressHTTPClient(&http.Client{Transport: transport}, providerEgressEndpoint(), nil, nil, config.DefaultProviderResponseBytes)
	testassert.ErrorIs(t, err, ErrProviderEgressDenied, "newProviderEgressHTTPClient()")
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
	testassert.NoError(t, err)
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

// TestModelSyncCatalogFetchHasItsOwnResponseLimit keeps the OpenRouter catalog
// fetch off limits.provider_response_bytes, which is sized to fit a paid
// response in its durable record (#1319) and is smaller than a catalog.
func TestModelSyncCatalogFetchHasItsOwnResponseLimit(t *testing.T) {
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{HTTPClient: &http.Client{}}}
	client, err := factory.modelSyncHTTPClient(providerEgressEndpoint())
	testassert.NoError(t, err)
	guard, ok := client.Transport.(*providerEgressRoundTripper)
	if !ok {
		t.Fatalf("client transport = %T, want production provider egress guard", client.Transport)
	}
	if got := guard.responseByteLimit(); got != modelsync.MaxResponseBytes || got <= config.DefaultProviderResponseBytes {
		t.Fatalf("catalog response limit = %d, want %d", got, modelsync.MaxResponseBytes)
	}
}

func TestNewProviderEgressHTTPClientUsesProductionPolicy(t *testing.T) {
	client, err := NewProviderEgressHTTPClient(&http.Client{}, providerEgressEndpoint())
	testassert.NoError(t, err, "NewProviderEgressHTTPClient() error")
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

func localProviderMockEndpoint(t *testing.T) config.EndpointConfig {
	t.Helper()
	_, source, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data := testassert.ReadFile(t, filepath.Join(filepath.Dir(source), "..", "..", "deploy", "local", "config.yaml"))
	loaded, err := config.Load(data)
	testassert.NoError(t, err, "load local config")
	endpoint, ok := loaded.Endpoints["provider-mock"]
	if !ok {
		t.Fatal("local config is missing provider-mock endpoint")
	}
	return endpoint
}

func TestBlockedProviderAddressDecodesNAT64WellKnownPrefix(t *testing.T) {
	for address, blocked := range map[string]bool{
		"64:ff9b::808:808":   false, // DNS64 synthesis of public 8.8.8.8.
		"64:ff9b::a9fe:a9fe": true,  // 169.254.169.254 metadata.
		"64:ff9b::a00:1":     true,  // 10.0.0.1 private.
		"64:ff9b::7f00:1":    true,  // 127.0.0.1 loopback.
		"64:ff9b:1::a00:1":   true,  // Local-use NAT64 is not decoded.
	} {
		if got := blockedProviderAddress(netip.MustParseAddr(address)); got != blocked {
			t.Errorf("blockedProviderAddress(%s) = %t, want %t", address, got, blocked)
		}
	}
}

// providerExamplePolicy allows only provider.example on port 443.
func providerExamplePolicy() *providerEgressPolicy {
	return &providerEgressPolicy{allowedHosts: map[string]map[string]struct{}{"provider.example": {"443": {}}}}
}

// providerExampleRequest is a POST to provider.example's Responses endpoint.
func providerExampleRequest(t *testing.T, ctx context.Context) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://provider.example/v1/responses", nil)
	testassert.NoError(t, err)
	return request
}

// roundTripGuard sends providerExampleRequest under ctx through an egress
// guard over next, bounding responses at maxResponseBytes (zero for none).
func roundTripGuard(t *testing.T, ctx context.Context, maxResponseBytes int64, next testkit.RoundTripFunc) (*http.Response, error) {
	t.Helper()
	guard := &providerEgressRoundTripper{policy: providerExamplePolicy(), next: next, maxResponseBytes: maxResponseBytes}
	return guard.RoundTrip(providerExampleRequest(t, ctx))
}

// requireNotDispatched fails the test unless mapped is a not-dispatched
// provider error with code and retry; what names the classified outcome.
func requireNotDispatched(t *testing.T, mapped *provider.Error, code provider.Code, retry provider.RetryDisposition, what string) {
	t.Helper()
	testkit.ProviderError(t, mapped, testkit.ProviderClass{Code: code, Dispatch: provider.DispatchNotDispatched, Retry: retry}, fmt.Sprintf("classified %s", what))
}

// requireEgressCallState returns the egress call state request carries.
func requireEgressCallState(t *testing.T, request *http.Request) *providerEgressCallState {
	t.Helper()
	state := providerEgressCallStateFromContext(request.Context())
	if state == nil {
		t.Fatal("RoundTrip request is missing egress call state")
	}
	return state
}

// gotReusedConn reports a reused connection through request's client trace.
func gotReusedConn(t *testing.T, request *http.Request) {
	t.Helper()
	trace := httptrace.ContextClientTrace(request.Context())
	if trace == nil || trace.GotConn == nil {
		t.Fatal("RoundTrip request is missing GotConn trace")
	}
	trace.GotConn(httptrace.GotConnInfo{Reused: true})
}
