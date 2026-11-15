package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestReceivedServerErrorClearsAClosedChannel(t *testing.T) {
	var channel <-chan error = make(chan error)
	failure := errors.New("listener failed")
	if err := receivedServerError(&channel, failure, true); err != failure || channel == nil {
		t.Fatalf("open channel: err = %v, channel cleared = %v", err, channel == nil)
	}
	if err := receivedServerError(&channel, nil, false); err != nil || channel != nil {
		t.Fatalf("closed channel: err = %v, channel cleared = %v", err, channel == nil)
	}
}

func TestContextOrBackgroundKeepsAGivenContext(t *testing.T) {
	type key struct{}
	given := context.WithValue(context.Background(), key{}, "v")
	if got := contextOrBackground(given); got != given {
		t.Fatalf("given context was replaced")
	}
	var unset context.Context
	if got := contextOrBackground(unset); got != context.Background() {
		t.Fatalf("nil context = %v", got)
	}
}

func TestGenerateSampleIndexReadsTheCacheVariant(t *testing.T) {
	if got := generateSampleIndex(llm.GenerateRequestV1{}); got != 0 {
		t.Fatalf("no cache policy = %d", got)
	}
	if got := generateSampleIndex(llm.GenerateRequestV1{Cache: &llm.CachePolicyV1{Variant: 3}}); got != 3 {
		t.Fatalf("variant 3 = %d", got)
	}
}

func TestEndpointProfileNamesTheEndpointOnError(t *testing.T) {
	profile, err := endpointProfile("ep", 7, nil)
	if err != nil || profile == nil || *profile != 7 {
		t.Fatalf("built profile = %v, %v", profile, err)
	}
	cause := errors.New("bad profile")
	profile, err = endpointProfile("ep", 0, cause)
	if profile != nil || !errors.Is(err, cause) || err.Error() != `endpoint "ep": bad profile` {
		t.Fatalf("failed profile = %v, %v", profile, err)
	}
}
