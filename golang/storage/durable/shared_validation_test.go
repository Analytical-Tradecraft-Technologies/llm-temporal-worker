package durable

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestCheckRoutePlanStagesAndContext(t *testing.T) {
	valid := RoutePlan{OperationID: "op", GenerationID: "gen", RouteID: "route", EndpointID: "endpoint", Provider: "provider", Model: "model"}
	if err := checkRoutePlan(context.Background(), "compact ", valid, nil, nil); err != nil {
		t.Fatalf("valid route = %v", err)
	}
	cause := errors.New("route port failed")
	if err := checkRoutePlan(context.Background(), "compact ", valid, cause, nil); !errors.Is(err, ErrV1Stage) || !errors.Is(err, cause) || !strings.Contains(err.Error(), ": compact route: ") {
		t.Fatalf("port error = %v", err)
	}
	if err := checkRoutePlan(context.Background(), "", RoutePlan{}, nil, nil); !errors.Is(err, ErrV1Stage) || !strings.Contains(err.Error(), ": route: ") {
		t.Fatalf("invalid route = %v", err)
	}
	mismatched := &responseCachePreparation{disposition: CacheMiss}
	if err := checkRoutePlan(context.Background(), "", valid, nil, mismatched); !errors.Is(err, ErrV1Stage) || !strings.Contains(err.Error(), ": cache route: ") {
		t.Fatalf("cache route = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checkRoutePlan(canceled, "", valid, nil, nil); err != context.Canceled {
		t.Fatalf("canceled route = %v, want unwrapped context.Canceled", err)
	}
}

func TestUnchangedIdentity(t *testing.T) {
	if err := unchangedIdentity[OperationID]("op", "op", "operation"); err != nil {
		t.Fatalf("same identity = %v", err)
	}
	if err := unchangedIdentity[GenerationID]("gen", "other", "generation"); err == nil || err.Error() != "generation id changed during Redis acceptance" {
		t.Fatalf("changed identity = %v", err)
	}
	if err := unchangedIdentity[OperationID]("", "op", "operation"); err == nil || err.Error() != "operation id is empty or unsafe" {
		t.Fatalf("invalid requested identity = %v", err)
	}
	if err := validateLeaseIdentities("op", "gen", ""); err == nil || err.Error() != "incarnation id is empty or unsafe" {
		t.Fatalf("missing incarnation = %v", err)
	}
}

func TestAdjustReferenceTotals(t *testing.T) {
	bucket, entry := pricing.MustUSD("1.00"), pricing.MustUSD("0.40")
	if err := adjustReferenceTotals(pricing.USD{}, pricing.USD.Sub, &bucket, &entry); err != nil || bucket.Cmp(pricing.MustUSD("1.00")) != 0 {
		t.Fatalf("zero delta = %v, %v", bucket, err)
	}
	if err := adjustReferenceTotals(pricing.MustUSD("0.25"), pricing.USD.Sub, &bucket, &entry); err != nil || bucket.Cmp(pricing.MustUSD("0.75")) != 0 || entry.Cmp(pricing.MustUSD("0.15")) != 0 {
		t.Fatalf("subtract = %v, %v, %v", bucket, entry, err)
	}
	if err := adjustReferenceTotals(pricing.MustUSD("0.20"), pricing.USD.Sub, &bucket, &entry); err == nil {
		t.Fatalf("negative entry total = %v, want error", entry)
	}
}
