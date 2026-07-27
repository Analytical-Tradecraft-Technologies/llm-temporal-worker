package budget

import (
	"math"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestMatcherCoversDimensionsWildcardsAndMissingContext(t *testing.T) {
	base := MatchContext{
		Tenant:       "acme",
		Project:      "chat",
		Actor:        "svc-worker-42",
		Environment:  "production",
		LogicalModel: "reasoning",
		EndpointID:   "endpoint-a",
		ServiceClass: llm.ServiceClassPriority,
	}

	tests := []struct {
		name    string
		matcher Matcher
		value   MatchContext
		want    bool
	}{
		{
			name:    "exact all fields",
			matcher: Matcher{Tenant: "acme", Project: "chat", ActorPrefix: "svc-", Environment: "production", LogicalModel: "reasoning", EndpointID: "endpoint-a", ServiceClass: llm.ServiceClassPriority},
			value:   base,
			want:    true,
		},
		{
			name:    "wildcards and omitted optional fields",
			matcher: Matcher{Tenant: "*", Project: "", Environment: "production", LogicalModel: "*", EndpointID: "*"},
			value:   base,
			want:    true,
		},
		{name: "tenant mismatch", matcher: Matcher{Tenant: "other"}, value: base},
		{name: "missing value does not match exact rule", matcher: Matcher{Project: "chat"}, value: MatchContext{}},
		{name: "actor prefix mismatch", matcher: Matcher{ActorPrefix: "user-"}, value: base},
		{name: "service class mismatch", matcher: Matcher{ServiceClass: llm.ServiceClassEconomy}, value: base},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.matcher.Matches(test.value); got != test.want {
				t.Fatalf("Matches() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestMatchPlanPreservesPolicyOrderAndUsesAttemptedClass(t *testing.T) {
	request := llm.Request{
		Model:        "logical-model",
		ServiceClass: llm.ServiceClassStandard,
		Context:      llm.RequestContext{Tenant: "acme", Project: "chat", Actor: "svc-api"},
	}
	policies := []Policy{
		{ID: "tenant", Match: Matcher{Tenant: "acme"}, Windows: []Window{{ID: "hour", Limit: 10}}},
		{ID: "priority", Match: Matcher{ServiceClass: llm.ServiceClassPriority}, Windows: []Window{{ID: "priority-hour", Limit: 20}}},
		{ID: "other", Match: Matcher{Tenant: "other"}, Windows: []Window{{ID: "other-hour", Limit: 30}}},
	}
	plan := routing.Plan{Candidates: []routing.Candidate{
		{ID: "priority-route", EndpointID: "endpoint-a", AttemptedClass: llm.ServiceClassPriority},
		{ID: "standard-route", EndpointID: "endpoint-b"},
	}}

	got := MatchPlan(policies, request, plan, "production")
	if len(got) != 2 {
		t.Fatalf("matched candidate count = %d, want 2", len(got))
	}
	if len(got["priority-route"]) != 2 || got["priority-route"][0].PolicyID != "tenant" || got["priority-route"][1].PolicyID != "priority" {
		t.Fatalf("priority matches = %#v, want tenant then priority", got["priority-route"])
	}
	if got["priority-route"][0].Context.ServiceClass != llm.ServiceClassPriority {
		t.Fatalf("priority context class = %q, want priority", got["priority-route"][0].Context.ServiceClass)
	}
	if len(got["standard-route"]) != 1 || got["standard-route"][0].PolicyID != "tenant" {
		t.Fatalf("standard matches = %#v, want tenant only", got["standard-route"])
	}
}

func TestPolicyAndWindowValidationRejectsUnsafeBounds(t *testing.T) {
	valid := Window{ID: "hour", Duration: time.Hour, Bucket: time.Minute, Limit: 100}
	if err := (Policy{ID: "acme", Match: Matcher{Tenant: "acme"}, Windows: []Window{valid}}).Validate(64); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}

	for _, test := range []struct {
		name   string
		policy Policy
		max    int
	}{
		{name: "missing id", policy: Policy{Windows: []Window{valid}}, max: 64},
		{name: "missing windows", policy: Policy{ID: "acme"}, max: 64},
		{name: "missing matcher restriction", policy: Policy{ID: "acme", Windows: []Window{valid}}, max: 64},
		{name: "wildcard-only matcher", policy: Policy{ID: "acme", Match: Matcher{Tenant: "*", ActorPrefix: "*"}, Windows: []Window{valid}}, max: 64},
		{name: "unknown service class", policy: Policy{ID: "acme", Match: Matcher{ServiceClass: "provider-default"}, Windows: []Window{valid}}, max: 64},
		{name: "zero duration", policy: Policy{ID: "acme", Windows: []Window{{Bucket: time.Minute, Limit: 1}}}, max: 64},
		{name: "bucket exceeds duration", policy: Policy{ID: "acme", Windows: []Window{{Duration: time.Minute, Bucket: time.Hour, Limit: 1}}}, max: 64},
		{name: "too many buckets", policy: Policy{ID: "acme", Windows: []Window{{Duration: time.Hour, Bucket: time.Minute, Limit: 1}}}, max: 10},
		{name: "unsafe limit", policy: Policy{ID: "acme", Windows: []Window{{Duration: time.Hour, Bucket: time.Minute, Limit: pricing.RedisSafeLimit + 1}}}, max: 64},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.policy.Validate(test.max); err == nil {
				t.Fatal("unsafe policy accepted")
			}
		})
	}
}

func TestFloorDivAndWindowRangeHandlePreEpochTimes(t *testing.T) {
	for _, test := range []struct {
		value, divisor int64
		want           int64
	}{
		{value: 0, divisor: 10, want: 0},
		{value: 9, divisor: 10, want: 0},
		{value: 10, divisor: 10, want: 1},
		{value: -1, divisor: 10, want: -1},
		{value: -10, divisor: 10, want: -1},
		{value: -11, divisor: 10, want: -2},
	} {
		if got := FloorDiv(test.value, test.divisor); got != test.want {
			t.Errorf("FloorDiv(%d, %d) = %d, want %d", test.value, test.divisor, got, test.want)
		}
	}

	window := Window{Duration: 2 * time.Second, Bucket: time.Second}
	first, last := window.Range(time.Unix(0, int64(-500*time.Millisecond)))
	if first != -3 || last != -1 {
		t.Fatalf("pre-epoch range = %d..%d, want -3..-1", first, last)
	}
}

func TestWindowActiveSumIncludesFullIntersectingBuckets(t *testing.T) {
	window := Window{Duration: 2 * time.Second, Bucket: time.Second, Limit: 20}
	at := time.Unix(0, int64(2500*time.Millisecond))
	buckets := map[int64]pricing.MicroUSD{0: 2, 1: 3, 2: 4, 3: 100}

	got, err := window.ActiveSum(buckets, at)
	if err != nil {
		t.Fatal(err)
	}
	if got != 9 {
		t.Fatalf("active sum = %d, want 9", got)
	}

	if _, err := (Window{Duration: time.Second, Bucket: time.Second}).ActiveSum(map[int64]pricing.MicroUSD{0: -1}, time.Unix(0, 0)); err == nil {
		t.Fatal("negative bucket accepted")
	}
	if _, err := (Window{Duration: time.Second, Bucket: time.Second}).ActiveSum(map[int64]pricing.MicroUSD{0: pricing.RedisSafeLimit, 1: 1}, time.Unix(0, int64(time.Second))); err == nil {
		t.Fatal("bucket sum overflow accepted")
	}
}

func TestWindowCanReserveChecksLimitAndSafeAmount(t *testing.T) {
	window := Window{Duration: 2 * time.Second, Bucket: time.Second, Limit: 10}
	at := time.Unix(0, int64(2500*time.Millisecond))
	buckets := map[int64]pricing.MicroUSD{0: 4, 1: 3}

	ok, active, err := window.CanReserve(buckets, 3, at)
	if err != nil || !ok || active != 7 {
		t.Fatalf("CanReserve accepted = (%v, %d, %v), want (true, 7, nil)", ok, active, err)
	}
	if ok, active, err := window.CanReserve(buckets, 4, at); err != nil || ok || active != 7 {
		t.Fatalf("CanReserve over limit = (%v, %d, %v), want (false, 7, nil)", ok, active, err)
	}
	for _, amount := range []pricing.MicroUSD{-1, pricing.RedisSafeLimit + 1} {
		if ok, _, err := window.CanReserve(nil, amount, at); err == nil || ok {
			t.Fatalf("unsafe amount %d accepted: ok=%v err=%v", amount, ok, err)
		}
	}
}

func TestWindowRetryAfterWaitsForSlidingWindowExpiryAndCapacity(t *testing.T) {
	window := Window{Duration: 2 * time.Second, Bucket: time.Second, Limit: 10}
	at := time.Unix(0, int64(2250*time.Millisecond))
	firstBucketOnly, err := window.RetryAfter(map[int64]pricing.MicroUSD{0: 6}, 5, at)
	if err != nil {
		t.Fatal(err)
	}
	if firstBucketOnly != 750*time.Millisecond {
		t.Fatalf("single-bucket retry after = %s, want 750ms", firstBucketOnly)
	}

	buckets := map[int64]pricing.MicroUSD{0: 4, 1: 6}

	got, err := window.RetryAfter(buckets, 5, at)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1750*time.Millisecond {
		t.Fatalf("retry after = %s, want 1.75s", got)
	}

	if got, err := window.RetryAfter(nil, 1, at); err != nil || got != 0 {
		t.Fatalf("unblocked retry after = (%s, %v), want (0, nil)", got, err)
	}
	if got, err := window.RetryAfter(nil, 11, at); err != nil || got != math.MaxInt64 {
		t.Fatalf("impossible retry after = (%s, %v), want max duration", got, err)
	}
}
