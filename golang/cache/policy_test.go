package cache

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"math"
	"testing"
	"time"
)

func TestNewPolicyAndWireAge(t *testing.T) {
	policy := mustPolicy(t, 60, 2)
	if policy.MaxAge != time.Minute || policy.Variant != 2 {
		t.Fatalf("policy=%#v", policy)
	}
	if got, err := policy.MaxAgeSeconds(); err != nil || got != 60 {
		t.Fatalf("wire age=%d err=%v", got, err)
	}
	for _, test := range []struct {
		name    string
		seconds int64
		variant int32
	}{
		{name: "negative age", seconds: -1},
		{name: "too old", seconds: int64(DefaultMaximumAge/time.Second) + 1},
		{name: "negative variant", seconds: 1, variant: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := NewPolicy(test.seconds, test.variant)
			if err == nil {
				t.Fatal("invalid age unexpectedly accepted")
			}
			_ = policy
		})
	}
}

func TestValidateOptionalCachePolicy(t *testing.T) {
	testassert.NoError(t, ValidateOptional(nil, OperationGenerate, nil), "omitted policy rejected")
	positive := 0.7
	zero := 0.0
	valid := mustPolicy(t, 60, 2)
	testassert.NoError(t, valid.Validate(OperationGenerate, &positive), "positive temperature rejected")
	for _, test := range []struct {
		name        string
		operation   OperationKind
		policy      Policy
		temperature *float64
	}{
		{name: "unknown temperature", operation: OperationGenerate, policy: valid},
		{name: "zero temperature", operation: OperationGenerate, policy: valid, temperature: &zero},
		{name: "compact variant", operation: OperationCompact, policy: valid, temperature: &positive},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.policy.Validate(test.operation, test.temperature); err != nil {
				t.Fatal("independent cache sample rejected", err)
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), -0.1} {
		if err := (Policy{MaxAge: time.Minute}).Validate(OperationGenerate, &value); err == nil {
			t.Fatalf("invalid temperature %v unexpectedly accepted", value)
		}
	}
	compact := mustPolicy(t, 60, 0)
	testassert.NoError(t, compact.Validate(OperationCompact, &positive), "compact variant zero rejected")
	if err := compact.ValidateWithMaximum(OperationGenerate, &positive, 30*time.Second); err == nil {
		t.Fatal("operator maximum was ignored")
	}
}

func TestPolicyMaxAgeSecondsRejectsSubsecond(t *testing.T) {
	if _, err := (Policy{MaxAge: time.Millisecond}).MaxAgeSeconds(); err == nil {
		t.Fatal("subsecond policy unexpectedly converted to wire seconds")
	}
}

func TestOmittedMaximumAgePolicy(t *testing.T) {
	policy := mustPolicy(t, 0, 0)
	testassert.NoError(t, policy.Validate(OperationGenerate, nil))
	if seconds, err := policy.MaxAgeSeconds(); err != nil || seconds != 0 {
		t.Fatalf("age=%d error=%v", seconds, err)
	}
}

func mustPolicy(t *testing.T, maxAgeSeconds int64, variant int32) Policy {
	t.Helper()
	policy, err := NewPolicy(maxAgeSeconds, variant)
	testassert.NoError(t, err)
	return policy
}
