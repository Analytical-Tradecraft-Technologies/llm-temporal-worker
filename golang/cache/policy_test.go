package cache

import (
	"math"
	"testing"
	"time"
)

func TestNewPolicyAndWireAge(t *testing.T) {
	policy, err := NewPolicy(60, 2)
	if err != nil {
		t.Fatal(err)
	}
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
		{name: "zero age", seconds: 0},
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
	if err := ValidateOptional(nil, OperationGenerate, nil); err != nil {
		t.Fatalf("omitted policy rejected: %v", err)
	}
	positive := 0.7
	zero := 0.0
	valid, err := NewPolicy(60, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := valid.Validate(OperationGenerate, &positive); err != nil {
		t.Fatalf("positive temperature rejected: %v", err)
	}
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
			if err := test.policy.Validate(test.operation, test.temperature); err == nil {
				t.Fatal("unsafe cache policy unexpectedly accepted")
			}
		})
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), -0.1} {
		if err := (Policy{MaxAge: time.Minute}).Validate(OperationGenerate, &value); err == nil {
			t.Fatalf("invalid temperature %v unexpectedly accepted", value)
		}
	}
	compact, err := NewPolicy(60, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := compact.Validate(OperationCompact, &positive); err != nil {
		t.Fatalf("compact variant zero rejected: %v", err)
	}
	if err := compact.ValidateWithMaximum(OperationGenerate, &positive, 30*time.Second); err == nil {
		t.Fatal("operator maximum was ignored")
	}
}

func TestPolicyMaxAgeSecondsRejectsSubsecond(t *testing.T) {
	if _, err := (Policy{MaxAge: time.Millisecond}).MaxAgeSeconds(); err == nil {
		t.Fatal("subsecond policy unexpectedly converted to wire seconds")
	}
}
