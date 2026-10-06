package redis

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func FuzzOperationCodecRoundTrip(f *testing.F) {
	seed, err := encodeOperation(admission.Operation{
		ID:               "operation-fuzz",
		ScopeKey:         "tenant-fuzz",
		RequestDigest:    [32]byte{1, 2, 3},
		State:            admission.StateReserved,
		ReservedMicroUSD: 7,
		IncurredMicroUSD: 0,
		FinalMicroUSD:    0,
		Reservations: []admission.WindowReservation{{
			PolicyID: "policy", WindowID: "window", Amount: pricing.MicroUSD(7), Limit: pricing.MicroUSD(10), BucketNanos: 1, DurationNanos: 2,
		}},
		DispatchToken: "token-fuzz",
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte(`{"schema":"admission/v1"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2<<20 {
			return
		}
		operation, err := decodeOperation(data)
		if err != nil {
			return
		}
		encoded, err := encodeOperation(operation)
		if err != nil {
			t.Fatalf("encode accepted operation: %v", err)
		}
		roundTripped, err := decodeOperation(encoded)
		if err != nil {
			t.Fatalf("decode re-encoded operation: %v", err)
		}
		if !reflect.DeepEqual(operation, roundTripped) {
			t.Fatalf("operation codec changed semantic value:\nfirst=%#v\nsecond=%#v", operation, roundTripped)
		}
	})
}
