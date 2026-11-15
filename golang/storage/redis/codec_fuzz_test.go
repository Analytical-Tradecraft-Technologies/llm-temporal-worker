package redis

import (
	"reflect"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/admission"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
	testassert.NoError(f, err)
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
		testassert.NoError(t, err, "encode accepted operation")
		roundTripped, err := decodeOperation(encoded)
		testassert.NoError(t, err, "decode re-encoded operation")
		if !reflect.DeepEqual(operation, roundTripped) {
			t.Fatalf("operation codec changed semantic value:\nfirst=%#v\nsecond=%#v", operation, roundTripped)
		}
	})
}
