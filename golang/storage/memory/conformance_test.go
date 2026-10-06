package memory

import (
	"bytes"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/conformance"
)

func TestStoreFactoryConformance(t *testing.T) {
	conformance.Run(t, conformance.StoreFactory{
		Name: "memory",
		New: func(t testing.TB) conformance.Stores {
			t.Helper()
			now := time.Now().UTC()
			keyring, err := state.NewKeyring([]state.Key{{
				ID:      "conformance",
				Secret:  bytes.Repeat([]byte{1}, 32),
				Primary: true,
			}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			continuations, err := NewContinuationStore(ContinuationOptions{
				Keyring: keyring,
				Clock:   func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			return conformance.Stores{
				Admission:     NewAdmissionStore(AdmissionOptions{Clock: func() time.Time { return now }}),
				Continuations: continuations,
				Now:           func() time.Time { return now },
			}
		},
	})
}
