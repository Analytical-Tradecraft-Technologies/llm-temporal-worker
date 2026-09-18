package postgres

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestOperationConcurrentBeginReplay(t *testing.T) {
	repository, ctx, cleanup := operationIntegrationRepository(t)
	defer cleanup()
	const (
		workers  = 100
		attempts = 8
	)
	for attempt := 0; attempt < attempts; attempt++ {
		request := admission.BeginRequest{ID: fmt.Sprintf("operation-concurrent-%d-%d", time.Now().UTC().UnixNano(), attempt), OperationKey: fmt.Sprintf("operation-concurrent-%d-%d", time.Now().UTC().UnixNano(), attempt), Actor: "postgres-test", ScopeKey: "concurrency/project", RequestDigest: admission.Digest([]byte("same")), ReservationUSD: pricing.MustUSD("0"), RequestManifest: []byte(`{}`)}
		results := make(chan admission.BeginResult, workers)
		errs := make(chan error, workers)
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer group.Done()
				<-start
				result, err := repository.Begin(ctx, request)
				results <- result
				errs <- err
			}()
		}
		close(start)
		group.Wait()
		close(results)
		close(errs)
		created := 0
		for result := range results {
			if !result.Existing {
				created++
			}
		}
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if created != 1 {
			t.Fatalf("attempt %d: created=%d, want exactly one durable operation", attempt, created)
		}
	}
}
