package durable

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
)

type finalizationResponseStore struct {
	cache.ResponseRepository
	events *[]string
}

func (s finalizationResponseStore) Publish(context.Context, cache.ResponseEntry) error {
	*s.events = append(*s.events, "publish")
	return nil
}

func (s finalizationResponseStore) RecordUse(context.Context, cache.ResponseUse) error {
	*s.events = append(*s.events, "use")
	return nil
}

type finalizationFillStore struct {
	cache.FillRepository
	events *[]string
}

func (s finalizationFillStore) Complete(context.Context, cache.FillLease, cache.FillCompletion) error {
	*s.events = append(*s.events, "complete")
	return nil
}

func TestResponseCacheFinalizationOrderForGenerateAndCompact(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		t.Run(string(kind), func(t *testing.T) {
			events := []string{}
			now := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
			c, err := NewResponseCache(finalizationResponseStore{events: &events}, finalizationFillStore{events: &events}, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			key := cache.ResponseKey{ScopeID: "scope", Operation: kind}
			lease := cache.FillLease{Key: key, OperationID: "origin", Attempt: "attempt", AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
			entry := cache.ResponseEntry{ID: "entry", Key: key, OriginOperationID: lease.OperationID, OriginCheckpointID: "origin-checkpoint", CompletedAt: now.Add(time.Second)}
			completion := cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}
			settle := func(context.Context) error { events = append(events, "settle"); return nil }
			if err := c.CompleteAttempt(context.Background(), lease, completion, &entry, settle); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(events, []string{"publish", "settle", "complete"}) {
				t.Fatalf("order: %v", events)
			}
			events = nil
			use := cache.ResponseUse{ScopeID: key.ScopeID, EntryID: entry.ID, OperationID: "consumer", CheckpointID: "consumer-checkpoint", CompletedAt: now.Add(time.Minute)}
			if err := c.RecordUse(context.Background(), entry, use); err != nil || !reflect.DeepEqual(events, []string{"use"}) {
				t.Fatalf("use order: %v %v", events, err)
			}
			events = nil
			bad := entry
			bad.Key.ScopeID = "another-scope"
			if err := c.CompleteAttempt(context.Background(), lease, completion, &bad, settle); err == nil || len(events) != 0 {
				t.Fatalf("mismatched entry caused side effects: %v %v", err, events)
			}
			badUse := use
			badUse.EntryID = "other-entry"
			if err := c.RecordUse(context.Background(), entry, badUse); err == nil || len(events) != 0 {
				t.Fatalf("mismatched use caused side effects: %v %v", err, events)
			}
		})
	}
}
