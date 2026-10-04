package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestBudgetKeySpaceKeepsGenerationFamiliesInOneHashSlot(t *testing.T) {
	keys, err := NewBudgetKeySpace(KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: []byte(strings.Repeat("k", 32))})
	if err != nil {
		t.Fatalf("NewBudgetKeySpace: %v", err)
	}
	for _, key := range []string{keys.ActiveGenerationKey(), keys.EventsKey(), keys.WorkersKey(), keys.ManifestKey("generation-1")} {
		if !strings.Contains(key, ":{budget}:") {
			t.Fatalf("key %q does not retain configured hash tag", key)
		}
	}
	if strings.Contains(keys.ManifestKey("tenant/raw"), "tenant/raw") {
		t.Fatal("manifest key exposed a raw generation identifier")
	}
}

func TestMemoryBudgetEventPortBroadcastsFromCursor(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	base := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventReserve, GenerationID: "generation-1", OperationHash: strings.Repeat("a", 64), MemberHash: strings.Repeat("b", 64), Revision: 1, NanoDelta: 4, OccurredAt: time.Unix(10, 0).UTC()}
	first, err := port.Append(context.Background(), base)
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	base.Kind = BudgetEventRelease
	second, err := port.Append(context.Background(), base)
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if first == second {
		t.Fatal("stream IDs are not monotonic")
	}
	rows, err := port.Read(context.Background(), first, 10)
	if err != nil {
		t.Fatalf("read after cursor: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != second || rows[0].Event.Kind != BudgetEventRelease {
		t.Fatalf("read after cursor = %#v", rows)
	}
}

func TestMemoryBudgetEventPortAcceptsCompoundRedisCursor(t *testing.T) {
	port := &MemoryBudgetEventPort{}
	event := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh, GenerationID: BudgetGenerationID("generation"), Revision: 1, OccurredAt: time.Unix(1, 0)}
	if _, err := port.Append(context.Background(), event); err != nil {
		t.Fatalf("append first event: %v", err)
	}
	if _, err := port.Append(context.Background(), event); err != nil {
		t.Fatalf("append second event: %v", err)
	}
	records, err := port.Read(context.Background(), "1-1", 10)
	if err != nil {
		t.Fatalf("compound cursor: %v", err)
	}
	if len(records) != 1 || records[0].ID != "2-0" {
		t.Fatalf("compound cursor records = %#v, want second event", records)
	}
}

func TestMemoryBudgetGenerationPortPublishesAndValidatesPointer(t *testing.T) {
	port := new(MemoryBudgetGenerationPort)
	manifest := testBudgetManifest(t)
	pointer, err := port.PublishGeneration(context.Background(), manifest)
	if err != nil {
		t.Fatalf("publish generation: %v", err)
	}
	active, err := port.ActiveGeneration(context.Background())
	if err != nil || active != pointer {
		t.Fatalf("active generation = %#v, %v; want %#v", active, err, pointer)
	}
	loaded, err := port.LoadManifest(context.Background(), pointer)
	if err != nil || loaded.GenerationID != manifest.GenerationID {
		t.Fatalf("load manifest = %#v, %v", loaded, err)
	}
	tampered := pointer
	tampered.ManifestDigest = strings.Repeat("0", 64)
	if _, err := port.LoadManifest(context.Background(), tampered); err == nil {
		t.Fatal("tampered pointer accepted")
	}
	mutated := manifest
	mutated.IncarnationID = BudgetIncarnationID("incarnation-2")
	if _, err := port.PublishGeneration(context.Background(), mutated); !errors.Is(err, ErrBudgetGenerationConflict) {
		t.Fatalf("mutable generation accepted: %v", err)
	}
	if _, err := port.PublishGeneration(context.Background(), manifest); err != nil {
		t.Fatalf("idempotent generation publish: %v", err)
	}
	if active, err := port.ActiveGeneration(context.Background()); err != nil || active != pointer {
		t.Fatalf("active generation after idempotent publish = %#v, %v; want %#v", active, err, pointer)
	}
}

func TestBudgetStreamEventRejectsRawOrUnboundedAccounting(t *testing.T) {
	event := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventReserve, GenerationID: "generation", OperationHash: "operation", MemberHash: strings.Repeat("b", 64), OccurredAt: time.Unix(1, 0)}
	if err := event.Validate(); err == nil {
		t.Fatal("non-digest operation identifier accepted")
	}
	event.OperationHash = strings.Repeat("a", 64)
	event.NanoDelta = -1
	if err := event.Validate(); err == nil {
		t.Fatal("negative nano delta accepted")
	}
	event.NanoDelta = int64(pricing.NanoUSDSafeLimit) + 1
	if err := event.Validate(); err == nil {
		t.Fatal("unsafe nano delta accepted")
	}
}

func TestBudgetStreamClaimAndExpiryRequireOpaqueMembers(t *testing.T) {
	for _, kind := range []BudgetStreamEventKind{BudgetEventClaim, BudgetEventExpire} {
		t.Run(string(kind), func(t *testing.T) {
			event := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: kind, GenerationID: "generation", OccurredAt: time.Unix(1, 0)}
			if err := event.Validate(); err == nil {
				t.Fatal("missing member digest accepted")
			}
			event.MemberHash = strings.Repeat("b", 64)
			if kind == BudgetEventClaim {
				if err := event.Validate(); err == nil {
					t.Fatal("claim without operation digest accepted")
				}
				event.OperationHash = strings.Repeat("a", 64)
			}
			if _, err := event.Marshal(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
