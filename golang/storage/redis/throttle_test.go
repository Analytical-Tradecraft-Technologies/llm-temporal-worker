package redis

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

type throttleHarness struct {
	records  map[string]string
	counters map[string]int64
}

func newThrottleHarness() *throttleHarness {
	return &throttleHarness{records: make(map[string]string), counters: make(map[string]int64)}
}

func (h *throttleHarness) Get(_ context.Context, key string) (string, error) {
	value, ok := h.records[key]
	if !ok {
		return "", redisclient.Nil
	}
	return value, nil
}

func (h *throttleHarness) Run(_ context.Context, _ string, keys []string, args ...string) ([]any, error) {
	if len(args) == 0 || len(keys) < 2 {
		return nil, errors.New("invalid throttle invocation")
	}
	switch args[0] {
	case "acquire":
		var incoming throttleWire
		if err := json.Unmarshal([]byte(args[1]), &incoming); err != nil {
			return []any{"invalid_request", ""}, nil
		}
		if existing, ok := h.records[keys[0]]; ok {
			var current throttleWire
			_ = json.Unmarshal([]byte(existing), &current)
			if current.Digest != incoming.Digest {
				return []any{"conflict", ""}, nil
			}
			return []any{"existing", existing}, nil
		}
		for index, limit := range incoming.Limits {
			if h.counters[keys[index+1]]+limit.Amount > limit.Limit {
				return []any{"denied", ""}, nil
			}
		}
		for index, limit := range incoming.Limits {
			h.counters[keys[index+1]] += limit.Amount
		}
		encoded, _ := json.Marshal(incoming)
		h.records[keys[0]] = string(encoded)
		return []any{"created", string(encoded)}, nil
	case "release":
		encoded, ok := h.records[keys[0]]
		if !ok {
			return []any{"not_found", ""}, nil
		}
		var current throttleWire
		_ = json.Unmarshal([]byte(encoded), &current)
		if current.Digest != args[1] || len(current.Limits) != len(keys)-1 {
			return []any{"conflict", ""}, nil
		}
		for index, limit := range current.Limits {
			if h.counters[keys[index+1]] < limit.Amount {
				return []any{"state_unavailable", ""}, nil
			}
		}
		for index, limit := range current.Limits {
			h.counters[keys[index+1]] -= limit.Amount
		}
		delete(h.records, keys[0])
		return []any{"released", ""}, nil
	default:
		return []any{"invalid_request", ""}, nil
	}
}

func TestThrottleFunctionMetadataIsStableAndSeparate(t *testing.T) {
	if ThrottleFunctionVersion == AdmissionFunctionVersion || ThrottleFunctionLibrary == AdmissionFunctionLibrary {
		t.Fatal("throttle function identity aliases monetary admission")
	}
	if len(ThrottleFunctionDigest()) != 64 || len(ThrottleLuaDigest()) != 64 || ThrottleLuaSHA1() == "" {
		t.Fatalf("invalid throttle digests: function=%q lua=%q sha1=%q", ThrottleFunctionDigest(), ThrottleLuaDigest(), ThrottleLuaSHA1())
	}
	if !strings.Contains(ThrottleFunctionSource(), "redis.register_function('"+ThrottleFunctionVersion+"'") || !strings.Contains(ThrottleLuaSource(), "ACTION == 'acquire'") {
		t.Fatal("throttle source does not expose versioned acquire function")
	}
	if strings.Contains(ThrottleFunctionSource(), "micro_usd") {
		t.Fatal("operational throttle function contains monetary accounting fields")
	}
}

func TestThrottleKeysAreOpaqueAndCoLocated(t *testing.T) {
	space, err := newKeySpace(testKeyOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{
		space.throttleKey("requests", "tenant/private"),
		space.throttleKey("tokens", "tenant/private"),
		space.throttleReservationKey("reservation/private"),
	} {
		if !strings.HasPrefix(key, "test:{admission}:") || strings.Contains(key, "tenant/private") || strings.Contains(key, "reservation/private") {
			t.Fatalf("unsafe throttle key: %q", key)
		}
	}
}

func TestThrottleAcquireIsAtomicIdempotentAndReleases(t *testing.T) {
	harness := newThrottleHarness()
	store, err := NewThrottleStore(ThrottleOptions{Invoker: harness, Reader: harness, Keys: testKeyOptions()})
	if err != nil {
		t.Fatal(err)
	}
	limits := []ThrottleLimit{
		{Kind: ThrottleRequests, Scope: "tenant-a", Amount: 2, Limit: 2, Window: time.Minute},
		{Kind: ThrottleTokens, Scope: "tenant-a", Amount: 4, Limit: 8, Window: time.Minute},
	}
	first, err := store.Acquire(context.Background(), "reservation-1", limits)
	if err != nil || first.Existing || first.Reservation.ID != "reservation-1" {
		t.Fatalf("first acquire = %#v, %v", first, err)
	}
	replay, err := store.Acquire(context.Background(), "reservation-1", limits)
	if err != nil || !replay.Existing || replay.Reservation.Digest != first.Reservation.Digest {
		t.Fatalf("replay acquire = %#v, %v", replay, err)
	}
	if _, err := store.Acquire(context.Background(), "reservation-1", append([]ThrottleLimit(nil), ThrottleLimit{Kind: ThrottleRequests, Scope: "tenant-a", Amount: 2, Limit: 2, Window: time.Minute})); !errors.Is(err, ErrThrottleConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
	if _, err := store.Acquire(context.Background(), "reservation-2", limits); !errors.Is(err, ErrThrottleDenied) {
		t.Fatalf("limit denial = %v", err)
	}
	if err := store.Release(context.Background(), first.Reservation); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(context.Background(), first.Reservation); err != nil {
		t.Fatalf("idempotent release = %v", err)
	}
	if _, err := store.Lookup(context.Background(), "reservation-1"); !errors.Is(err, ErrThrottleNotFound) {
		t.Fatalf("released lookup = %v", err)
	}
}

// The throttle names carry the Lua source digest, and the compose fixture
// loads this release's library, so an upgraded stack never keeps calling an
// older body under an unchanged name (#976).
func TestThrottleFunctionNamesCarryTheLuaSourceDigestAndAreProvisioned(t *testing.T) {
	tag := throttleSourceTag()
	if ThrottleFunctionLibrary != "llmtw_throttle_"+tag || ThrottleFunctionVersion != "throttle_"+tag {
		t.Fatalf("names = %q/%q, want the %s source tag", ThrottleFunctionLibrary, ThrottleFunctionVersion, tag)
	}
	compose, err := os.ReadFile("../../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if want := "load_function " + ThrottleFunctionLibrary + " " + ThrottleFunctionVersion + " "; !strings.Contains(string(compose), want) {
		t.Fatalf("compose.yaml does not provision %q", want)
	}
}
