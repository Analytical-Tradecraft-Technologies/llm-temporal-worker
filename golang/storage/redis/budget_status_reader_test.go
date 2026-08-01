package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/control"
)

type budgetStatusGenerationFake struct {
	pointer  ActiveBudgetGeneration
	manifest BudgetManifest
}

func (fake budgetStatusGenerationFake) ActiveGeneration(context.Context) (ActiveBudgetGeneration, error) {
	return fake.pointer, nil
}
func (fake budgetStatusGenerationFake) LoadManifest(context.Context, ActiveBudgetGeneration) (BudgetManifest, error) {
	return fake.manifest, nil
}
func (fake budgetStatusGenerationFake) PublishGeneration(context.Context, BudgetManifest) (ActiveBudgetGeneration, error) {
	return ActiveBudgetGeneration{}, errors.New("not used")
}

type budgetStatusInvokerFake struct {
	result []any
	calls  int
	keys   [][]string
	args   [][]string
}

func (fake *budgetStatusInvokerFake) Run(_ context.Context, _ string, keys []string, args ...string) ([]any, error) {
	fake.calls++
	fake.keys = append(fake.keys, append([]string(nil), keys...))
	fake.args = append(fake.args, append([]string(nil), args...))
	return fake.result, nil
}

// budgetStatusPointerFenceInvokerFake models the small part of the Redis
// Function that cannot be exercised by the offline command seam: Redis runs
// the pointer check atomically with the member read. Keeping the fence in this
// deterministic harness makes each provenance mismatch a regression test
// rather than only a source/key-shape assertion.
type budgetStatusPointerFenceInvokerFake struct {
	active ActiveBudgetGeneration
	result []any
}

func (fake budgetStatusPointerFenceInvokerFake) Run(_ context.Context, _ string, _ []string, args ...string) ([]any, error) {
	if len(args) < 5 {
		return []any{"invalid_request", ""}, nil
	}
	want := ActiveBudgetGeneration{GenerationID: BudgetGenerationID(args[1]), IncarnationID: BudgetIncarnationID(args[2]), ManifestDigest: args[3]}
	if want != fake.active {
		return []any{"state_unavailable", ""}, nil
	}
	return fake.result, nil
}

func testBudgetStatusReader(t *testing.T, invoker FunctionInvoker) (*RedisBudgetStatusReader, BudgetManifest, time.Time) {
	t.Helper()
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	members := []BudgetManifestMember{{PolicyID: "policy", WindowID: "window", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), ConfigVersion: "config-1", PriceVersion: "price-1", CoverageStart: start, CoverageEnd: end, BucketCount: 60, BucketWidth: time.Minute, BucketCatalogDigest: strings.Repeat("c", 64), LimitNanoUSD: "1000000000"}}
	catalog, err := MemberCatalogDigest(members)
	if err != nil {
		t.Fatal(err)
	}
	manifest := BudgetManifest{Schema: BudgetManifestSchema, GenerationID: "generation-1", IncarnationID: "incarnation-1", ConfigVersion: "config-1", PriceVersion: "price-1", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), RebuildComplete: true, CoverageStart: start, CoverageEnd: end, PolicyCount: 1, WindowCount: 1, BucketCount: 60, StreamHighWaterMark: "1-0", RoundingVersion: BudgetRoundingVersion, MemberCatalogDigest: catalog, Members: members}
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewRedisBudgetStatusReader(BudgetStatusReaderOptions{Generation: budgetStatusGenerationFake{pointer: pointer, manifest: manifest}, Invoker: invoker, Keys: testBudgetKeySpaceForAdapter(t), Clock: func() time.Time { return start.Add(10 * time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	return reader, manifest, start.Add(10 * time.Minute)
}

func TestRedisBudgetStatusReaderUsesOneCoherentFunctionRead(t *testing.T) {
	invoker := &budgetStatusInvokerFake{}
	reader, manifest, activeAt := testBudgetStatusReader(t, invoker)
	invoker.result = budgetStatusResultForManifest(t, manifest, "1-4")
	policy := control.PolicyKey("policy")
	result, err := reader.ReadBudgetStatus(context.Background(), control.BudgetStatusQuery{PolicyKey: &policy}, activeAt)
	if err != nil {
		t.Fatal(err)
	}
	if invoker.calls != 1 || len(invoker.keys[0]) != 4 || len(invoker.args[0]) != 7 {
		t.Fatalf("function invocation = calls %d keys %d args %d", invoker.calls, len(invoker.keys[0]), len(invoker.args[0]))
	}
	keys := reader.keys
	if invoker.keys[0][0] != keys.ActiveGenerationKey() || invoker.keys[0][1] != keys.EventsKey() {
		t.Fatalf("function keys = %#v, want active pointer followed by stream", invoker.keys[0])
	}
	if result.GenerationID != control.BudgetGenerationID("generation-1") || result.StreamHighWaterMark != control.StreamHighWaterMark("1-4") || len(result.Windows) != 1 {
		t.Fatalf("result = %#v", result)
	}
	if result.Windows[0].AvailableUSD != control.DecimalUSD("0.700000000000000000") {
		t.Fatalf("available = %q", result.Windows[0].AvailableUSD)
	}
}

func TestRedisBudgetStatusReaderRejectsHistoryAndV1OrMalformedState(t *testing.T) {
	invoker := &budgetStatusInvokerFake{}
	reader, manifest, activeAt := testBudgetStatusReader(t, invoker)
	invoker.result = budgetStatusResultForManifest(t, manifest, "1-4")
	if _, err := reader.ReadBudgetStatus(context.Background(), control.BudgetStatusQuery{}, activeAt.Add(-2*time.Second)); !errors.Is(err, ErrBudgetHistoryNotAvailable) {
		t.Fatalf("history error = %v", err)
	}
	bad := budgetStatusResultForManifest(t, manifest, "1-4")
	bad[1] = strings.Replace(bad[1].(string), `"schema":"budget-window/v2"`, `"schema":"durable-budget/v1"`, 1)
	invoker.result = bad
	if _, err := reader.ReadBudgetStatus(context.Background(), control.BudgetStatusQuery{}, activeAt); !errors.Is(err, ErrBudgetStatusUnavailable) {
		t.Fatalf("v1 error = %v", err)
	}
	bad = budgetStatusResultForManifest(t, manifest, "1-4")
	bad[1] = strings.Replace(bad[1].(string), `"reserved_nano_usd":"200000000"`, `"reserved_nano_usd":"not-an-int"`, 1)
	invoker.result = bad
	if _, err := reader.ReadBudgetStatus(context.Background(), control.BudgetStatusQuery{}, activeAt); !errors.Is(err, ErrBudgetStatusUnavailable) {
		t.Fatalf("malformed error = %v", err)
	}
}

func TestRedisBudgetStatusReaderFailsClosedWhenActivePointerChanges(t *testing.T) {
	readerInvoker := &budgetStatusInvokerFake{}
	_, manifest, activeAt := testBudgetStatusReader(t, readerInvoker)
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	result := budgetStatusResultForManifest(t, manifest, "1-4")
	for _, test := range []struct {
		name   string
		mutate func(*ActiveBudgetGeneration)
	}{
		{name: "generation", mutate: func(value *ActiveBudgetGeneration) { value.GenerationID = "generation-2" }},
		{name: "incarnation", mutate: func(value *ActiveBudgetGeneration) { value.IncarnationID = "incarnation-2" }},
		{name: "manifest digest", mutate: func(value *ActiveBudgetGeneration) { value.ManifestDigest = strings.Repeat("f", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			active := pointer
			test.mutate(&active)
			invoker := &budgetStatusPointerFenceInvokerFake{active: active, result: result}
			reader, err := NewRedisBudgetStatusReader(BudgetStatusReaderOptions{
				Generation: budgetStatusGenerationFake{pointer: pointer, manifest: manifest},
				Invoker:    invoker,
				Keys:       testBudgetKeySpaceForAdapter(t),
				Clock:      func() time.Time { return activeAt },
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.ReadBudgetStatus(context.Background(), control.BudgetStatusQuery{}, activeAt)
			if !errors.Is(err, ErrBudgetStatusUnavailable) {
				t.Fatalf("error = %v, want fail-closed Redis status error", err)
			}
		})
	}
}

func TestBudgetStatusFunctionSourceIsBoundedAndVersioned(t *testing.T) {
	source := BudgetStatusFunctionLibrarySource()
	for _, forbidden := range []string{"HSCAN", "HGETALL", "durable-budget/v1"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("source contains forbidden %q", forbidden)
		}
	}
	for _, required := range []string{"GET", "cjson.decode", "HMGET", "ZRANGEBYSCORE", "XINFO", "TIME", BudgetStatusWindowSchema, BudgetStatusFunctionVersion} {
		if !strings.Contains(source, required) {
			t.Fatalf("source is missing %q", required)
		}
	}
}

func TestBudgetStatusFunctionIdentitySeparatesFencedKeyContract(t *testing.T) {
	if BudgetStatusFunctionLibrary == "llmtw_budget_status_v2" || BudgetStatusFunctionVersion == "budget_status_v2" {
		t.Fatalf("fenced budget status Function must not reuse the v2 identity: %q/%q", BudgetStatusFunctionLibrary, BudgetStatusFunctionVersion)
	}
	source := BudgetStatusFunctionLibrarySource()
	legacy := strings.Replace(source, "#!lua name="+BudgetStatusFunctionLibrary, "#!lua name=llmtw_budget_status_v2", 1)
	legacy = strings.Replace(legacy, "redis.register_function('"+BudgetStatusFunctionVersion+"'", "redis.register_function('budget_status_v2'", 1)
	legacyDigest := sha256.Sum256([]byte(legacy))
	if BudgetStatusFunctionDigest() == hex.EncodeToString(legacyDigest[:]) {
		t.Fatal("fenced Function digest must differ from the v2 key contract")
	}
}

func FuzzParseBudgetStatusNano(f *testing.F) {
	for _, seed := range []string{"0", "1", "1000000000", "9007199254740991", "-1", "01", "x", "9007199254740992"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		parsed, err := parseNano(value)
		if err == nil && (parsed < 0 || parsed > 9007199254740991) {
			t.Fatalf("unsafe parse %q = %d", value, parsed)
		}
	})
}

func budgetStatusResultForManifest(t *testing.T, manifest BudgetManifest, highWater string) []any {
	t.Helper()
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		t.Fatal(err)
	}
	read := BudgetStatusRead{GenerationID: string(manifest.GenerationID), IncarnationID: string(manifest.IncarnationID), ManifestDigest: digest, StreamHighWaterMark: highWater, Members: []BudgetStatusWindowRecord{{Schema: BudgetStatusWindowSchema, GenerationID: string(manifest.GenerationID), IncarnationID: string(manifest.IncarnationID), ManifestDigest: digest, MemberKey: manifest.Members[0].Key(), LimitNanoUSD: "1000000000", ReservedNanoUSD: "200000000", AccountedNanoUSD: "100000000", CoverageStart: manifest.Members[0].CoverageStart, CoverageEnd: manifest.Members[0].CoverageEnd}}}
	data, err := json.Marshal(read)
	if err != nil {
		t.Fatal(err)
	}
	return []any{"ok", string(data)}
}
