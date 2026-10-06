package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

type cliBudgetGeneration struct {
	pointer  redisstore.ActiveBudgetGeneration
	manifest redisstore.BudgetManifest
	missing  bool
	reads    int
}

func (fake *cliBudgetGeneration) ActiveGeneration(context.Context) (redisstore.ActiveBudgetGeneration, error) {
	fake.reads++
	if fake.missing {
		return redisstore.ActiveBudgetGeneration{}, fmt.Errorf("%w: %w", redisstore.ErrBudgetManifestInvalid, redisstore.ErrBudgetActiveGenerationMissing)
	}
	return fake.pointer, nil
}

func (fake *cliBudgetGeneration) LoadManifest(context.Context, redisstore.ActiveBudgetGeneration) (redisstore.BudgetManifest, error) {
	return fake.manifest, nil
}

func (fake *cliBudgetGeneration) PublishGeneration(context.Context, redisstore.BudgetManifest) (redisstore.ActiveBudgetGeneration, error) {
	return redisstore.ActiveBudgetGeneration{}, errors.New("not used")
}

type cliBudgetInvoker struct {
	result []any
	calls  int
}

func (fake *cliBudgetInvoker) Run(context.Context, string, []string, ...any) ([]any, error) {
	fake.calls++
	return fake.result, nil
}

type cliBudgetFunctions struct {
	libraries []redisclient.Library
	err       error
	calls     int
}

func (fake *cliBudgetFunctions) FunctionList(ctx context.Context, query redisclient.FunctionListQuery) *redisclient.FunctionListCmd {
	fake.calls++
	command := redisclient.NewFunctionListCmd(ctx)
	if query.LibraryNamePattern != redisstore.BudgetStatusFunctionLibrary || !query.WithCode {
		command.SetErr(errors.New("unexpected FUNCTION LIST query"))
		return command
	}
	command.SetVal(fake.libraries)
	command.SetErr(fake.err)
	return command
}

func provisionedBudgetLibrary() []redisclient.Library {
	return []redisclient.Library{{Name: redisstore.BudgetStatusFunctionLibrary, Engine: "LUA", Code: redisstore.BudgetStatusFunctionLibrarySource(), Functions: []redisclient.Function{{Name: redisstore.BudgetStatusFunctionVersion}}}}
}

type cliBudgetFixture struct {
	generation *cliBudgetGeneration
	invoker    *cliBudgetInvoker
	functions  *cliBudgetFunctions
	// masters, when set, replaces the single node with a cluster-shaped
	// fan-out over every master, as ForEachMaster does in production.
	masters []*cliBudgetFunctions
	service activity.QueryService
	now     time.Time
}

// newCLIBudgetFixture composes budget_status exactly as the production CLI
// does: the trusted-Temporal query builder over the CLI budget reader, which
// wraps the versioned Redis reader. Only the Redis commands are faked.
func newCLIBudgetFixture(t *testing.T) *cliBudgetFixture {
	t.Helper()
	start := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	now := start.Add(10 * time.Minute)
	members := []redisstore.BudgetManifestMember{
		{PolicyID: "policy-a", WindowID: "window", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), ConfigVersion: "config-1", PriceVersion: "price-1", CoverageStart: start, CoverageEnd: end, BucketCount: 60, BucketWidth: time.Minute, BucketCatalogDigest: strings.Repeat("c", 64), LimitNanoUSD: "1000000000"},
		{PolicyID: "policy-b", WindowID: "window", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), ConfigVersion: "config-1", PriceVersion: "price-1", CoverageStart: start, CoverageEnd: end, BucketCount: 60, BucketWidth: time.Minute, BucketCatalogDigest: strings.Repeat("c", 64), LimitNanoUSD: "2000000000"},
	}
	catalog, err := redisstore.MemberCatalogDigest(members)
	if err != nil {
		t.Fatal(err)
	}
	manifest := redisstore.BudgetManifest{Schema: redisstore.BudgetManifestSchema, GenerationID: "generation-1", IncarnationID: "incarnation-1", ConfigVersion: "config-1", PriceVersion: "price-1", PolicyHash: strings.Repeat("d", 64), WindowHash: strings.Repeat("e", 64), RebuildComplete: true, CoverageStart: start, CoverageEnd: end, PolicyCount: 2, WindowCount: 2, BucketCount: 120, StreamHighWaterMark: "1-0", RoundingVersion: redisstore.BudgetRoundingVersion, MemberCatalogDigest: catalog, Members: members}
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		t.Fatal(err)
	}
	read := redisstore.BudgetStatusRead{GenerationID: "generation-1", IncarnationID: "incarnation-1", ManifestDigest: digest, StreamHighWaterMark: "1-4"}
	for _, member := range members {
		read.Members = append(read.Members, redisstore.BudgetStatusWindowRecord{Schema: redisstore.BudgetStatusWindowSchema, GenerationID: "generation-1", IncarnationID: "incarnation-1", ManifestDigest: digest, MemberKey: member.Key(), LimitNanoUSD: member.LimitNanoUSD, ReservedNanoUSD: "200000000", AccountedNanoUSD: "100000000", CoverageStart: start, CoverageEnd: end})
	}
	payload, err := json.Marshal(read)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := redisstore.NewBudgetKeySpace(redisstore.KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: bytes.Repeat([]byte{8}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	f := &cliBudgetFixture{
		generation: &cliBudgetGeneration{pointer: pointer, manifest: manifest},
		invoker:    &cliBudgetInvoker{result: []any{"ok", string(payload)}},
		functions:  &cliBudgetFunctions{libraries: provisionedBudgetLibrary()},
		now:        now,
	}
	clock := func() time.Time { return f.now }
	nodes := func(ctx context.Context, fn func(context.Context, budgetFunctionLister) error) error {
		if f.masters == nil {
			return fn(ctx, f.functions)
		}
		for _, master := range f.masters {
			if err := fn(ctx, master); err != nil {
				return err
			}
		}
		return nil
	}
	reader, err := newCLIBudgetStatusReader(nodes, redisstore.BudgetStatusReaderOptions{Invoker: f.invoker, Generation: f.generation, Keys: keys, Mode: redisstore.AdmissionModeFunction, Clock: clock})
	if err != nil || reader == nil {
		t.Fatal("CLI budget reader was not composed", err)
	}
	value := trustedTemporalTestConfig(t)
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Clock: clock, Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
		return bytes.Repeat([]byte{7}, 32), nil
	})}}
	f.service, err = trustedTemporalQueryBuilder(factory, value)(context.Background(), trustedTemporalSnapshot(t, value), QueryRepositories{BudgetStatus: reader})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cliBudgetFixture) query(project, filter string) (llm.QueryResponseV1, error) {
	return f.service.Execute(context.Background(), llm.QueryRequestV1{
		APIVersion: llm.QueryAPIVersion, OperationKey: "budget-query",
		Context: llm.RequestContext{Tenant: "tenant", Project: project, Actor: "actor"},
		Kind:    llm.QueryBudgetStatus, Query: json.RawMessage(filter),
	})
}

func budgetResult(t *testing.T, response llm.QueryResponseV1) llm.BudgetStatus {
	t.Helper()
	switch value := response.Result.(type) {
	case llm.BudgetStatus:
		return value
	case *llm.BudgetStatus:
		return *value
	}
	t.Fatalf("result = %T", response.Result)
	return llm.BudgetStatus{}
}

func budgetWindow(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var window map[string]any
	if err := json.Unmarshal(raw, &window); err != nil {
		t.Fatal(err)
	}
	return window
}

func requireUnsupportedBudget(t *testing.T, err error) {
	t.Helper()
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeUnsupportedCapability || classified.Retry != provider.RetryNever {
		t.Fatalf("error = %v, want typed unsupported", err)
	}
}

func TestCLIBudgetStatusAllowedScopeReadsRedisGeneration(t *testing.T) {
	f := newCLIBudgetFixture(t)
	response, err := f.query("project", `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if response.Source != string(control.QuerySourceRedisBudget) || !response.Complete || response.NextCursor != nil {
		t.Fatalf("response = %+v", response)
	}
	result := budgetResult(t, response)
	if result.GenerationID != "generation-1" || result.StreamHighWaterMark != "1-4" || len(result.Windows) != 2 {
		t.Fatalf("result = %+v", result)
	}
	first, second := budgetWindow(t, result.Windows[0]), budgetWindow(t, result.Windows[1])
	if first["policy_key"] != "policy-a" || first["available_usd"] != "0.700000000000000000" || second["available_usd"] != "1.700000000000000000" {
		t.Fatalf("windows = %v, %v", first, second)
	}
	if f.functions.calls != 1 || f.invoker.calls != 1 {
		t.Fatalf("Function checks %d, reads %d", f.functions.calls, f.invoker.calls)
	}
	// policy_key narrows the coherent snapshot to one policy's windows.
	response, err = f.query("project", `{"policy_key":"policy-b"}`)
	if err != nil {
		t.Fatal(err)
	}
	if windows := budgetResult(t, response).Windows; len(windows) != 1 || budgetWindow(t, windows[0])["policy_key"] != "policy-b" {
		t.Fatalf("filtered windows = %s", windows)
	}
}

func TestCLIBudgetStatusDeniesOtherScopeBeforeRedis(t *testing.T) {
	f := newCLIBudgetFixture(t)
	for _, project := range []string{"not-allowed", "invoice-processing"} {
		// tenant/invoice-processing crosses the allowed acme/invoice-processing
		// and tenant/project pairs; only exact pairs are authorized.
		if _, err := f.query(project, `{}`); !errors.Is(err, control.ErrQueryAuthorization) {
			t.Fatalf("project %q = %v, want authorization denial", project, err)
		}
	}
	if f.functions.calls != 0 || f.generation.reads != 0 || f.invoker.calls != 0 {
		t.Fatal("denied query touched Redis")
	}
}

func TestCLIBudgetStatusIncludeWindowsFalse(t *testing.T) {
	f := newCLIBudgetFixture(t)
	response, err := f.query("project", `{"include_windows":false}`)
	if err != nil {
		t.Fatal(err)
	}
	result := budgetResult(t, response)
	if result.Windows == nil || len(result.Windows) != 0 || result.GenerationID != "generation-1" || result.ManifestDigest == "" {
		t.Fatalf("result = %+v, want provenance with an empty window array", result)
	}
}

func TestCLIBudgetStatusIsOneCompleteSnapshot(t *testing.T) {
	f := newCLIBudgetFixture(t)
	// budget_status is a bounded snapshot, not a keyset page: page members are
	// rejected before authorization or storage, and the answer never carries a
	// continuation.
	for _, filter := range []string{`{"page_size":1}`, `{"cursor":"opaque"}`} {
		if _, err := f.query("project", filter); err == nil || errors.Is(err, control.ErrQueryAuthorization) {
			t.Fatalf("filter %s = %v, want request validation failure", filter, err)
		}
	}
	if f.functions.calls != 0 || f.invoker.calls != 0 {
		t.Fatal("rejected page request touched Redis")
	}
	response, err := f.query("project", `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Complete || response.NextCursor != nil {
		t.Fatalf("budget snapshot complete=%v next=%v", response.Complete, response.NextCursor)
	}
}

func TestCLIBudgetStatusUnsupportedWithoutProvisionedState(t *testing.T) {
	for name, change := range map[string]func(*cliBudgetFixture){
		"absent library": func(f *cliBudgetFixture) { f.functions.libraries = nil },
		"different code": func(f *cliBudgetFixture) { f.functions.libraries[0].Code += "\n-- changed" },
		"missing function": func(f *cliBudgetFixture) {
			f.functions.libraries[0].Functions = []redisclient.Function{{Name: "budget_status_v2"}}
		},
		"no generation": func(f *cliBudgetFixture) { f.generation.missing = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newCLIBudgetFixture(t)
			change(f)
			_, err := f.query("project", `{}`)
			requireUnsupportedBudget(t, err)
			if f.invoker.calls != 0 {
				t.Fatal("unprovisioned state reached the budget Function")
			}
		})
	}
	// A Redis failure while checking is a retryable outage, not unsupported.
	f := newCLIBudgetFixture(t)
	f.functions.err = errors.New("connection refused")
	_, err := f.query("project", `{}`)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeStateUnavailable {
		t.Fatalf("error = %v, want state unavailable", err)
	}
}

// FUNCTION LIST is keyless, so in Redis Cluster it reaches an arbitrary
// master while FCALL reaches the slot owner. Every master must hold the
// library, or the check fails closed.
func TestCLIBudgetStatusClusterRequiresFunctionOnEveryMaster(t *testing.T) {
	provisioned := func() *cliBudgetFunctions { return &cliBudgetFunctions{libraries: provisionedBudgetLibrary()} }
	f := newCLIBudgetFixture(t)
	f.masters = []*cliBudgetFunctions{provisioned(), provisioned(), provisioned()}
	if _, err := f.query("project", `{}`); err != nil {
		t.Fatal(err)
	}
	for index, master := range f.masters {
		if master.calls != 1 {
			t.Fatalf("master %d checked %d times", index, master.calls)
		}
	}
	for _, missing := range []int{0, 2} {
		f := newCLIBudgetFixture(t)
		f.masters = []*cliBudgetFunctions{provisioned(), provisioned(), provisioned()}
		f.masters[missing].libraries = nil
		_, err := f.query("project", `{}`)
		requireUnsupportedBudget(t, err)
		if f.invoker.calls != 0 {
			t.Fatal("partially provisioned cluster reached the budget Function")
		}
	}
	// A topology with no masters proves nothing and must not pass.
	f = newCLIBudgetFixture(t)
	f.masters = []*cliBudgetFunctions{}
	_, err := f.query("project", `{}`)
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeStateUnavailable || f.invoker.calls != 0 {
		t.Fatalf("empty cluster = %v, want state unavailable without a read", err)
	}
}

func TestCLIBudgetStatusNodeFanOutMatchesClient(t *testing.T) {
	cluster := redisclient.NewClusterClient(&redisclient.ClusterOptions{Addrs: []string{"127.0.0.1:0"}})
	ring := redisclient.NewRing(&redisclient.RingOptions{Addrs: map[string]string{"a": "127.0.0.1:0"}})
	single := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _, _, _ = cluster.Close(), ring.Close(), single.Close() })
	for name, client := range map[string]redisclient.Scripter{"cluster": cluster, "ring": ring, "single": single} {
		if budgetFunctionNodesFor(client) == nil {
			t.Fatalf("%s client has no Function check", name)
		}
	}
}

func TestCLIBudgetStatusFactoryRequiresFunctionMode(t *testing.T) {
	factory := cliBudgetStatusReaderFactory()
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	generation := &cliBudgetGeneration{}
	keys, err := redisstore.NewBudgetKeySpace(redisstore.KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: bytes.Repeat([]byte{8}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := factory(context.Background(), nil, redisstore.BudgetStatusReaderOptions{Client: client, Generation: generation, Keys: keys, Mode: redisstore.AdmissionModeLua})
	if err != nil || reader != nil {
		t.Fatalf("Lua mode reader = %v, %v; want unsupported", reader, err)
	}
	reader, err = factory(context.Background(), nil, redisstore.BudgetStatusReaderOptions{Client: client, Generation: generation, Keys: keys, Mode: redisstore.AdmissionModeFunction})
	if err != nil || reader == nil {
		t.Fatalf("Function mode reader = %v, %v", reader, err)
	}
}

func TestPersistedQueryLogsAuthorizationDecisionsWithoutContent(t *testing.T) {
	value := trustedTemporalTestConfig(t)
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewPersistedQueryService(trustedTemporalSnapshot(t, value), QueryRepositories{}, PersistedQueryOptions{
		Authorize: func(_ context.Context, request control.Authorization) error {
			if request.Project != "project" {
				return errors.New("private-denial-reason")
			}
			return nil
		},
		Cursor: &control.CursorCodec{Key: bytes.Repeat([]byte{1}, 32)},
		Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"project", "private-project"} {
		_, _ = service.Execute(context.Background(), llm.QueryRequestV1{
			APIVersion: llm.QueryAPIVersion, OperationKey: "private-operation",
			Context: llm.RequestContext{Tenant: "private-tenant", Project: project, Actor: "private-actor"},
			Kind:    llm.QueryBudgetStatus, Query: json.RawMessage(`{"policy_key":"private-policy"}`),
		})
	}
	var decisions []string
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatal(err)
		}
		if entry["msg"] != "control query access decision" {
			continue
		}
		if entry["query_kind"] != "budget_status" || entry["tenant_hash"] == nil || entry["project_hash"] == nil {
			t.Fatalf("entry = %v", entry)
		}
		decisions = append(decisions, fmt.Sprint(entry["outcome"], "/", entry["level"]))
	}
	if strings.Join(decisions, ",") != "allowed/INFO,denied/WARN" {
		t.Fatalf("decisions = %v\n%s", decisions, output.String())
	}
	for _, private := range []string{"private-tenant", "private-project", "private-actor", "private-operation", "private-policy", "private-denial-reason"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("authorization log leaked %q", private)
		}
	}
}
