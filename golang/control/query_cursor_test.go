package control

import (
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestCursorCodecBindsScopeFilterAndHorizon(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key"), TTL: 10 * time.Minute}
	query := QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QuerySpendSummary, Filter: SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now, GroupBy: []SpendDimension{SpendByProvider, SpendByModel}, OperationKinds: []OperationKind{OperationGenerate}}}
	token, err := codec.Sign(query, "position-1", now.Add(-30*time.Second), now)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := codec.Decode(query, token, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if claims.Position != "position-1" || !claims.Horizon.Equal(now.Add(-30*time.Second)) {
		t.Fatalf("unexpected claims: %#v", claims)
	}

	changedScope := query
	changedScope.Scope.Project = "other-project"
	if _, err := codec.Decode(changedScope, token, now); err == nil {
		t.Fatal("scope mutation accepted")
	}
	changedFilter := query
	changedFilter.Filter = SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now, GroupBy: []SpendDimension{SpendByModel}, OperationKinds: []OperationKind{OperationGenerate}}
	if _, err := codec.Decode(changedFilter, token, now); err == nil {
		t.Fatal("filter mutation accepted")
	}
}

func TestCursorCodecIgnoresContinuationCursorInFilterDigest(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	first := QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{Page: QueryPage{Size: 20}}}
	cursor := QueryCursor("continuation")
	continuation := first
	continuation.Filter = ProviderStatusQuery{Page: QueryPage{Size: 20, Cursor: &cursor}}
	codec := CursorCodec{Key: []byte("key")}
	token, err := codec.Sign(first, "position", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(continuation, token, now); err != nil {
		t.Fatalf("continuation cursor changed binding: %v", err)
	}
}

func TestCursorCodecCanonicalizesUnorderedSpendDimensions(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key")}
	first := QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QuerySpendSummary, Filter: SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now, GroupBy: []SpendDimension{SpendByProvider, SpendByModel}, OperationKinds: []OperationKind{OperationGenerate, OperationCompact}}}
	second := first
	second.Filter = SpendSummaryQuery{StartTime: first.Filter.(SpendSummaryQuery).StartTime, EndTime: now, GroupBy: []SpendDimension{SpendByModel, SpendByProvider}, OperationKinds: []OperationKind{OperationCompact, OperationGenerate}}
	token, err := codec.Sign(first, "position", now.Add(-time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(second, token, now); err != nil {
		t.Fatalf("unordered dimensions should share binding: %v", err)
	}
}

func TestCursorCodecRejectsTamperExpiryFutureAndOversize(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key"), TTL: time.Minute, MaxPosition: 8}
	query := QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{}}
	if _, err := codec.Sign(query, strings.Repeat("x", 9), now, now); err == nil {
		t.Fatal("oversized position accepted")
	}
	token, err := codec.Sign(query, "position", now, now)
	if err != nil {
		t.Fatal(err)
	}
	tampered := QueryCursor(string(token)[:len(token)-1] + "A")
	if _, err := codec.Decode(query, tampered, now); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := codec.Decode(query, token, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired token accepted")
	}
	future, err := codec.Sign(query, "position", now, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(query, future, now); err == nil {
		t.Fatal("future token accepted")
	}
}

func TestCursorCodecSupportsEveryQueryKind(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	policy := PolicyKey("daily")
	queries := []QueryRequest{
		{OperationKey: "provider", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{}},
		{OperationKey: "inventory", Scope: testScope(), Kind: llm.QueryModelInventory, Filter: ModelInventoryQuery{}},
		{OperationKey: "credit", Scope: testScope(), Kind: llm.QueryCreditStatus, Filter: CreditStatusQuery{}},
		{OperationKey: "budget", Scope: testScope(), Kind: llm.QueryBudgetStatus, Filter: BudgetStatusQuery{PolicyKey: &policy, ActiveAt: &now}},
		{OperationKey: "spend", Scope: testScope(), Kind: llm.QuerySpendSummary, Filter: SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now}},
	}
	codec := CursorCodec{Key: []byte("all-kinds")}
	for _, query := range queries {
		token, err := codec.Sign(query, "position", now.Add(-time.Second), now)
		if err != nil {
			t.Fatalf("%s sign: %v", query.Kind, err)
		}
		if _, err := codec.Decode(query, token, now); err != nil {
			t.Fatalf("%s decode: %v", query.Kind, err)
		}
	}
}

func TestCursorCodecRejectsTypedNilFilter(t *testing.T) {
	var filter *ProviderStatusQuery
	query := QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: filter}
	codec := CursorCodec{Key: []byte("key")}
	now := time.Now().UTC()
	if _, err := codec.Sign(query, "position", now, now); err == nil {
		t.Fatal("typed nil filter was signed")
	}
	if _, err := codec.Decode(query, "bad", now); err == nil {
		t.Fatal("typed nil filter was decoded")
	}
}

func TestCursorCodecRejectsMalformedTypedRequest(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("request-validation")}
	tests := []struct {
		name  string
		query QueryRequest
	}{
		{
			name:  "empty tenant",
			query: QueryRequest{OperationKey: "op", Scope: QueryScope{Project: "project", Actor: "actor"}, Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{}},
		},
		{
			name:  "unsafe actor",
			query: QueryRequest{OperationKey: "op", Scope: QueryScope{Tenant: "tenant", Project: "project", Actor: "actor\nforged"}, Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{}},
		},
		{
			name:  "invalid page size",
			query: QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryProviderStatus, Filter: ProviderStatusQuery{Page: QueryPage{Size: 1001}}},
		},
		{
			name:  "filter kind mismatch",
			query: QueryRequest{OperationKey: "op", Scope: testScope(), Kind: llm.QueryModelInventory, Filter: ProviderStatusQuery{}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := codec.Sign(test.query, "position", now, now); err == nil {
				t.Fatal("malformed request was signed")
			}
			if _, err := codec.Decode(test.query, "invalid", now); err == nil {
				t.Fatal("malformed request was accepted by Decode")
			}
		})
	}
}
