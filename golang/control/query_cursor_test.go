package control

import (
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// mustSignCursor signs request with codec and fails the test on error.
func mustSignCursor(t *testing.T, codec *CursorCodec, request QueryRequest, position string, horizon, issuedAt time.Time) QueryCursor {
	t.Helper()
	token, err := codec.Sign(request, position, horizon, issuedAt)
	testassert.NoError(t, err, "CursorCodec.Sign() error")
	return token
}

func TestCursorCodecBindsScopeFilterAndHorizon(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key"), TTL: 10 * time.Minute}
	query := scopedQuery(llm.QuerySpendSummary, SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now, GroupBy: []SpendDimension{SpendByProvider, SpendByModel}, OperationKinds: []OperationKind{OperationGenerate}})
	token := mustSignCursor(t, &codec, query, "position-1", now.Add(-30*time.Second), now)
	claims, err := codec.Decode(query, token, now.Add(time.Minute))
	testassert.NoError(t, err)
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
	first := scopedQuery(llm.QueryProviderStatus, ProviderStatusQuery{Page: QueryPage{Size: 20}})
	cursor := QueryCursor("continuation")
	continuation := first
	continuation.Filter = ProviderStatusQuery{Page: QueryPage{Size: 20, Cursor: &cursor}}
	codec := CursorCodec{Key: []byte("key")}
	token := mustSignCursor(t, &codec, first, "position", now, now)
	testassert.NoError(t, testassert.Err(codec.Decode(continuation, token, now)), "continuation cursor changed binding")
}

func TestCursorCodecCanonicalizesUnorderedSpendDimensions(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key")}
	first := scopedQuery(llm.QuerySpendSummary, SpendSummaryQuery{StartTime: now.Add(-time.Hour), EndTime: now, GroupBy: []SpendDimension{SpendByProvider, SpendByModel}, OperationKinds: []OperationKind{OperationGenerate, OperationCompact}})
	second := first
	second.Filter = SpendSummaryQuery{StartTime: first.Filter.(SpendSummaryQuery).StartTime, EndTime: now, GroupBy: []SpendDimension{SpendByModel, SpendByProvider}, OperationKinds: []OperationKind{OperationCompact, OperationGenerate}}
	token := mustSignCursor(t, &codec, first, "position", now.Add(-time.Minute), now)
	testassert.NoError(t, testassert.Err(codec.Decode(second, token, now)), "unordered dimensions should share binding")
}

func TestCursorCodecRejectsTamperExpiryFutureAndOversize(t *testing.T) {
	now := time.Date(2026, time.July, 21, 2, 0, 0, 0, time.UTC)
	codec := CursorCodec{Key: []byte("test-key"), TTL: time.Minute, MaxPosition: 8}
	query := scopedQuery(llm.QueryProviderStatus, ProviderStatusQuery{})
	if _, err := codec.Sign(query, strings.Repeat("x", 9), now, now); err == nil {
		t.Fatal("oversized position accepted")
	}
	token := mustSignCursor(t, &codec, query, "position", now, now)
	tampered := QueryCursor(string(token)[:len(token)-1] + "A")
	if _, err := codec.Decode(query, tampered, now); err == nil {
		t.Fatal("tampered token accepted")
	}
	if _, err := codec.Decode(query, token, now.Add(2*time.Minute)); err == nil {
		t.Fatal("expired token accepted")
	}
	future := mustSignCursor(t, &codec, query, "position", now, now.Add(3*time.Minute))
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
	query := scopedQuery(llm.QueryProviderStatus, filter)
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
			query: scopedQuery(llm.QueryProviderStatus, ProviderStatusQuery{Page: QueryPage{Size: 1001}}),
		},
		{
			name:  "filter kind mismatch",
			query: scopedQuery(llm.QueryModelInventory, ProviderStatusQuery{}),
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
