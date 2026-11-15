package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCanonicalFilterPointerCanonicalizesACopy(t *testing.T) {
	cursor := QueryCursor("next")
	original := &ProviderStatusQuery{Page: QueryPage{Size: 5, Cursor: &cursor}}
	canonical, ok := canonicalFilter(original).(*ProviderStatusQuery)
	if !ok || canonical == original {
		t.Fatalf("canonical filter = %#v, want a new *ProviderStatusQuery", canonical)
	}
	if canonical.Page.Cursor != nil || canonical.Page.Size != 5 {
		t.Fatalf("canonical page = %#v, want size 5 without cursor", canonical.Page)
	}
	if original.Page.Cursor != &cursor {
		t.Fatal("canonicalFilter mutated the caller's filter")
	}
	var typedNil *SpendSummaryQuery
	if got := canonicalFilter(typedNil); got != QueryFilter(typedNil) {
		t.Fatalf("canonical typed nil = %#v, want the same typed nil", got)
	}
}

func TestEncodeFilterPointerMatchesValue(t *testing.T) {
	value := CreditStatusQuery{RefreshIfOlderThan: time.Minute, Page: QueryPage{Size: 3}}
	fromValue, err := encodeFilter(value)
	if err != nil {
		t.Fatal(err)
	}
	fromPointer, err := encodeFilter(&value)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fromValue, fromPointer) {
		t.Fatalf("pointer encoding %s differs from value encoding %s", fromPointer, fromValue)
	}
	if _, err := encodeFilter((*CreditStatusQuery)(nil)); err == nil || err.Error() != "nil typed query filter" {
		t.Fatalf("typed nil error = %v", err)
	}
	testassert.ErrorContains(t, testassert.Err(encodeFilter(&ModelInventoryQuery{RefreshIfOlderThan: time.Millisecond})), "refresh_if_older_than")
	if _, err := encodeFilter(nil); err == nil || err.Error() != "unknown typed query filter" {
		t.Fatalf("nil filter error = %v", err)
	}
}

func TestValidateResultRows(t *testing.T) {
	key := func(row string) (string, error) {
		if row == "" {
			return "", errFixture
		}
		return row, nil
	}
	for _, tc := range []struct {
		name string
		kind llm.QueryKind
		rows []string
		want string
	}{
		{name: "sorted", kind: llm.QueryProviderStatus, rows: []string{"a", "b"}},
		{name: "kind mismatch", kind: llm.QuerySpendSummary, rows: []string{"a"}, want: `query result kind "spend_summary" does not match fixture rows`},
		{name: "unsorted", kind: llm.QueryProviderStatus, rows: []string{"b", "a"}, want: "unsorted"},
		{name: "duplicate", kind: llm.QueryProviderStatus, rows: []string{"a", "a"}, want: "unsorted"},
		{name: "unkeyed", kind: llm.QueryProviderStatus, rows: []string{"a", ""}, want: errFixture.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateResultRows(tc.kind, llm.QueryProviderStatus, "fixture rows", "unsorted", tc.rows, key)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

var errFixture = errors.New("row has no key")

// The page wrappers must keep encoding/json's case-insensitive key matching.
func TestDecodePageRowsKeepsJSONKeyMatching(t *testing.T) {
	row, err := json.Marshal(ProviderStatusRow{RouteID: "route-a", Provider: "p", Endpoint: "e", Availability: QueryAvailable})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"routes", "Routes", "ROUTES"} {
		raw := []byte(`{"` + key + `":[` + string(row) + `]}`)
		rows, err := decodePageRows[ProviderStatusRow, routesPage](raw, "route")
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if len(rows) != 1 || rows[0].RouteID != "route-a" {
			t.Fatalf("%s: rows = %#v", key, rows)
		}
	}
	if _, err := decodePageRows[ModelInventoryRow, modelsPage]([]byte(`{"models":[1]}`), "model"); err == nil || !strings.HasPrefix(err.Error(), "model 0: ") {
		t.Fatalf("row error = %v", err)
	}
}

func TestNormalizePageLimit(t *testing.T) {
	limit := 0
	if err := normalizePageLimit(&limit, 10, 20, "fixture"); err != nil || limit != 10 {
		t.Fatalf("default limit = %d, %v", limit, err)
	}
	limit = 21
	if err := normalizePageLimit(&limit, 10, 20, "fixture"); err == nil || err.Error() != "fixture page size must be between 1 and 20" {
		t.Fatalf("oversized limit error = %v", err)
	}
}

func TestStableSort(t *testing.T) {
	values := []SpendDimension{"provider", "model", "operation_kind", "model"}
	stableSort(values)
	want := []SpendDimension{"model", "model", "operation_kind", "provider"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("sorted = %v, want %v", values, want)
		}
	}
}
