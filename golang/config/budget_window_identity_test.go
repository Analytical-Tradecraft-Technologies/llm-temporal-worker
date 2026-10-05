package config

import (
	"strings"
	"testing"
	"time"
)

func budgetsJSONWithWindows(policyID string, windows ...string) []byte {
	return []byte(`{"require_match":true,"policies":[{"id":"` + policyID + `","match":{"tenant":"acme"},"windows":[` + strings.Join(windows, ",") + `]}]}`)
}

func TestBudgetWindowIdentityDerivesFromGeometryOrExplicitID(t *testing.T) {
	budgets, err := ParseBudgetsJSON(budgetsJSONWithWindows("acme",
		`{"duration":"1h","bucket":"1m","limit_usd":"10"}`,
		`{"duration":"36h","bucket":"90m","limit_usd":"100"}`,
		`{"duration":"90s","bucket":"1500ms","limit_usd":"1"}`,
		`{"id":"0","duration":"24h","bucket":"5m","limit_usd":"100"}`,
		`{"id":"monthly.v2_a-b","duration":"7d","bucket":"1h","limit_usd":"1000"}`))
	if err != nil {
		t.Fatal(err)
	}
	policy := budgets.Policies[0]
	want := []string{"acme/1h-1m", "acme/36h-1h30m", "acme/1m30s-1.5s", "acme/0", "acme/monthly.v2_a-b"}
	for index, window := range policy.Windows {
		if got := policy.WindowIdentity(window); got != want[index] {
			t.Errorf("windows[%d] identity = %q, want %q", index, got, want[index])
		}
	}
	if got := (BudgetWindow{Duration: Duration(1500 * time.Microsecond), Bucket: Duration(time.Microsecond)}).Key(); got != "1.5ms-1us" {
		t.Errorf("sub-millisecond geometry key = %q, want ASCII 1.5ms-1us", got)
	}
}

func TestBudgetWindowIdentityValidation(t *testing.T) {
	hour := `{"duration":"1h","bucket":"1m","limit_usd":"10"}`
	longID := strings.Repeat("w", 65)
	tests := []struct {
		name, policyID string
		windows        []string
		want           string
	}{
		{name: "same geometry without ids", windows: []string{hour, `{"duration":"60m","bucket":"60s","limit_usd":"20"}`}, want: "windows[1] has the same duration and bucket as windows[0]"},
		{name: "duplicate explicit id", windows: []string{`{"id":"a","duration":"1h","bucket":"1m","limit_usd":"10"}`, `{"id":"a","duration":"24h","bucket":"5m","limit_usd":"10"}`}, want: `windows[1] duplicate window identity "a"`},
		{name: "explicit id equals derived identity", windows: []string{hour, `{"id":"1h-1m","duration":"24h","bucket":"5m","limit_usd":"10"}`}, want: `windows[1] duplicate window identity "1h-1m"`},
		{name: "slash", windows: []string{`{"id":"a/b","duration":"1h","bucket":"1m","limit_usd":"10"}`}, want: "windows[0].id must match"},
		{name: "space", windows: []string{`{"id":"a b","duration":"1h","bucket":"1m","limit_usd":"10"}`}, want: "windows[0].id must match"},
		{name: "leading punctuation", windows: []string{`{"id":"-a","duration":"1h","bucket":"1m","limit_usd":"10"}`}, want: "windows[0].id must match"},
		{name: "brace", windows: []string{`{"id":"a{b}","duration":"1h","bucket":"1m","limit_usd":"10"}`}, want: "windows[0].id must match"},
		{name: "over-length id", windows: []string{`{"id":"` + longID + `","duration":"1h","bucket":"1m","limit_usd":"10"}`}, want: "windows[0].id must match"},
		{name: "over-length identity", policyID: strings.Repeat("p", 123), windows: []string{hour}, want: "must be at most 128 bytes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policyID := test.policyID
			if policyID == "" {
				policyID = "acme"
			}
			_, err := ParseBudgetsJSON(budgetsJSONWithWindows(policyID, test.windows...))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseBudgetsJSON() error = %v, want substring %q", err, test.want)
			}
		})
	}
	// Same geometry is fine once the windows are told apart, and the longest
	// identity that fits is accepted.
	if _, err := ParseBudgetsJSON(budgetsJSONWithWindows("acme", hour, `{"id":"second","duration":"1h","bucket":"1m","limit_usd":"20"}`)); err != nil {
		t.Fatalf("explicit id did not separate same-geometry windows: %v", err)
	}
	if _, err := ParseBudgetsJSON(budgetsJSONWithWindows(strings.Repeat("p", 122), hour)); err != nil {
		t.Fatalf("128-byte identity rejected: %v", err)
	}
}
