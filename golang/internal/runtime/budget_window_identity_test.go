package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

// localConfigWithBudgetWindows returns the shipped local configuration with
// its budget windows replaced, pointing at copies of the local catalogs.
func localConfigWithBudgetWindows(t *testing.T, windows string) []byte {
	t.Helper()
	directory := t.TempDir()
	data, err := os.ReadFile("../../deploy/local/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, name := range []string{"capabilities.yaml", "prices.yaml"} {
		catalogData, err := os.ReadFile(filepath.Join("../../deploy/local", name))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, catalogData, 0o600); err != nil {
			t.Fatal(err)
		}
		text = strings.ReplaceAll(text, "/etc/llmtw/"+name, path)
	}
	start, end := strings.Index(text, "\nbudgets:\n"), strings.Index(text, "\ncontinuation:\n")
	if start < 0 || end < start {
		t.Fatal("local configuration has no budgets section before continuation")
	}
	budgets := "\nbudgets:\n  require_match: true\n  policies:\n    - id: acme\n      match:\n        service_class: standard\n      windows:\n" + windows
	return []byte(text[:start] + budgets + text[end:])
}

// loadBudgetPolicies takes budget windows through the production path:
// strict configuration compile, then the catalog snapshot loader.
func loadBudgetPolicies(t *testing.T, windows string) []budget.Policy {
	t.Helper()
	compiled, err := config.Compile(context.Background(), localConfigWithBudgetWindows(t, windows), nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	loaded, err := (CatalogSnapshotLoader{Clock: func() time.Time { return now }}).Load(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.BudgetPolicies) != 1 {
		t.Fatalf("loaded budget policies = %#v", loaded.BudgetPolicies)
	}
	return loaded.BudgetPolicies
}

func budgetWindowYAML(id, duration, bucket, limit string) string {
	text := "        - duration: " + duration + "\n          bucket: " + bucket + "\n          limit_usd: \"" + limit + "\"\n"
	if id != "" {
		text = "        - id: " + id + "\n" + strings.Replace(text, "        - ", "          ", 1)
	}
	return text
}

func windowIdentity(t *testing.T, policies []budget.Policy, duration time.Duration) string {
	t.Helper()
	for _, window := range policies[0].Windows {
		if window.Duration == duration {
			return window.ID
		}
	}
	t.Fatalf("no %s window in %#v", duration, policies[0].Windows)
	return ""
}

func TestBudgetWindowIdentitySurvivesEditingOtherWindows(t *testing.T) {
	hour := budgetWindowYAML("", "1h", "1m", "10")
	day := budgetWindowYAML("", "24h", "5m", "100")
	week := budgetWindowYAML("", "7d", "1h", "1000")
	want := windowIdentity(t, loadBudgetPolicies(t, hour+day), 24*time.Hour)
	if want != "acme/24h-5m" {
		t.Errorf("derived 24h window identity = %q, want acme/24h-5m", want)
	}
	for name, windows := range map[string]string{
		"hourly window removed":  day,
		"windows reordered":      day + hour,
		"window inserted before": week + hour + day,
		"window appended":        hour + day + week,
	} {
		t.Run(name, func(t *testing.T) {
			if got := windowIdentity(t, loadBudgetPolicies(t, windows), 24*time.Hour); got != want {
				t.Fatalf("24h window identity = %q, want %q", got, want)
			}
		})
	}
}

func TestBudgetWindowExplicitIDSelectsIdentity(t *testing.T) {
	// Explicit ids "0" and "1" reproduce the former positional identities, so
	// an upgraded deployment can keep its existing accounting.
	policies := loadBudgetPolicies(t, budgetWindowYAML("\"0\"", "1h", "1m", "10")+budgetWindowYAML("\"1\"", "24h", "5m", "100"))
	if got := []string{policies[0].Windows[0].ID, policies[0].Windows[1].ID}; got[0] != "acme/0" || got[1] != "acme/1" {
		t.Fatalf("pinned window identities = %q", got)
	}
	policies = loadBudgetPolicies(t, budgetWindowYAML("daily", "24h", "5m", "100")+budgetWindowYAML("", "1h", "1m", "10"))
	if got := []string{policies[0].Windows[0].ID, policies[0].Windows[1].ID}; got[0] != "acme/daily" || got[1] != "acme/1h-1m" {
		t.Fatalf("explicit and derived window identities = %q", got)
	}
}

// TestBudgetWindowSpendSurvivesRemovingAnotherWindow spends under a policy
// with hourly and daily windows, removes the hourly window after its spend has
// aged out, and requires the daily window to still count the spend.
func TestBudgetWindowSpendSurvivesRemovingAnotherWindow(t *testing.T) {
	ctx := context.Background()
	hour := budgetWindowYAML("", "1h", "5m", "100")
	var reserved pricing.USD
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		b.source.value.BudgetPolicies = loadBudgetPolicies(t, hour+budgetWindowYAML("", "24h", "1h", "100"))
	})
	f.cap.Budgets = &admissionLeaser{BudgetLeaser: f.cap.Budgets}
	leaser := f.cap.Budgets.(*admissionLeaser)
	leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
		for _, reservation := range request.Reservations {
			if time.Duration(reservation.DurationNanos) == 24*time.Hour {
				reserved = reservation.AmountUSD
			}
		}
		return leaser.BudgetLeaser.Accept(ctx, request)
	}
	f.restart(t)
	f.finish(t)
	if reserved.IsZero() {
		t.Fatal("no reservation was taken against the daily window")
	}

	// The daily limit now equals one reservation: a second request fits only
	// if the first request's accounted spend has been forgotten.
	f.now = f.now.Add(2 * time.Hour)
	f.cap.Snapshot.(*planningSource).value.BudgetPolicies = loadBudgetPolicies(t, budgetWindowYAML("", "24h", "1h", reserved.String()))
	f.restart(t)
	f.request.OperationKey = "after-hourly-window-removed"
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionBudgetWait)
	if f.submits.Load() != 1 {
		t.Fatal("over-limit request was dispatched after the hourly window was removed")
	}
}

func TestReloadRejectsGeometryChangeBehindBudgetWindowIdentity(t *testing.T) {
	application, err := app.New(context.Background(), app.Options{
		InitialConfig: localConfigWithBudgetWindows(t, budgetWindowYAML("daily", "24h", "5m", "100")+budgetWindowYAML("", "1h", "1m", "10")),
		Builder:       app.SnapshotBuilder{},
		Clients: func(context.Context, *config.Snapshot) (app.ClientSet, error) {
			return app.ClientSetFunc(func(context.Context) error { return nil }), nil
		},
		ReplacementValidator: newRuntimeReplacementValidator(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Close(context.Background()) })
	before := application.Current()

	for name, windows := range map[string]string{
		"bucket":   budgetWindowYAML("daily", "24h", "1h", "100"),
		"duration": budgetWindowYAML("daily", "12h", "5m", "100"),
	} {
		err = application.Reload(context.Background(), localConfigWithBudgetWindows(t, windows))
		if !errors.Is(err, errBudgetWindowGeometryChanged) || !strings.Contains(err.Error(), "budgets.policies[0].windows[0]") {
			t.Fatalf("Reload() with changed %s = %v, want geometry rejection naming the window", name, err)
		}
		if application.Current() != before {
			t.Fatalf("rejected %s change replaced the active snapshot", name)
		}
	}

	// A new limit, a new position and a re-derived identity are all allowed.
	allowed := budgetWindowYAML("", "1h", "5m", "10") + budgetWindowYAML("daily", "24h", "5m", "250")
	if err := application.Reload(context.Background(), localConfigWithBudgetWindows(t, allowed)); err != nil {
		t.Fatalf("Reload() with stable identities = %v", err)
	}

	// Removing the window does not free its id for another geometry: the
	// accounting written under it may still be in Redis.
	if err := application.Reload(context.Background(), localConfigWithBudgetWindows(t, budgetWindowYAML("", "1h", "5m", "10"))); err != nil {
		t.Fatalf("Reload() removing the daily window = %v", err)
	}
	err = application.Reload(context.Background(), localConfigWithBudgetWindows(t, budgetWindowYAML("daily", "12h", "1h", "100")))
	if !errors.Is(err, errBudgetWindowGeometryChanged) {
		t.Fatalf("Reload() reintroducing a removed id with another geometry = %v, want geometry rejection", err)
	}
	if err := application.Reload(context.Background(), localConfigWithBudgetWindows(t, budgetWindowYAML("daily", "24h", "5m", "100"))); err != nil {
		t.Fatalf("Reload() reintroducing a removed id with its geometry = %v", err)
	}
}
