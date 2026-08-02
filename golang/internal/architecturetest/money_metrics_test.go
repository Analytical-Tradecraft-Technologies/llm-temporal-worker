package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetricsDoNotAcceptLegacyMoneyAmounts(t *testing.T) {
	metricsSource := readMetricsArchitectureSource(t, filepath.Join("internal", "observability", "metrics.go"))
	engineSource := readMetricsArchitectureSource(t, filepath.Join("engine", "metrics.go"))

	for _, forbidden := range []string{
		"SetBudgetReserved(",
		"RecordCost(",
		"microUSD float64",
		"llmtw_budget_reserved_micro_usd",
		"llmtw_cost_micro_usd_total",
		"float64(materialized)",
	} {
		for path, source := range map[string]string{
			"internal/observability/metrics.go": metricsSource,
			"engine/metrics.go":                 engineSource,
		} {
			if strings.Contains(source, forbidden) {
				t.Errorf("%s contains forbidden legacy monetary metrics construct %q", path, forbidden)
			}
		}
	}
	for _, required := range []string{"RecordExactCost(", "RecordCostStatus("} {
		if !strings.Contains(engineSource, required) {
			t.Errorf("engine/metrics.go does not record bounded exact-cost signal %q", required)
		}
	}
}

func readMetricsArchitectureSource(t *testing.T, relativePath string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(moduleRoot(t), relativePath))
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	return string(contents)
}
