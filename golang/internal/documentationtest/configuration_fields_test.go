package documentationtest

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestConfigurationReferenceNamesEveryField keeps docs/reference/configuration.md
// from drifting behind the configuration types: every yaml field name declared
// in golang/config must appear in it (#1531).
func TestConfigurationReferenceNamesEveryField(t *testing.T) {
	reference, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "reference", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	sources, err := filepath.Glob(filepath.Join("..", "..", "config", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	tag := regexp.MustCompile(`yaml:"([A-Za-z0-9_]+)`)
	fields := map[string]string{}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range tag.FindAllStringSubmatch(string(data), -1) {
			fields[match[1]] = filepath.Base(source)
		}
	}
	if len(fields) < 100 {
		t.Fatalf("found only %d configuration fields; the scan is broken", len(fields))
	}
	var missing []string
	for field, source := range fields {
		if !regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(field) + `([^A-Za-z0-9_]|$)`).Match(reference) {
			missing = append(missing, field+" ("+source+")")
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/reference/configuration.md does not mention configuration fields: %s", strings.Join(missing, ", "))
	}
}
