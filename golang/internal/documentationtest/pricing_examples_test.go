package documentationtest

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/catalog"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

var priceCatalogExample = regexp.MustCompile("(?s)```yaml\n(version: llmtw-prices/[^`]*?)```")

func TestDocumentedPriceCatalogExamplesAreUsable(t *testing.T) {
	root := repositoryRoot(t)
	for _, relative := range []string{"docs/reference/configuration.md", "docs/reference/catalog-loaders.md"} {
		data, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		matches := priceCatalogExample.FindAllStringSubmatch(string(data), -1)
		if len(matches) == 0 {
			t.Fatalf("%s has no price catalog example", relative)
		}
		for index, match := range matches {
			path := filepath.Join(t.TempDir(), "prices.yaml")
			if err := os.WriteFile(path, []byte(match[1]), 0o600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256([]byte(match[1]))
			loaded, err := catalog.LoadPricing(config.CatalogRef{File: path, SHA256: hex.EncodeToString(sum[:])})
			if err != nil {
				t.Fatalf("%s price example %d does not load: %v", relative, index, err)
			}
			for _, entry := range loaded.Catalog.Entries {
				for _, component := range []pricing.PriceComponent{pricing.PriceComponentInput, pricing.PriceComponentOutput, pricing.PriceComponentCacheWrite, pricing.PriceComponentPerRequest, pricing.PriceComponentReasoning} {
					if entry.ComponentUnknown(component) {
						t.Errorf("%s price example %d leaves %s unknown, so its route can never be selected", relative, index, component)
					}
				}
			}
		}
	}
}
