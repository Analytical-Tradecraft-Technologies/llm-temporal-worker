package buildinfo_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo"
)

func TestCurrentIncludesEveryImageMetadataField(t *testing.T) {
	metadata := buildinfo.Current()
	fields := map[string]string{
		"version":    metadata.Version,
		"revision":   metadata.Revision,
		"build_time": metadata.BuildTime,
		"go_version": metadata.GoVersion,
		"source":     metadata.Source,
	}
	for name, value := range fields {
		if value == "" {
			t.Errorf("metadata %s is empty", name)
		}
	}
}
