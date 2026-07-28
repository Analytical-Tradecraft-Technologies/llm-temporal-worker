package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/maintenance"
	"github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

const testNow = "2026-07-29T00:00:00Z"

func validArgs() []string {
	return []string{
		"--config", "/etc/llmtw/config.yaml",
		"--now", testNow,
		"--limit", "100",
		"--cache-before", "2026-07-01T00:00:00Z",
		"--provider-status-before", "2026-07-01T00:00:00Z",
		"--inventory-before", "2026-07-01T00:00:00Z",
		"--query-executions-before", "2026-07-01T00:00:00Z",
		"--operations-before", "2026-07-01T00:00:00Z",
		"--budget-buckets-before", "2026-07-01T00:00:00Z",
		"--checkpoints-before", "2026-07-01T00:00:00Z",
		"--max-budget-window", "24h",
	}
}

func TestParseOptions(t *testing.T) {
	options, err := parseOptions(validArgs(), &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}
	if options.ConfigPath != "/etc/llmtw/config.yaml" || options.Limit != 100 || options.MaxWindow != 24*time.Hour {
		t.Fatalf("unexpected options: %#v", options)
	}
	if options.Now.Location() != time.UTC || options.Cache.Location() != time.UTC || options.Checkpoints.Location() != time.UTC {
		t.Fatalf("timestamps must retain UTC location: %#v", options)
	}
}

func TestParseOptionsRejectsUnsafeValues(t *testing.T) {
	tests := []struct {
		name string
		edit func([]string) []string
	}{
		{name: "missing cutoff", edit: func(args []string) []string { return dropFlag(args, "--cache-before") }},
		{name: "offset timestamp", edit: func(args []string) []string { return replaceFlag(args, "--now", "2026-07-29T00:00:00+00:00") }},
		{name: "zero limit", edit: func(args []string) []string { return replaceFlag(args, "--limit", "0") }},
		{name: "oversized limit", edit: func(args []string) []string { return replaceFlag(args, "--limit", "10001") }},
		{name: "zero budget window", edit: func(args []string) []string { return replaceFlag(args, "--max-budget-window", "0s") }},
		{name: "positional argument", edit: func(args []string) []string { return append(args, "unexpected") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseOptions(test.edit(validArgs()), &bytes.Buffer{}); err == nil {
				t.Fatal("parseOptions() unexpectedly accepted invalid input")
			}
		})
	}
}

func TestRequiredSecret(t *testing.T) {
	lookup := func(name string) (string, bool) {
		if name == "PRESENT" {
			return "value", true
		}
		if name == "BLANK" {
			return "  ", true
		}
		return "", false
	}
	if got, err := requiredSecret(lookup, "PRESENT"); err != nil || got != "value" {
		t.Fatalf("requiredSecret() = %q, %v", got, err)
	}
	for _, name := range []string{"MISSING", "BLANK"} {
		if _, err := requiredSecret(lookup, name); err == nil {
			t.Fatalf("requiredSecret(%q) unexpectedly succeeded", name)
		}
	}
}

func TestReadConfigRejectsOversizedInput(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	if _, err := file.WriteString(strings.Repeat("x", maxConfigBytes+1)); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := readConfig(file.Name()); err == nil {
		t.Fatal("readConfig() unexpectedly accepted oversized input")
	}
}

func TestEncodeResult(t *testing.T) {
	var output bytes.Buffer
	result := postgres.RetentionBatchResult{Passes: []postgres.RetentionBatchPass{
		{Name: "cache", Result: maintenance.RetentionResult{Examined: 4, Tombstoned: 2, Deleted: 1, Skipped: 1}},
		{Name: "provider_status", Err: errors.New("database unavailable")},
	}}
	if err := encodeResult(&output, result); err != nil {
		t.Fatalf("encodeResult() error = %v", err)
	}
	var decoded resultJSON
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("result is not JSON: %v", err)
	}
	if len(decoded.Passes) != 2 || decoded.Passes[0].Deleted != 1 || decoded.Passes[1].Error != "database unavailable" {
		t.Fatalf("unexpected encoded result: %#v", decoded)
	}
	if strings.Contains(output.String(), "password") {
		t.Fatal("encoded result contains a credential marker")
	}
}

func replaceFlag(args []string, flag, value string) []string {
	result := append([]string(nil), args...)
	for index := range result {
		if result[index] == flag && index+1 < len(result) {
			result[index+1] = value
			return result
		}
	}
	return result
}

func dropFlag(args []string, flag string) []string {
	result := make([]string, 0, len(args)-2)
	for index := 0; index < len(args); index++ {
		if args[index] == flag && index+1 < len(args) {
			index++
			continue
		}
		result = append(result, args[index])
	}
	return result
}
