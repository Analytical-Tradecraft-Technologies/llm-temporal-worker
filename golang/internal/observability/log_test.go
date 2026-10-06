package observability_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestLoggerDropsContentAndRawErrorsAndHashesTenant(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "json", Level: "debug", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	secret := "adversarial-secret-value"
	logger.Info(context.Background(), "provider request accepted", slog.String("tenant", secret), slog.String("prompt", secret), slog.String("unknown", secret))
	logger.Error(context.Background(), "provider failure", errors.New(secret), slog.String("operation_id", "op-1"))
	providerErr := provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "safe provider failure")
	logger.Error(context.Background(), "provider failure", providerErr)
	encoded := output.String()
	if strings.Contains(encoded, secret) || strings.Contains(encoded, `"prompt"`) || strings.Contains(encoded, `"unknown"`) {
		t.Fatalf("unsafe log field leaked: %s", encoded)
	}
	if !strings.Contains(encoded, `"tenant_hash"`) || !strings.Contains(encoded, `"error_code":"provider_unavailable"`) {
		t.Fatalf("safe fields missing: %s", encoded)
	}
}

func TestLoggerRejectsUnsafeMessage(t *testing.T) {
	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info(context.Background(), "prompt: hidden user content")
	if strings.Contains(output.String(), "hidden user content") {
		t.Fatalf("unsafe message leaked: %s", output.String())
	}
}

func TestLoggerConfigurationAndAttributeBoundaries(t *testing.T) {
	if _, err := observability.NewLogger(observability.LogOptions{Level: "trace"}); err == nil {
		t.Fatal("unsupported log level was accepted")
	}
	if _, err := observability.NewLogger(observability.LogOptions{Format: "xml"}); err == nil {
		t.Fatal("unsupported log format was accepted")
	}

	var output bytes.Buffer
	logger, err := observability.NewLogger(observability.LogOptions{Format: "text", Level: "warning", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	if logger.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("warning logger enabled info records")
	}
	if !logger.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warning logger disabled warning records")
	}
	logger.Info(context.Background(), "this info record is filtered")
	logger.Warn(context.Background(), "safe warning", slog.Int("duration_ms", 4), slog.String("provider", ""), slog.String("provider", strings.Repeat("x", 97)), slog.String("status", "line\nbreak"))
	logger.Error(context.Background(), "", nil, slog.String("operation_id", "op-1"))
	logger.Warn(context.Background(), strings.Repeat("x", 161))
	logger.Warn(context.Background(), "control\tmessage")
	if err := logger.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := observability.WithTime(-time.Second).Value.Int64(); got != 0 {
		t.Fatalf("negative duration attribute = %d, want 0", got)
	}
	if got := observability.WithTime(1500 * time.Millisecond).Value.Int64(); got != 1500 {
		t.Fatalf("positive duration attribute = %d, want 1500", got)
	}
	encoded := output.String()
	for _, forbidden := range []string{"line", "break", strings.Repeat("x", 161), "filtered"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("unsafe or filtered log text %q was emitted: %s", forbidden, encoded)
		}
	}
}
