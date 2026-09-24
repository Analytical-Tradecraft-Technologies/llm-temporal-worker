package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type LogOptions struct {
	Format         string
	Level          string
	ContentLogging string
	Output         io.Writer
}

type Logger struct {
	logger *slog.Logger
}

var allowedLogAttrs = map[string]struct{}{
	"trace_id": {}, "span_id": {}, "temporal_workflow_id": {}, "temporal_run_id": {},
	"activity_id": {}, "operation_id": {}, "route_id": {}, "tenant_hash": {},
	"status": {}, "error_code": {}, "phase": {}, "config_version": {},
	"task_queue": {}, "endpoint": {}, "model": {}, "service_class": {},
	"outcome": {}, "duration_ms": {}, "command": {}, "dispatch": {},
	"retry": {}, "provider": {},
	"project_hash": {}, "operation_key_hash": {}, "query_kind": {}, "api_version": {},
	"request_fingerprint": {}, "response_digest": {}, "source": {},
	"actual_cost_usd": {}, "cost_status": {}, "cost_method": {}, "cost_unknown_reason": {},
	"started_at": {}, "completed_at": {},
}

var unsafeMessageWords = []string{
	"prompt", "output", "secret", "authorization", "bearer", "credential",
	"provider body", "provider_state", "tool", "transcript", "raw error",
}

func NewLogger(options LogOptions) (*Logger, error) {
	if options.Output == nil {
		options.Output = io.Discard
	}
	level := new(slog.LevelVar)
	switch strings.ToLower(options.Level) {
	case "", "info":
		level.Set(slog.LevelInfo)
	case "debug":
		level.Set(slog.LevelDebug)
	case "warn", "warning":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return nil, errors.New("unsupported log level")
	}
	var handler slog.Handler
	common := &slog.HandlerOptions{Level: level, AddSource: false}
	switch strings.ToLower(options.Format) {
	case "", "json":
		handler = slog.NewJSONHandler(options.Output, common)
	case "text":
		handler = slog.NewTextHandler(options.Output, common)
	default:
		return nil, errors.New("unsupported log format")
	}
	return &Logger{logger: slog.New(handler)}, nil
}

func (logger *Logger) Enabled(ctx context.Context, level slog.Level) bool {
	return logger != nil && logger.logger != nil && logger.logger.Enabled(ctx, level)
}

func (logger *Logger) Info(ctx context.Context, message string, attrs ...slog.Attr) {
	logger.log(ctx, slog.LevelInfo, message, attrs...)
}

func (logger *Logger) Warn(ctx context.Context, message string, attrs ...slog.Attr) {
	logger.log(ctx, slog.LevelWarn, message, attrs...)
}

func (logger *Logger) Error(ctx context.Context, message string, err error, attrs ...slog.Attr) {
	if logger == nil || logger.logger == nil {
		return
	}
	attrs = append(attrs, errorAttrs(err)...)
	logger.log(ctx, slog.LevelError, message, attrs...)
}

func (logger *Logger) log(ctx context.Context, level slog.Level, message string, attrs ...slog.Attr) {
	if logger == nil || logger.logger == nil {
		return
	}
	filtered := make([]slog.Attr, 0, len(attrs))
	for _, attr := range attrs {
		if safe, ok := safeAttr(attr); ok {
			filtered = append(filtered, safe)
		}
	}
	logger.logger.LogAttrs(ctx, level, safeMessage(message), filtered...)
}

func safeMessage(message string) string {
	if message == "" || len(message) > 160 {
		return "event"
	}
	lower := strings.ToLower(message)
	for _, word := range unsafeMessageWords {
		if strings.Contains(lower, word) {
			return "event"
		}
	}
	for _, runeValue := range message {
		if unicode.IsControl(runeValue) {
			return "event"
		}
	}
	return message
}

func safeAttr(attr slog.Attr) (slog.Attr, bool) {
	key := attr.Key
	if key == "tenant" {
		value := attr.Value.String()
		return slog.String("tenant_hash", hashIdentifier(value)), true
	}
	if _, ok := allowedLogAttrs[key]; !ok {
		return slog.Attr{}, false
	}
	if key == "provider" {
		// Provider names are configured identifiers, not raw SDK responses.
		value := attr.Value.String()
		if len(value) == 0 || len(value) > 96 {
			return slog.Attr{}, false
		}
	}
	if attr.Value.Kind() == slog.KindString {
		value := attr.Value.String()
		if len(value) > 160 || strings.ContainsAny(value, "\r\n") {
			return slog.Attr{}, false
		}
	}
	return attr, true
}

func errorAttrs(err error) []slog.Attr {
	if err == nil {
		return nil
	}
	var providerErr *provider.Error
	if errors.As(err, &providerErr) && providerErr != nil {
		return []slog.Attr{
			slog.String("error_code", string(providerErr.Code)),
			slog.String("phase", string(providerErr.Phase)),
			slog.String("dispatch", string(providerErr.Dispatch)),
			slog.String("retry", string(providerErr.Retry)),
		}
	}
	return []slog.Attr{slog.String("error_code", string(provider.CodeInternal))}
}

func hashIdentifier(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func (logger *Logger) Flush(context.Context) error { return nil }

// WithTime is a convenience for callers that want a bounded duration field
// without exposing arbitrary values in the attribute set.
func WithTime(duration time.Duration) slog.Attr {
	if duration < 0 {
		duration = 0
	}
	return slog.Int64("duration_ms", duration.Milliseconds())
}
