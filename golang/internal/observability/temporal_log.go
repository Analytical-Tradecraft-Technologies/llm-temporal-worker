package observability

import (
	"context"
	"log/slog"

	temporallog "go.temporal.io/sdk/log"
)

// TemporalLogger routes SDK client and worker events through the worker's slog
// logger, retaining its level, destination, format, and content filtering.
func (logger *Logger) TemporalLogger() temporallog.Logger {
	return &temporalLogger{logger: logger}
}

type temporalLogger struct {
	logger *Logger
	attrs  []slog.Attr
}

var _ temporallog.WithLogger = (*temporalLogger)(nil)

func (logger *temporalLogger) Debug(message string, keyvals ...any) {
	logger.log(slog.LevelDebug, message, keyvals)
}

func (logger *temporalLogger) Info(message string, keyvals ...any) {
	logger.log(slog.LevelInfo, message, keyvals)
}

func (logger *temporalLogger) Warn(message string, keyvals ...any) {
	logger.log(slog.LevelWarn, message, keyvals)
}

func (logger *temporalLogger) Error(message string, keyvals ...any) {
	logger.log(slog.LevelError, message, keyvals)
}

func (logger *temporalLogger) With(keyvals ...any) temporallog.Logger {
	attrs := append([]slog.Attr(nil), logger.attrs...)
	attrs = append(attrs, temporalLogAttrs(keyvals)...)
	return &temporalLogger{logger: logger.logger, attrs: attrs}
}

func (logger *temporalLogger) log(level slog.Level, message string, keyvals []any) {
	ctx := context.Background()
	if !logger.logger.Enabled(ctx, level) {
		return
	}
	attrs := append([]slog.Attr(nil), logger.attrs...)
	attrs = append(attrs, temporalLogAttrs(keyvals)...)
	logger.logger.log(ctx, level, message, attrs...)
}

// SDK attributes are untrusted: only known scalar identifiers and classified
// errors enter the existing logger. Do not stringify arbitrary SDK values;
// they can contain payloads, credentials, or raw transport/provider errors.
func temporalLogAttrs(keyvals []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(keyvals)/2)
	for index := 0; index+1 < len(keyvals); index += 2 {
		key, ok := keyvals[index].(string)
		if !ok {
			continue
		}
		value := keyvals[index+1]
		if key == "Error" || key == "error" {
			if err, ok := value.(error); ok {
				attrs = append(attrs, errorAttrs(err)...)
			}
			continue
		}
		switch key {
		case "WorkflowID", "WorkflowId":
			key = "temporal_workflow_id"
		case "RunID", "RunId":
			key = "temporal_run_id"
		case "ActivityID", "ActivityId":
			key = "activity_id"
		case "TaskQueue":
			key = "task_queue"
		default:
			continue
		}
		if text, ok := value.(string); ok {
			if attr, ok := safeAttr(slog.String(key, text)); ok {
				attrs = append(attrs, attr)
			}
		}
	}
	return attrs
}
