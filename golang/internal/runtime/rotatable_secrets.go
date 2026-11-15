package runtime

import (
	"context"
	"log/slog"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
)

// envSecretWarning is logged once per reloadable credential that a
// production snapshot reads from an environment variable (#1559). The record
// names the schema path only, never the variable name or its value. The
// logger replaces messages naming secrets or credentials, so the text avoids
// those words.
const envSecretWarning = "reloadable key or password is read from an environment variable; rotating it needs a restart, so use a file reference and SIGHUP instead"

// warnRotatableEnvSecrets logs envSecretWarning for every field that
// config.RotatableEnvSecretFields names. It runs at startup and after each
// published reload, so a reload that switches a reference to env is reported.
func warnRotatableEnvSecrets(ctx context.Context, logger *observability.Logger, value config.Config) {
	if logger == nil {
		return
	}
	for _, field := range value.RotatableEnvSecretFields() {
		logger.Warn(ctx, envSecretWarning, slog.String("config_field", field))
	}
}
