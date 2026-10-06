package observability

import (
	"context"
	"encoding/hex"
	"log/slog"

	"github.com/mfow/llm-temporal-worker/golang/control"
)

// QueryAudit logs a content-free summary of a completed control query. Request
// and response bodies, raw scope identifiers, and operation keys are excluded.
// Normal log levels and destinations apply. Logging failures do not affect the
// query result; this is not a durable ledger or an idempotency store.
func (logger *Logger) QueryAudit(ctx context.Context, record control.QueryAuditRecord) error {
	if !logger.Enabled(ctx, slog.LevelInfo) {
		return nil
	}
	attrs := []slog.Attr{
		slog.String("tenant_hash", hashIdentifier(record.Tenant)),
		slog.String("project_hash", hashIdentifier(record.Project)),
		slog.String("operation_key_hash", hashIdentifier(record.OperationKey)),
		slog.String("query_kind", string(record.Kind)),
		slog.String("api_version", record.APIVersion),
		slog.String("request_fingerprint", hex.EncodeToString(record.RequestFingerprint[:])),
		slog.String("response_digest", hex.EncodeToString(record.ResponseDigest[:])),
		slog.String("source", record.Source),
		slog.String("cost_status", record.CostStatus),
		slog.String("cost_method", record.CostMethod),
		slog.String("cost_unknown_reason", record.CostUnknownReasonCode),
		slog.Time("started_at", record.StartedAt.UTC()),
		slog.Time("completed_at", record.CompletedAt.UTC()),
		WithTime(record.CompletedAt.Sub(record.StartedAt)),
	}
	if record.ActualCostUSD != nil {
		attrs = append(attrs, slog.String("actual_cost_usd", *record.ActualCostUSD))
	}
	logger.Info(ctx, "control query completed", attrs...)
	return nil
}

// QueryAuthorization logs one control-query authorization decision, which the
// query service makes before any storage read. Only the query kind, the
// outcome ("allowed" or "denied") and truncated hashes of the tenant and
// project are logged; the actor, query body, cursor and the authorizer's error
// text are never included. A denial logs at warn so it survives a typical
// production level, while an allow logs at info. Like QueryAudit this is an
// ordinary best-effort log, not a durable audit store.
func (logger *Logger) QueryAuthorization(ctx context.Context, request control.Authorization, allowed bool) {
	level, outcome := slog.LevelInfo, "allowed"
	if !allowed {
		level, outcome = slog.LevelWarn, "denied"
	}
	if !logger.Enabled(ctx, level) {
		return
	}
	// The message avoids the word "authorization", which the safe-message
	// filter would replace with a generic "event".
	logger.log(ctx, level, "control query access decision",
		slog.String("outcome", outcome),
		slog.String("query_kind", string(request.Kind)),
		slog.String("tenant_hash", hashIdentifier(request.Tenant)),
		slog.String("project_hash", hashIdentifier(request.Project)),
	)
}
