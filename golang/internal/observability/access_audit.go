package observability

import (
	"context"
	"log/slog"
)

// RequestAccessPhase names the point of a Generate or Compact request that a
// RequestAccessRecord reports.
type RequestAccessPhase string

const (
	// RequestAccessPrepared is an allowed access: the caller's tenant and
	// project passed the scope policy and the request, with its parent
	// checkpoint, was prepared from durable state.
	RequestAccessPrepared RequestAccessPhase = "prepared"
	// RequestAccessCompleted is a terminal result (completed, or failed and
	// not retryable) returned to an authorized caller.
	RequestAccessCompleted RequestAccessPhase = "completed"
)

// RequestAccessRecord is the content-free access event of a durable Generate
// or Compact request (#1541). It holds identifiers and closed-vocabulary
// values only: no request or response body, transcript, provider payload,
// credential or reference capability. The log sink hashes every caller-
// supplied identifier and every checkpoint or operation identifier, since an
// operation ID or checkpoint handle can locate a response (#1362).
type RequestAccessRecord struct {
	Phase RequestAccessPhase
	// Kind is "generate" or "compact".
	Kind string
	// ActivityType is the registered Temporal Activity type, empty outside an
	// Activity.
	ActivityType string
	Tenant       string
	Project      string
	Actor        string
	OperationKey string
	// OperationID is the worker-generated request (operation) ID.
	OperationID string
	// ParentCheckpoint is the checkpoint handle the request continues or
	// compacts, empty for a Generate without a parent.
	ParentCheckpoint string
	// Checkpoint, RouteID, Status and ErrorCode describe a completed result:
	// the checkpoint it published, the route that served it when the response
	// reports one, the terminal execution state ("completed" or "failed") and
	// a failure's stable error code.
	Checkpoint string
	RouteID    string
	Status     string
	ErrorCode  string
}

// RequestAccessFunc observes a request access event on a best-effort basis,
// with the same contract as control.AuditFunc: it must return promptly and
// must not require durable storage, and the runtime ignores its error, so a
// sink failure never fails or retries a request.
type RequestAccessFunc func(context.Context, RequestAccessRecord) error

// RequestAccess logs a RequestAccessRecord at info. A prepared event uses the
// message of the matching scope denial, "request scope access decision", with
// outcome "allowed", so allowed and denied decisions share one message as the
// control query decisions do. A completed event is "request access completed".
// Tenant, project, actor, operation key, operation ID and checkpoint handles
// are logged only as truncated hashes; empty optional identifiers are omitted.
// Like QueryAudit this is an ordinary best-effort log, not a durable audit
// store, and it never returns an error.
func (logger *Logger) RequestAccess(ctx context.Context, record RequestAccessRecord) error {
	if !logger.Enabled(ctx, slog.LevelInfo) {
		return nil
	}
	message := "request scope access decision"
	if record.Phase == RequestAccessCompleted {
		message = "request access completed"
	}
	attrs := []slog.Attr{
		slog.String("outcome", "allowed"),
		slog.String("request_kind", record.Kind),
		slog.String("tenant_hash", hashIdentifier(record.Tenant)),
		slog.String("project_hash", hashIdentifier(record.Project)),
		slog.String("operation_key_hash", hashIdentifier(record.OperationKey)),
	}
	optional := func(key, value string, hash bool) {
		if value == "" {
			return
		}
		if hash {
			value = hashIdentifier(value)
		}
		attrs = append(attrs, slog.String(key, value))
	}
	optional("actor_hash", record.Actor, true)
	optional("operation_id_hash", record.OperationID, true)
	optional("parent_checkpoint_hash", record.ParentCheckpoint, true)
	optional("checkpoint_hash", record.Checkpoint, true)
	optional("activity_type", record.ActivityType, false)
	optional("route_id", record.RouteID, false)
	optional("status", record.Status, false)
	optional("error_code", record.ErrorCode, false)
	logger.log(ctx, slog.LevelInfo, message, attrs...)
	return nil
}
