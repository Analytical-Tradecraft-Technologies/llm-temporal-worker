package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/diagnostic"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/typednil"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// pendingRecoveryStore is the privileged, read-only discovery surface the
// pending tooling walks. Every write goes through the existing execution
// runtime, never through this interface.
type pendingRecoveryStore interface {
	ListPending(context.Context, int, int, string) (cloudstate.PendingPage, error)
	InspectPending(context.Context, cloudstate.RequestID) (cloudstate.PendingInspection, error)
	ReadForRecovery(context.Context, cloudstate.RequestID) (cloudstate.Record, error)
}

var _ pendingRecoveryStore = (*cloudstate.Repository)(nil)

// Dispositions are stable, content-free reason codes; the runbook documents
// each one.
const (
	pendingInitializing   = "initializing"    // Create never published; nothing exists or was paid.
	pendingUnprepared     = "unprepared"      // created but never prepared; nothing was paid.
	pendingTerminal       = "terminal"        // the discovery row lags a completed or failed record.
	pendingUnpaid         = "unpaid"          // no provider execution has started.
	pendingWaiting        = "waiting"         // recovery is not due before not_before.
	pendingPollDue        = "poll_due"        // a provider job can be polled.
	pendingRecoveryDue    = "recovery_due"    // a possibly dispatched attempt is past its recovery window.
	pendingSettlementDue  = "settlement_due"  // a saved settlement has not reached Redis.
	pendingClosureDue     = "closure_due"     // a settled failure has not closed its request.
	pendingCompletionDue  = "completion_due"  // a settled success has not been published.
	pendingRetryRequired  = "retry_required"  // a retryable failure needs a new paid attempt.
	pendingSettledUnknown = "settled_unknown" // charged at its full reservation; awaits exact-cost reconciliation.
	pendingUnknownUnpaid  = "unknown_unpaid"  // an unresolved attempt that reserved no budget.
	pendingSuperseded     = "superseded"      // an attempt its root no longer uses, with nothing to settle.
	pendingUnreadable     = "unreadable"      // the record could not be read; see error.
)

// Recovery actions. Each runs an existing runtime step; none acquires budget
// or submits a provider request.
const (
	pendingActionNone     = "none"
	pendingActionPoll     = "poll"     // the poll Activity step on the root.
	pendingActionComplete = "complete" // the completion Activity step on the root.
	pendingActionSettle   = "settle"   // settle a superseded attempt's accounting.
)

// pendingReport is one content-free line of pending-list or pending-recover
// output. It never includes scope, manifest, provider IDs or receipts.
type pendingReport struct {
	Type        string     `json:"type"`
	RequestID   string     `json:"request_id"`
	Shard       int        `json:"shard"`
	Role        string     `json:"role,omitempty"`
	RootID      string     `json:"root_id,omitempty"`
	Kind        string     `json:"kind,omitempty"`
	Status      string     `json:"status,omitempty"`
	Revision    uint64     `json:"revision"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	Stage       string     `json:"execution_stage,omitempty"`
	Paid        bool       `json:"paid,omitempty"`
	Settled     bool       `json:"settled,omitempty"`
	Owner       string     `json:"owner,omitempty"`
	Stale       bool       `json:"stale"`
	Orphan      bool       `json:"orphan"`
	Disposition string     `json:"disposition"`
	Action      string     `json:"action"`
	NotBefore   *time.Time `json:"not_before,omitempty"`
	Error       string     `json:"error,omitempty"`
	// Recovery results, for pending-recover only.
	Applied bool `json:"applied,omitempty"`
	Steps   int  `json:"steps,omitempty"`
}

type pendingSummary struct {
	Type       string `json:"type"`
	Scanned    int    `json:"scanned"`
	Orphans    int    `json:"orphans"`
	Actionable int    `json:"actionable"`
	Unreadable int    `json:"unreadable"`
	Recovered  int    `json:"recovered,omitempty"`
	Failed     int    `json:"failed,omitempty"`
}

var errPendingUnreadable = errors.New("some pending requests could not be read or recovered; see their error codes, then rerun")

// classifyPending decides what recovery may do for one inspected record. An
// owner of "workflow" means a request workflow may still advance it; "none"
// means nothing will. Staleness only suggests a terminated or timed-out
// workflow: records do not name their workflow, so confirm it in Temporal.
// Recovery is safe either way, because it runs the same idempotent steps.
func classifyPending(inspection cloudstate.PendingInspection, now time.Time, staleAfter time.Duration) pendingReport {
	report := pendingReport{Type: "request", RequestID: string(inspection.ID), Shard: inspection.Shard, Role: string(inspection.Role), RootID: string(inspection.RootID),
		Kind: inspection.Kind, Status: string(inspection.Status), Revision: inspection.Revision, Action: pendingActionNone}
	if !inspection.UpdatedAt.IsZero() {
		updated := inspection.UpdatedAt.UTC()
		report.UpdatedAt = &updated
		report.Stale = now.Sub(updated) >= staleAfter
	}
	if execution := inspection.Execution; execution != nil {
		report.Stage, report.Paid, report.Settled = string(execution.Stage), execution.Paid, execution.Settled
	}
	switch {
	case inspection.Initializing:
		report.Disposition = pendingInitializing
	case inspection.Terminal():
		report.Disposition = pendingTerminal
	case inspection.Role == "":
		report.Disposition, report.Owner = pendingUnprepared, "workflow"
	case inspection.Role == cloudstate.PendingRoleAttempt && inspection.ActiveAttemptID != inspection.ID:
		report.Owner = "none"
		classifySuperseded(&report, inspection.Execution, now)
	default:
		report.Owner = "workflow"
		if inspection.Role == cloudstate.PendingRoleRoot && inspection.Finalizing {
			report.Disposition, report.Action = pendingCompletionDue, pendingActionComplete
			break
		}
		if inspection.Execution == nil {
			report.Disposition = pendingUnpaid
			break
		}
		if inspection.Role == cloudstate.PendingRoleRoot && inspection.ActiveAttemptID == "" {
			// A root from before independent attempts holds its own
			// execution. The current steps advance only attempts, so only
			// its accounting may be finished.
			classifySuperseded(&report, inspection.Execution, now)
			break
		}
		classifyActive(&report, *inspection.Execution, now)
	}
	report.Orphan = report.Disposition != pendingTerminal && report.Disposition != pendingInitializing && (report.Owner == "none" || report.Stale)
	return report
}

func pendingWait(report *pendingReport, until time.Time) {
	report.Disposition = pendingWaiting
	until = until.UTC()
	report.NotBefore = &until
}

// classifyActive covers a root and its active attempt. Every action is a
// step the request workflow itself would run next, minus acquisition and
// submission: recovery never starts new paid work.
func classifyActive(report *pendingReport, execution cloudstate.PendingExecution, now time.Time) {
	report.Action = pendingActionPoll
	switch execution.Stage {
	case cloudstate.ExecutionPending:
		if now.Before(execution.PollAfter) {
			pendingWait(report, execution.PollAfter)
		} else {
			report.Disposition = pendingPollDue
		}
	case cloudstate.ExecutionClaiming, cloudstate.ExecutionSubmitting:
		if now.Before(execution.RecoverAfter) {
			pendingWait(report, execution.RecoverAfter)
		} else {
			report.Disposition = pendingRecoveryDue
		}
	case cloudstate.ExecutionUnknown:
		switch {
		case execution.SettlementSaved && !execution.Settled:
			report.Disposition = pendingSettlementDue
		case execution.Settled:
			report.Disposition = pendingSettledUnknown
		case !execution.Paid:
			report.Disposition = pendingUnknownUnpaid
		case now.Before(execution.RecoverAfter):
			pendingWait(report, execution.RecoverAfter)
		default:
			report.Disposition = pendingRecoveryDue
		}
	case cloudstate.ExecutionSucceeded:
		if !execution.Settled && execution.SettlementSaved {
			report.Disposition = pendingSettlementDue
		} else {
			report.Disposition, report.Action = pendingCompletionDue, pendingActionComplete
		}
	case cloudstate.ExecutionFailed:
		switch {
		case !execution.Settled && execution.SettlementSaved:
			report.Disposition = pendingSettlementDue
		case execution.Retryable:
			report.Disposition = pendingRetryRequired
		default:
			report.Disposition = pendingClosureDue
		}
	}
	if report.Disposition == pendingWaiting || report.Disposition == pendingSettledUnknown || report.Disposition == pendingUnknownUnpaid || report.Disposition == pendingRetryRequired {
		report.Action = pendingActionNone
	}
}

// classifySuperseded covers an attempt its root no longer uses: a retired
// unknown attempt, or one left behind by a completed root. No workflow will
// advance it, so only its accounting may be finished, and only with the
// conservative settlement the runtime already uses.
func classifySuperseded(report *pendingReport, execution *cloudstate.PendingExecution, now time.Time) {
	report.Disposition = pendingSuperseded
	if execution == nil {
		return
	}
	switch {
	case execution.SettlementSaved && !execution.Settled:
		report.Disposition, report.Action = pendingSettlementDue, pendingActionSettle
	case execution.Stage != cloudstate.ExecutionUnknown:
	case execution.Settled:
		report.Disposition = pendingSettledUnknown
	case !execution.Paid:
		report.Disposition = pendingUnknownUnpaid
	case now.Before(execution.RecoverAfter):
		pendingWait(report, execution.RecoverAfter)
	default:
		report.Disposition, report.Action = pendingRecoveryDue, pendingActionSettle
	}
}

// pendingErrorCode maps an error to a stable, content-free code.
func pendingErrorCode(err error) string {
	var classified *provider.Error
	switch {
	case errors.As(err, &classified):
		return string(classified.Code)
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, cloudstate.ErrCorrupt):
		return "corrupt"
	case errors.Is(err, contracts.ErrNotFound):
		return "not_found"
	case errors.Is(err, contracts.ErrConflict):
		return "conflict"
	default:
		return "unavailable"
	}
}

func unreadablePending(id cloudstate.RequestID, err error) pendingReport {
	shard, _ := cloudstate.PendingShard(id)
	return pendingReport{Type: "request", RequestID: string(id), Shard: shard, Disposition: pendingUnreadable, Action: pendingActionNone, Error: pendingErrorCode(err)}
}

// walkPending visits every discovery row of every shard, following every
// cursor including after empty pages. A failed page stops the walk; a record
// that cannot be inspected is reported and the walk continues.
func walkPending(ctx context.Context, store pendingRecoveryStore, visit func(cloudstate.RequestID, cloudstate.PendingInspection, error) error) error {
	for shard := range cloudstate.PendingShards {
		token := ""
		for {
			page, err := store.ListPending(ctx, shard, 100, token)
			if err != nil {
				return err
			}
			for _, pending := range page.Requests {
				inspection, err := store.InspectPending(ctx, pending.ID)
				if err := visit(pending.ID, inspection, err); err != nil {
					return err
				}
			}
			if token = page.NextPageToken; token == "" {
				break
			}
		}
	}
	return nil
}

// listPending writes one report per pending discovery row and a summary. It
// performs reads only.
func listPending(ctx context.Context, store pendingRecoveryStore, now time.Time, staleAfter time.Duration, output io.Writer) error {
	encoder := json.NewEncoder(output)
	summary := pendingSummary{Type: "summary"}
	err := walkPending(ctx, store, func(id cloudstate.RequestID, inspection cloudstate.PendingInspection, err error) error {
		summary.Scanned++
		report := unreadablePending(id, err)
		if err == nil {
			report = classifyPending(inspection, now, staleAfter)
		} else {
			summary.Unreadable++
		}
		countPending(&summary, report)
		return encoder.Encode(report)
	})
	if err != nil {
		return err
	}
	if err := encoder.Encode(summary); err != nil {
		return err
	}
	if summary.Unreadable > 0 {
		return errPendingUnreadable
	}
	return nil
}

func countPending(summary *pendingSummary, report pendingReport) {
	if report.Orphan {
		summary.Orphans++
	}
	if report.Action != pendingActionNone {
		summary.Actionable++
	}
}

// maxPendingRecoverySteps bounds the steps one recovery runs for a request:
// recovering a lost submission may take one step to record it as unknown, one
// to settle it and one more to finish its request.
const maxPendingRecoverySteps = 4

// pendingRecovery runs recovery for explicitly named or stale records. Without
// a runtime (no --apply) it only reports the planned action.
type pendingRecovery struct {
	store      pendingRecoveryStore
	runtime    *CloudExecutionRuntime
	clock      func() time.Time
	staleAfter time.Duration
	// recovered holds roots this run already recovered, so a root and its
	// active attempt are not recovered twice.
	recovered map[cloudstate.RequestID]bool
}

func (recovery *pendingRecovery) now() time.Time { return recovery.clock().UTC() }

func (recovery *pendingRecovery) inspect(ctx context.Context, id cloudstate.RequestID) (pendingReport, error) {
	inspection, err := recovery.store.InspectPending(ctx, id)
	if err != nil {
		return unreadablePending(id, err), err
	}
	return classifyPending(inspection, recovery.now(), recovery.staleAfter), nil
}

// recover plans, and with a runtime runs, the recovery of one record. An
// active attempt is recovered through its root, whose steps own it.
func (recovery *pendingRecovery) recover(ctx context.Context, id cloudstate.RequestID) (pendingReport, error) {
	report, err := recovery.inspect(ctx, id)
	if err != nil || report.Action == pendingActionNone || recovery.runtime == nil {
		return report, err
	}
	target := id
	if report.Owner == "workflow" && report.Role == string(cloudstate.PendingRoleAttempt) {
		target = cloudstate.RequestID(report.RootID)
	}
	if recovery.recovered[target] {
		// This run already recovered the root; report where that left it.
		return recovery.inspect(ctx, id)
	}
	recovery.recovered[target] = true
	if report.Action == pendingActionSettle {
		if err = recovery.settle(ctx, id); err == nil {
			report.Steps = 1
		}
	} else {
		report.Steps, err = recovery.advanceRoot(ctx, target)
	}
	report.Applied = report.Steps > 0
	if err != nil {
		report.Error = pendingErrorCode(err)
		return report, err
	}
	after, err := recovery.inspect(ctx, id)
	if err != nil {
		report.Error = after.Error
		return report, err
	}
	after.Applied, after.Steps = report.Applied, report.Steps
	return after, nil
}

// advanceRoot runs the poll and completion Activity steps on a root, exactly
// as its request workflow would, until recovery is no longer due or a step
// makes no progress. It never runs acquisition or submission.
func (recovery *pendingRecovery) advanceRoot(ctx context.Context, rootID cloudstate.RequestID) (int, error) {
	record, err := recovery.store.ReadForRecovery(ctx, rootID)
	if err != nil {
		return 0, err
	}
	reference, err := recovery.runtime.pendingReference(record)
	if err != nil {
		return 0, err
	}
	steps := 0
	before, err := recovery.store.InspectPending(ctx, rootID)
	for err == nil && steps < maxPendingRecoverySteps {
		switch classifyPending(before, recovery.now(), recovery.staleAfter).Action {
		case pendingActionPoll:
			_, err = recovery.runtime.PollExecutionV1(ctx, reference)
		case pendingActionComplete:
			_, err = recovery.runtime.CompleteExecutionV1(ctx, reference)
		default:
			return steps, nil
		}
		if err != nil {
			break
		}
		steps++
		var after cloudstate.PendingInspection
		after, err = recovery.store.InspectPending(ctx, rootID)
		if err == nil && samePendingProgress(before, after) {
			return steps, nil
		}
		before = after
	}
	return steps, err
}

func samePendingProgress(left, right cloudstate.PendingInspection) bool {
	if left.Revision != right.Revision || left.Status != right.Status || left.Finalizing != right.Finalizing || left.ActiveAttemptID != right.ActiveAttemptID || (left.Execution == nil) != (right.Execution == nil) {
		return false
	}
	return left.Execution == nil || *left.Execution == *right.Execution
}

// settle finishes a superseded attempt's accounting through the provider
// execution's own settlement: a saved settlement is applied, and an unknown
// attempt past its recovery window is charged at its full reservation. The
// caller's scope must still be authorized.
func (recovery *pendingRecovery) settle(ctx context.Context, id cloudstate.RequestID) error {
	record, err := recovery.store.ReadForRecovery(ctx, id)
	if err != nil {
		return err
	}
	caller, err := pendingCaller(record)
	if err != nil {
		return err
	}
	runtime := recovery.runtime
	if _, err := runtime.preparation.authorize(ctx, caller); err != nil {
		return err
	}
	scope := record.Request.Scope
	saved, err := runtime.store.LoadProviderExecution(ctx, scope, id)
	if err != nil {
		return err
	}
	if saved.Execution.Stage == cloudstate.ExecutionUnknown {
		_, err = runtime.execution.settleUnknown(ctx, scope, id, saved)
	} else {
		_, err = runtime.execution.settle(ctx, scope, id, saved)
	}
	return err
}

// pendingCaller returns the caller context saved in a request's manifest.
func pendingCaller(record cloudstate.Record) (llm.RequestContext, error) {
	var caller llm.RequestContext
	switch record.Request.Kind {
	case "generate":
		var request llm.GenerateRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil {
			return llm.RequestContext{}, cloudstate.ErrCorrupt
		}
		caller = request.Context
	case "compact":
		var request llm.CompactRequestV1
		if json.Unmarshal(record.Request.Manifest, &request) != nil {
			return llm.RequestContext{}, cloudstate.ErrCorrupt
		}
		caller = request.Context
	default:
		return llm.RequestContext{}, cloudstate.ErrCorrupt
	}
	if record.Request.Scope != (cloudstate.Scope{Tenant: caller.Tenant, Project: caller.Project}) {
		return llm.RequestContext{}, cloudstate.ErrCorrupt
	}
	return caller, nil
}

// pendingReference is the execution reference a request workflow would hold
// for root: the caller context saved in its manifest and a capability issued
// for it. Each step still authorizes that caller against the current policy.
func (r *CloudExecutionRuntime) pendingReference(root cloudstate.Record) (llm.ExecutionReferenceV1, error) {
	caller, err := pendingCaller(root)
	if err != nil {
		return llm.ExecutionReferenceV1{}, err
	}
	if r == nil || r.preparation == nil || r.preparation.references == nil {
		return llm.ExecutionReferenceV1{}, executionError(provider.CodeConfiguration)
	}
	capability, err := r.preparation.references.IssueReferenceCapability(root.Request.Scope.Tenant, root.Request.Scope.Project, string(root.Request.ID))
	if err != nil {
		return llm.ExecutionReferenceV1{}, executionError(provider.CodeConfiguration)
	}
	return llm.ExecutionReferenceV1{RequestID: string(root.Request.ID), Context: caller, Capability: capability}, nil
}

// recoverPending recovers each named ID, or with stale every stale orphan
// whose recovery is due, and writes one report per record and a summary.
func recoverPending(ctx context.Context, recovery *pendingRecovery, ids []cloudstate.RequestID, stale bool, output io.Writer) error {
	encoder := json.NewEncoder(output)
	summary := pendingSummary{Type: "summary"}
	if recovery.recovered == nil {
		recovery.recovered = make(map[cloudstate.RequestID]bool)
	}
	handle := func(id cloudstate.RequestID) error {
		summary.Scanned++
		report, err := recovery.recover(ctx, id)
		switch {
		case report.Disposition == pendingUnreadable:
			summary.Unreadable++
		case err != nil:
			summary.Failed++
		case report.Applied:
			summary.Recovered++
		}
		countPending(&summary, report)
		return encoder.Encode(report)
	}
	var err error
	if stale {
		err = walkPending(ctx, recovery.store, func(id cloudstate.RequestID, inspection cloudstate.PendingInspection, inspectErr error) error {
			if inspectErr != nil {
				summary.Scanned++
				summary.Unreadable++
				return encoder.Encode(unreadablePending(id, inspectErr))
			}
			report := classifyPending(inspection, recovery.now(), recovery.staleAfter)
			if !report.Orphan || report.Action == pendingActionNone {
				return nil
			}
			return handle(id)
		})
	} else {
		for _, id := range ids {
			if err = handle(id); err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}
	if err := encoder.Encode(summary); err != nil {
		return err
	}
	if summary.Unreadable > 0 || summary.Failed > 0 {
		return errPendingUnreadable
	}
	return nil
}

// ParsePendingRequestID validates an operator-supplied internal request ID.
func ParsePendingRequestID(value string) (cloudstate.RequestID, bool) {
	id := cloudstate.RequestID(value)
	_, err := cloudstate.PendingShard(id)
	return id, err == nil
}

// pendingConfig compiles a durable configuration whose authorization policy
// the worker would accept.
func pendingConfig(ctx context.Context, data []byte, resolver config.ReferenceResolver) (*config.Snapshot, error) {
	snapshot, err := config.Compile(ctx, data, resolver)
	if err != nil {
		return nil, errors.New("pending recovery configuration is invalid")
	}
	value := snapshot.Config()
	if value.State.Kind != config.StateKindDurable || value.State.Requests == nil {
		return nil, errors.New("pending recovery requires durable cloud storage")
	}
	if _, err := trustedTemporalCloudOptions(value); err != nil {
		return nil, diagnostic.Safe("pending recovery authorization configuration is invalid", err)
	}
	return snapshot, nil
}

// openPendingStore opens cloud request storage only: no Redis, provider,
// Temporal or budget client.
func openPendingStore(ctx context.Context, snapshot *config.Snapshot) (pendingRecoveryStore, error) {
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: secrets.New(secrets.Options{})}}
	repository, err := factory.buildCloudRequests(ctx, snapshot.Config().State.Requests)
	if err != nil {
		return nil, errors.New("pending recovery storage could not be opened")
	}
	store, ok := repository.(pendingRecoveryStore)
	if !ok || typednil.Is(store) {
		return nil, errors.New("pending recovery storage is unsupported")
	}
	return store, nil
}

// RunPendingList reports every pending request and what recovery may do for
// it. It opens cloud request storage only and performs reads only.
func RunPendingList(ctx context.Context, data []byte, staleAfter time.Duration, output io.Writer) error {
	if ctx == nil || output == nil || staleAfter <= 0 {
		return errors.New("pending-list requires a positive stale-after")
	}
	snapshot, err := pendingConfig(ctx, data, nil)
	if err != nil {
		return err
	}
	store, err := openPendingStore(ctx, snapshot)
	if err != nil {
		return err
	}
	return listPending(ctx, store, time.Now().UTC(), staleAfter, output)
}

// RunPendingRecover recovers explicitly named requests, or with stale every
// stale orphan whose recovery is due. Without apply it opens storage only and
// reports the planned action. With apply it composes the same cloud runtime
// as the worker, without a Temporal worker, and runs only the existing poll,
// completion and settlement steps: it never acquires budget, submits a
// provider request, refunds a charge or deletes a record.
func RunPendingRecover(ctx context.Context, data []byte, ids []string, stale bool, staleAfter time.Duration, apply bool, output io.Writer) error {
	if ctx == nil || output == nil || staleAfter <= 0 || stale == (len(ids) > 0) {
		return errors.New("pending-recover requires request IDs or --stale, not both")
	}
	parsed := make([]cloudstate.RequestID, 0, len(ids))
	for _, value := range ids {
		id, ok := ParsePendingRequestID(value)
		if !ok {
			return errors.New("pending-recover request IDs must be internal request IDs")
		}
		parsed = append(parsed, id)
	}
	recovery := &pendingRecovery{clock: time.Now, staleAfter: staleAfter}
	if !apply {
		snapshot, err := pendingConfig(ctx, data, nil)
		if err != nil {
			return err
		}
		if recovery.store, err = openPendingStore(ctx, snapshot); err != nil {
			return err
		}
		return recoverPending(ctx, recovery, parsed, stale, output)
	}
	resolver := secrets.New(secrets.Options{})
	snapshot, err := pendingConfig(ctx, data, newCLIReferenceResolver(resolver))
	if err != nil {
		return err
	}
	factory, err := newCLIEngineFactory(ProductionFactoryOptions{Resolver: resolver, SnapshotLoader: CatalogSnapshotLoader{}})
	if err != nil {
		return errors.New("pending recovery runtime could not be composed")
	}
	_, clients, err := factory.Build(ctx, snapshot)
	if err != nil {
		if errors.Is(err, ErrBudgetInitializationRequired) {
			return err
		}
		return errors.New("pending recovery runtime could not be composed; check storage, Redis and provider configuration")
	}
	defer clients.Close(context.WithoutCancel(ctx))
	source, sourced := clients.(V1RuntimeSource)
	capabilities, capable := clients.(V1RuntimeCapabilitiesSource)
	if !sourced || !capable {
		return errors.New("pending recovery runtime is unsupported")
	}
	cloud, ok := source.V1Runtime().(*cloudV1Runtime)
	store, stored := capabilities.V1RuntimeCapabilities().Requests.(pendingRecoveryStore)
	if !ok || cloud == nil || cloud.CloudExecutionRuntime == nil || !stored || typednil.Is(store) {
		return errors.New("pending recovery runtime is unsupported")
	}
	recovery.store, recovery.runtime = store, cloud.CloudExecutionRuntime
	return recoverPending(ctx, recovery, parsed, stale, output)
}
