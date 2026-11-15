package runtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// decodePendingOutput splits pending-list or pending-recover output into its
// per-request reports, keyed by request ID, and its summary.
func decodePendingOutput(t *testing.T, output *bytes.Buffer) (map[string]pendingReport, pendingSummary) {
	t.Helper()
	reports := make(map[string]pendingReport)
	var summary pendingSummary
	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		line := scanner.Bytes()
		var kind struct{ Type string }
		if err := json.Unmarshal(line, &kind); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		switch kind.Type {
		case "request":
			var report pendingReport
			if err := json.Unmarshal(line, &report); err != nil {
				t.Fatal(err)
			}
			reports[report.RequestID] = report
		case "summary":
			if err := json.Unmarshal(line, &summary); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected line %q", line)
		}
	}
	if summary.Type != "summary" {
		t.Fatalf("output has no summary: %q", output.String())
	}
	return reports, summary
}

// forbidFixtureWrites fails the test on any storage mutation until the
// returned function restores the fixture.
func forbidFixtureWrites(t *testing.T, f *boundedCloudFixture) func() {
	t.Helper()
	f.table.hook = func(op string, item kv.KeyValueItem) (error, error) {
		t.Errorf("read-only pending tooling wrote %s %s", op, item.PartitionKey)
		return errors.New("write forbidden"), nil
	}
	f.blobs.hook = func(key blob.BlobKey) (error, error) {
		t.Errorf("read-only pending tooling created blob %s", key)
		return errors.New("write forbidden"), nil
	}
	return func() { f.table.hook, f.blobs.hook = nil, nil }
}

func (f *boundedCloudFixture) pendingRecovery(apply bool, staleAfter time.Duration) *pendingRecovery {
	recovery := &pendingRecovery{store: f.repository, clock: func() time.Time { return f.now }, staleAfter: staleAfter}
	if apply {
		recovery.runtime = f.runtime
	}
	return recovery
}

func (f *boundedCloudFixture) listPending(t *testing.T, staleAfter time.Duration) (map[string]pendingReport, pendingSummary) {
	t.Helper()
	var output bytes.Buffer
	restore := forbidFixtureWrites(t, f)
	defer restore()
	if err := listPending(context.Background(), f.repository, f.now, staleAfter, &output); err != nil {
		t.Fatal(err)
	}
	return decodePendingOutput(t, &output)
}

// budgetRecoveryOperations returns the operation IDs of every paid execution
// that budget recovery would restore, across every shard.
func (f *boundedCloudFixture) budgetRecoveryOperations(t *testing.T) map[string]bool {
	t.Helper()
	operations := make(map[string]bool)
	for shard := range cloudstate.PendingShards {
		for token := ""; ; {
			page, err := f.repository.ListBudgetRecovery(context.Background(), shard, 100, token, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			for _, execution := range page.Executions {
				operations[string(execution.Plan.Reservation.OperationID)] = true
			}
			if token = page.NextPageToken; token == "" {
				break
			}
		}
	}
	return operations
}

// A workflow terminated after a paid submission whose response was lost
// leaves its root and attempt pending. Listing reports both without writing;
// once the recovery window passes, recovery settles the attempt at its full
// reservation through the runtime's own settlement, never submits again, and
// keeps every row budget recovery reads.
func TestPendingRecoverySettlesALostSubmissionOfATerminatedWorkflow(t *testing.T) {
	f := boundedCloud(t, false, singleReservationLimits)
	ctx, v, _, scope, first := startUnknownOutcome(t, f)
	root := v.RequestID
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil {
		t.Fatal(err)
	}

	reports, summary := f.listPending(t, time.Hour)
	rootReport, attemptReport := reports[root], reports[string(first.ID)]
	if rootReport.Role != "root" || rootReport.Owner != "workflow" || rootReport.Disposition != pendingWaiting || rootReport.Action != pendingActionNone ||
		rootReport.NotBefore == nil || !rootReport.NotBefore.Equal(saved.Execution.RecoverAfter) || rootReport.Stale || rootReport.Orphan || !rootReport.Paid {
		t.Fatalf("root inside its recovery window = %+v", rootReport)
	}
	if attemptReport.Role != "attempt" || attemptReport.RootID != root || attemptReport.Owner != "workflow" || attemptReport.Disposition != pendingWaiting {
		t.Fatalf("active attempt inside its recovery window = %+v", attemptReport)
	}
	if summary.Scanned != 2 || summary.Orphans != 0 || summary.Actionable != 0 || summary.Unreadable != 0 {
		t.Fatalf("summary = %+v", summary)
	}

	// Nothing advanced the request for longer than the stale threshold.
	f.now = f.now.Add(16 * time.Minute)
	f.restart(t)
	reports, summary = f.listPending(t, 15*time.Minute)
	if report := reports[root]; report.Disposition != pendingRecoveryDue || report.Action != pendingActionPoll || !report.Stale || !report.Orphan {
		t.Fatalf("stale root past its recovery window = %+v", report)
	}
	if summary.Orphans != 2 || summary.Actionable != 2 {
		t.Fatalf("summary = %+v", summary)
	}

	// Without --apply recovery only plans.
	var output bytes.Buffer
	restore := forbidFixtureWrites(t, f)
	err = recoverPending(ctx, f.pendingRecovery(false, 15*time.Minute), []cloudstate.RequestID{first.ID}, false, &output)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	planned, summary := decodePendingOutput(t, &output)
	if report := planned[string(first.ID)]; report.Action != pendingActionPoll || report.Applied || summary.Recovered != 0 {
		t.Fatalf("dry run = %+v %+v", report, summary)
	}

	output.Reset()
	if err := recoverPending(ctx, f.pendingRecovery(true, 15*time.Minute), nil, true, &output); err != nil {
		t.Fatalf("recover: %v\n%s", err, output.String())
	}
	recovered, summary := decodePendingOutput(t, &output)
	if summary.Recovered != 1 || summary.Failed != 0 {
		t.Fatalf("stale recovery = %+v\n%s", summary, output.String())
	}
	for _, report := range recovered {
		if report.Disposition != pendingSettledUnknown || report.Action != pendingActionNone || !report.Settled {
			t.Fatalf("recovered request = %+v", report)
		}
	}
	settled := f.requireUnknownSettled(t, ctx, scope, first.ID, "recovery did not settle the lost submission")
	assertUnknownWorkCharged(t, settled)
	requireCount(t, &f.submits, 1, "recovery resubmitted paid work")

	// Recovery is idempotent and leaves nothing for another pass.
	output.Reset()
	if err := recoverPending(ctx, f.pendingRecovery(true, 15*time.Minute), []cloudstate.RequestID{cloudstate.RequestID(root), first.ID}, false, &output); err != nil {
		t.Fatal(err)
	}
	if _, summary := decodePendingOutput(t, &output); summary.Recovered != 0 || summary.Actionable != 0 {
		t.Fatalf("second recovery = %+v", summary)
	}
	if !f.budgetRecoveryOperations(t)[string(first.ID)] {
		t.Fatal("budget recovery lost the settled paid attempt")
	}
	requireCount(t, &f.submits, 1, "repeated recovery resubmitted paid work")
}

// A workflow terminated while its provider job was still running is
// recovered by the poll and completion steps it would have run: one poll,
// one settlement, a published result and no further submission. Recovery
// first requires the caller's scope to still be authorized.
func TestPendingRecoveryPollsAndCompletesAnAbandonedProviderJob(t *testing.T) {
	f := boundedCloud(t, true)
	ctx, v := f.generateStep(t, llm.ExecutionPending)
	root := cloudstate.RequestID(v.RequestID)
	f.now = f.now.Add(2 * time.Second)

	allow := f.options.ResolveScope
	f.options.ResolveScope = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("denied") }
	f.restart(t)
	var output bytes.Buffer
	if err := recoverPending(ctx, f.pendingRecovery(true, time.Second), []cloudstate.RequestID{root}, false, &output); !errors.Is(err, errPendingUnreadable) {
		t.Fatalf("unauthorized recovery = %v", err)
	}
	if reports, summary := decodePendingOutput(t, &output); summary.Failed != 1 || reports[string(root)].Error == "" || reports[string(root)].Applied {
		t.Fatalf("unauthorized recovery report = %+v %+v", reports, summary)
	}
	requireCount(t, &f.polls, 0, "unauthorized recovery polled")

	f.options.ResolveScope = allow
	f.restart(t)
	output.Reset()
	if err := recoverPending(ctx, f.pendingRecovery(true, time.Second), []cloudstate.RequestID{root}, false, &output); err != nil {
		t.Fatalf("recover: %v\n%s", err, output.String())
	}
	reports, summary := decodePendingOutput(t, &output)
	if report := reports[string(root)]; report.Disposition != pendingTerminal || report.Status != string(cloudstate.StatusCompleted) || !report.Applied || report.Steps != 2 || summary.Recovered != 1 {
		t.Fatalf("recovered job = %+v %+v", report, summary)
	}
	requireCount(t, &f.polls, 1, "recovery polls")
	requireCount(t, &f.submits, 1, "recovery submissions")
	if reports, summary := f.listPending(t, time.Second); len(reports) != 0 || summary.Scanned != 0 {
		t.Fatalf("completed request still pending: %+v", reports)
	}
	// A caller retrying the operation replays the recovered result.
	if result := f.finish(t); result.Generate == nil || result.RequestID != string(root) {
		t.Fatalf("replay after recovery = %+v", result)
	}
	requireCount(t, &f.submits, 1, "replay after recovery submitted")
}

// An attempt its root replaced without settling it has no workflow left to
// settle it. Recovery charges it at its full reservation through the
// runtime's unknown settlement, and leaves the root's unpaid replacement
// alone: recovery never starts new paid work.
func TestPendingRecoverySettlesASupersededUnknownAttempt(t *testing.T) {
	f := boundedCloud(t, false, singleReservationLimits)
	ctx, v, _, scope, first := startUnknownOutcome(t, f)
	f.now = f.now.Add(16 * time.Minute)
	saved, err := f.repository.LoadProviderExecution(ctx, scope, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The record still holds the unknown attempt unsettled: a worker
	// replaced it, then stopped before settlement.
	next, err := f.repository.BeginRequestAttempt(ctx, scope, cloudstate.RequestID(v.RequestID), first.ID, saved.Execution.RecoverAfter)
	if err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	reports, _ := f.listPending(t, time.Hour)
	if report := reports[string(first.ID)]; report.Owner != "none" || !report.Orphan || report.Disposition != pendingRecoveryDue || report.Action != pendingActionSettle {
		t.Fatalf("superseded attempt = %+v", report)
	}
	if report := reports[v.RequestID]; report.Disposition != pendingUnpaid || report.Action != pendingActionNone {
		t.Fatalf("root with an unpaid replacement = %+v", report)
	}
	if report := reports[string(next.ID)]; report.Disposition != pendingUnpaid || report.Owner != "workflow" {
		t.Fatalf("unpaid replacement = %+v", report)
	}

	var output bytes.Buffer
	if err := recoverPending(ctx, f.pendingRecovery(true, time.Hour), nil, true, &output); err != nil {
		t.Fatalf("recover: %v\n%s", err, output.String())
	}
	recovered, summary := decodePendingOutput(t, &output)
	if report := recovered[string(first.ID)]; report.Disposition != pendingSettledUnknown || !report.Applied || len(recovered) != 1 || summary.Recovered != 1 {
		t.Fatalf("superseded recovery = %+v %+v", recovered, summary)
	}
	assertUnknownWorkCharged(t, f.requireUnknownSettled(t, ctx, scope, first.ID, "superseded attempt not settled"))
	requireCount(t, &f.submits, 1, "recovery submitted the unpaid replacement")
	if _, err := f.repository.LoadProviderExecution(ctx, scope, next.ID); !errors.Is(err, cloudstate.ErrProviderExecutionMissing) && !errors.Is(err, cloudstate.ErrBudgetPlanMissing) {
		t.Fatalf("recovery started the replacement attempt: %v", err)
	}
}

func TestClassifyPendingDispositions(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	root := cloudstate.RequestID("llmtw_req_00000000-0000-4000-8000-000000000001")
	attempt := cloudstate.RequestID("llmtw_req_00000000-0000-4000-8000-000000000002")
	execution := func(stage cloudstate.ExecutionStage, change func(*cloudstate.PendingExecution)) *cloudstate.PendingExecution {
		value := &cloudstate.PendingExecution{AttemptID: attempt, Stage: stage, Paid: true, Claimed: true, RecoverAfter: now.Add(-time.Minute), PollAfter: now.Add(-time.Second)}
		if change != nil {
			change(value)
		}
		return value
	}
	active := func(value *cloudstate.PendingExecution) cloudstate.PendingInspection {
		return cloudstate.PendingInspection{ID: root, Role: cloudstate.PendingRoleRoot, Status: cloudstate.StatusRunning, UpdatedAt: now, ActiveAttemptID: attempt, Execution: value}
	}
	superseded := func(value *cloudstate.PendingExecution) cloudstate.PendingInspection {
		return cloudstate.PendingInspection{ID: attempt, Role: cloudstate.PendingRoleAttempt, RootID: root, Retired: true, Status: cloudstate.StatusOutcomeUnknown, UpdatedAt: now.Add(-48 * time.Hour), Execution: value}
	}
	for _, test := range []struct {
		name        string
		inspection  cloudstate.PendingInspection
		disposition string
		action      string
	}{
		{"initializing", cloudstate.PendingInspection{ID: root, Initializing: true, Status: cloudstate.StatusPending, UpdatedAt: now}, pendingInitializing, pendingActionNone},
		{"lagging terminal", cloudstate.PendingInspection{ID: root, Role: cloudstate.PendingRoleRoot, Status: cloudstate.StatusCompleted, UpdatedAt: now}, pendingTerminal, pendingActionNone},
		{"unprepared", cloudstate.PendingInspection{ID: root, Status: cloudstate.StatusPending, UpdatedAt: now}, pendingUnprepared, pendingActionNone},
		{"unpaid", active(nil), pendingUnpaid, pendingActionNone},
		{"poll waiting", active(execution(cloudstate.ExecutionPending, func(e *cloudstate.PendingExecution) { e.PollAfter = now.Add(time.Minute) })), pendingWaiting, pendingActionNone},
		{"poll due", active(execution(cloudstate.ExecutionPending, nil)), pendingPollDue, pendingActionPoll},
		{"submission waiting", active(execution(cloudstate.ExecutionSubmitting, func(e *cloudstate.PendingExecution) { e.RecoverAfter = now.Add(time.Minute) })), pendingWaiting, pendingActionNone},
		{"submission due", active(execution(cloudstate.ExecutionClaiming, nil)), pendingRecoveryDue, pendingActionPoll},
		{"unknown due", active(execution(cloudstate.ExecutionUnknown, nil)), pendingRecoveryDue, pendingActionPoll},
		{"unknown settlement due", active(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.SettlementSaved = true })), pendingSettlementDue, pendingActionPoll},
		{"unknown settled", active(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled = true, true })), pendingSettledUnknown, pendingActionNone},
		{"unknown unpaid", active(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.Paid = false })), pendingUnknownUnpaid, pendingActionNone},
		{"success settlement due", active(execution(cloudstate.ExecutionSucceeded, func(e *cloudstate.PendingExecution) { e.SettlementSaved = true })), pendingSettlementDue, pendingActionPoll},
		{"success completion due", active(execution(cloudstate.ExecutionSucceeded, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled = true, true })), pendingCompletionDue, pendingActionComplete},
		{"finalizing", func() cloudstate.PendingInspection { v := active(nil); v.Finalizing = true; return v }(), pendingCompletionDue, pendingActionComplete},
		{"failure closure due", active(execution(cloudstate.ExecutionFailed, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled = true, true })), pendingClosureDue, pendingActionPoll},
		{"failure retry", active(execution(cloudstate.ExecutionFailed, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled, e.Retryable = true, true, true })), pendingRetryRequired, pendingActionNone},
		{"superseded due", superseded(execution(cloudstate.ExecutionUnknown, nil)), pendingRecoveryDue, pendingActionSettle},
		{"superseded settlement due", superseded(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.SettlementSaved = true })), pendingSettlementDue, pendingActionSettle},
		{"superseded settled", superseded(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled = true, true })), pendingSettledUnknown, pendingActionNone},
		{"superseded waiting", superseded(execution(cloudstate.ExecutionUnknown, func(e *cloudstate.PendingExecution) { e.RecoverAfter = now.Add(time.Minute) })), pendingWaiting, pendingActionNone},
		{"superseded success", superseded(execution(cloudstate.ExecutionSucceeded, func(e *cloudstate.PendingExecution) { e.SettlementSaved, e.Settled = true, true })), pendingSuperseded, pendingActionNone},
		{"superseded without execution", superseded(nil), pendingSuperseded, pendingActionNone},
		// A root from before independent attempts is never polled: the
		// current steps would try to give it an attempt.
		{"legacy root execution", func() cloudstate.PendingInspection {
			v := active(execution(cloudstate.ExecutionPending, nil))
			v.ActiveAttemptID = ""
			return v
		}(), pendingSuperseded, pendingActionNone},
		{"legacy root unknown", func() cloudstate.PendingInspection {
			v := active(execution(cloudstate.ExecutionUnknown, nil))
			v.ActiveAttemptID = ""
			return v
		}(), pendingRecoveryDue, pendingActionSettle},
	} {
		t.Run(test.name, func(t *testing.T) {
			report := classifyPending(test.inspection, now, time.Hour)
			if report.Disposition != test.disposition || report.Action != test.action {
				t.Fatalf("disposition=%s action=%s, want %s %s", report.Disposition, report.Action, test.disposition, test.action)
			}
			if test.inspection.Role == cloudstate.PendingRoleAttempt && (report.Owner != "none" || !report.Orphan || !report.Stale) {
				t.Fatalf("superseded attempt ownership = %+v", report)
			}
			encoded, err := json.Marshal(report)
			if err != nil || strings.Contains(string(encoded), "tenant") || strings.Contains(string(encoded), "project") {
				t.Fatalf("report is not content-free: %s %v", encoded, err)
			}
		})
	}
}

type failingPendingStore struct {
	pendingRecoveryStore
	inspect error
}

func (store failingPendingStore) InspectPending(context.Context, cloudstate.RequestID) (cloudstate.PendingInspection, error) {
	return cloudstate.PendingInspection{}, store.inspect
}

// One unreadable record does not hide the rest; the walk reports it with a
// content-free code and fails at the end.
func TestPendingListReportsUnreadableRecordsAndFails(t *testing.T) {
	f := boundedCloud(t, false, singleReservationLimits)
	startUnknownOutcome(t, f)
	var output bytes.Buffer
	store := failingPendingStore{pendingRecoveryStore: f.repository, inspect: cloudstate.ErrCorrupt}
	if err := listPending(context.Background(), store, f.now, time.Hour, &output); !errors.Is(err, errPendingUnreadable) {
		t.Fatalf("unreadable listing = %v", err)
	}
	reports, summary := decodePendingOutput(t, &output)
	if summary.Scanned != 2 || summary.Unreadable != 2 || len(reports) != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	for _, report := range reports {
		if report.Disposition != pendingUnreadable || report.Error != "corrupt" {
			t.Fatalf("unreadable report = %+v", report)
		}
	}
	if code := pendingErrorCode(provider.NewError(provider.CodePermissionDenied, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever, "private detail")); code != string(provider.CodePermissionDenied) {
		t.Fatalf("provider error code = %q", code)
	}
}

func TestRunPendingRecoverRejectsAmbiguousTargets(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		ids   []string
		stale bool
	}{{nil, false}, {[]string{"llmtw_req_00000000-0000-4000-8000-000000000001"}, true}, {[]string{"private-id"}, false}} {
		if err := RunPendingRecover(ctx, nil, test.ids, test.stale, time.Hour, false, &bytes.Buffer{}); err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("targets %v stale=%t = %v", test.ids, test.stale, err)
		}
	}
	if err := RunPendingList(ctx, nil, 0, &bytes.Buffer{}); err == nil {
		t.Fatal("pending-list accepted a zero stale-after")
	}
}
