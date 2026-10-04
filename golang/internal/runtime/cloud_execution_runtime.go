package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type cloudExecutionStore interface {
	CloudRequestPreparationStore
	cloudAdmissionStore
	CloudProviderExecutionStore
	BeginRequestAttempt(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestID, time.Time) (cloudstate.RequestAttempt, error)
	LoadRequestAttempt(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.RequestAttempt, error)
	FinishRequestFailure(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestID, llm.ExecutionResultV1, time.Time) error
	CacheFingerprint(cache.Input) (cache.Fingerprint, error)
}

// CloudExecutionOptions supplies deployment-owned authorization and signing.
// No default resolver trusts the tenant/project fields of an incoming payload.
type CloudExecutionOptions struct {
	ResolveScope     CheckpointScopeResolver
	Keyring          *state.Keyring
	Limits           state.MaterializeLimits
	CheckpointTTL    time.Duration
	BudgetGeneration durable.GenerationID
}

// CloudExecutionRuntime implements the bounded activity state machine. All
// durable access follows authorization, including terminal result replay. A
// budget activity tries once and a poll activity performs at most one read of
// the provider. Neither sleeps. Instances own no mutable per-request state.
type CloudExecutionRuntime struct {
	capabilities V1RuntimeCapabilities
	store        cloudExecutionStore
	preparation  *CloudRequestPreparation
	execution    *CloudProviderExecution
	publication  *CheckpointPublication
	options      CloudExecutionOptions
}

var _ activity.ExecutionRuntime = (*CloudExecutionRuntime)(nil)

func (c V1RuntimeCapabilities) NewCloudExecutionRuntime(ctx context.Context, options CloudExecutionOptions) (*CloudExecutionRuntime, error) {
	store, ok := c.Requests.(cloudExecutionStore)
	if !ok || isNilCapability(store) || c.Finalizer == nil || isNilCapability(c.Responses) || isNilCapability(c.ResponseFills) || options.CheckpointTTL <= 0 || options.BudgetGeneration.Validate() != nil {
		return nil, executionError(provider.CodeConfiguration)
	}
	preparation, err := c.NewCloudRequestPreparation(options.ResolveScope, options.Limits)
	if err != nil {
		return nil, err
	}
	execution, err := c.NewCloudProviderExecution(ctx)
	if err != nil {
		return nil, err
	}
	publication, err := c.NewCheckpointPublication(options.Keyring, options.Limits)
	if err != nil {
		return nil, err
	}
	return &CloudExecutionRuntime{capabilities: c, store: store, preparation: preparation, execution: execution, publication: publication, options: options}, nil
}

type cloudStep int

const (
	cloudPrepare cloudStep = iota
	cloudAcquire
	cloudSubmit
	cloudPoll
	cloudComplete
)

func (r *CloudExecutionRuntime) PrepareExecutionV1(ctx context.Context, input llm.PrepareExecutionV1) (llm.ExecutionResultV1, error) {
	return r.prepareStep(ctx, input, cloudPrepare)
}
func (r *CloudExecutionRuntime) GenerateStepV1(ctx context.Context, input llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	return r.prepareStep(ctx, llm.PrepareExecutionV1{Generate: &input}, cloudSubmit)
}
func (r *CloudExecutionRuntime) CompactStepV1(ctx context.Context, input llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	return r.prepareStep(ctx, llm.PrepareExecutionV1{Compact: &input}, cloudSubmit)
}
func (r *CloudExecutionRuntime) AcquireBudgetV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return r.referenceStep(ctx, ref, cloudAcquire)
}
func (r *CloudExecutionRuntime) PollExecutionV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return r.referenceStep(ctx, ref, cloudPoll)
}
func (r *CloudExecutionRuntime) CompleteExecutionV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return r.referenceStep(ctx, ref, cloudComplete)
}
func (r *CloudExecutionRuntime) prepareStep(ctx context.Context, input llm.PrepareExecutionV1, step cloudStep) (llm.ExecutionResultV1, error) {
	if r == nil {
		return llm.ExecutionResultV1{}, executionError(provider.CodeConfiguration)
	}
	prepared, err := r.preparation.Prepare(ctx, input)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	return r.advance(ctx, prepared, step)
}
func (r *CloudExecutionRuntime) referenceStep(ctx context.Context, ref llm.ExecutionReferenceV1, step cloudStep) (llm.ExecutionResultV1, error) {
	if r == nil {
		return llm.ExecutionResultV1{}, executionError(provider.CodeConfiguration)
	}
	prepared, err := r.preparation.Load(ctx, ref)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	return r.advance(ctx, prepared, step)
}

func (r *CloudExecutionRuntime) advance(ctx context.Context, p PreparedCloudRequest, step cloudStep) (llm.ExecutionResultV1, error) {
	for tries := 0; tries < 16; tries++ {
		result, err := r.advanceAttempt(ctx, p, step)
		if err != nil {
			completed, recoveryErr := r.preparation.completedAfterError(ctx, p.Record, p.Preparation.CheckpointScope, err)
			if recoveryErr == nil {
				result, _, replayErr := r.replay(ctx, completed)
				return result, replayErr
			}
			err = recoveryErr
		}
		if !errors.Is(err, errCloudAttemptAdvanced) {
			return result, err
		}
		// Only acquisition may follow a competing caller to a newer attempt.
		// Retrying a submission here could dispatch another paid request.
		if step != cloudAcquire {
			return llm.ExecutionResultV1{}, cloudRuntimeError(contracts.ErrConflict, true)
		}
	}
	return llm.ExecutionResultV1{}, cloudRuntimeError(errCloudAttemptAdvanced, false)
}

var errCloudAttemptAdvanced = errors.New("cloud request attempt advanced")

func (r *CloudExecutionRuntime) advanceAttempt(ctx context.Context, p PreparedCloudRequest, step cloudStep) (llm.ExecutionResultV1, error) {
	if done, found, err := r.replay(ctx, p); found || err != nil {
		return done, err
	}
	if p.Compact != nil {
		input, err := PrepareCompactInput(ctx, *p.Compact, p.CompactReplay)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		if input.Request == nil {
			return r.finishNoWork(ctx, p)
		}
	}
	root := p.Record.Request
	attempt, err := r.store.LoadRequestAttempt(ctx, root.Scope, root.ID)
	if errors.Is(err, cloudstate.ErrRequestAttemptMissing) {
		attempt, err = r.store.BeginRequestAttempt(ctx, root.Scope, root.ID, "", r.now())
	}
	if err != nil {
		return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
	}
	saved, loadErr := r.store.LoadProviderExecution(ctx, root.Scope, attempt.ID)
	if loadErr == nil {
		// Only explicit acquisition can replace an unresolved paid attempt.
		// The old child remains charged and discoverable independently.
		if step == cloudAcquire && saved.Execution.Stage == cloudstate.ExecutionUnknown && !r.now().Before(saved.Execution.RecoverAfter) {
			if err := r.finishUnknownFill(ctx, p, attempt, saved); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			attempt, err = r.store.BeginRequestAttempt(ctx, root.Scope, root.ID, attempt.ID, r.now())
			if err != nil {
				return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
			}
		} else if step == cloudAcquire && saved.Execution.Stage == cloudstate.ExecutionFailed && saved.Execution.Failure.Retryable {
			result, err := r.execution.Resume(ctx, root.Scope, attempt.ID, p.GenerateReplay, p.CompactReplay)
			if err != nil {
				return llm.ExecutionResultV1{}, err
			}
			if _, err := r.finishProviderStep(ctx, p, attempt, result); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			if r.now().Before(saved.Execution.Failure.RetryNotBefore) {
				return cloudStatus(p, llm.ExecutionBudgetWait, saved.Execution.Failure.RetryNotBefore.Sub(r.now())), nil
			}
			attempt, err = r.store.BeginRequestAttempt(ctx, root.Scope, root.ID, attempt.ID, r.now())
			if err != nil {
				return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
			}
		} else {
			return r.resumeAttempt(ctx, p, attempt, saved, step)
		}
	} else if !errors.Is(loadErr, cloudstate.ErrProviderExecutionMissing) && !errors.Is(loadErr, cloudstate.ErrBudgetPlanMissing) {
		return llm.ExecutionResultV1{}, cloudRuntimeError(loadErr, false)
	}
	// An unused quote/fill can expire while a workflow waits for capacity.
	// Renew the child, never mutate the original quote or reservation identity.
	if !r.now().Before(attempt.CreatedAt.Add(cache.MaxFillLease)) {
		attempt, err = r.store.BeginRequestAttempt(ctx, root.Scope, root.ID, attempt.ID, r.now())
		if err != nil {
			return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
		}
	}
	budgetAttempt := BudgetAttempt{PriorCandidates: append([]string(nil), attempt.PriorCandidates...), OperationID: durable.OperationID(attempt.ID), GenerationID: r.options.BudgetGeneration, QuotedAt: attempt.CreatedAt, ExpiresAt: attempt.CreatedAt.Add(cache.MaxFillLease)}
	var call *CloudBudgetCall
	if p.Generate != nil {
		call, err = r.execution.admission.PrepareGenerate(ctx, root.Scope, attempt.ID, p.GenerateReplay, budgetAttempt)
	} else {
		call, err = r.execution.admission.PrepareCompact(ctx, root.Scope, attempt.ID, p.CompactReplay, budgetAttempt)
	}
	if err != nil {
		return r.resumeAdmissionWinner(ctx, p, attempt, step, err)
	}
	lease, err := r.cacheLease(ctx, p, attempt, call.plan)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	if lease != nil {
		status, found, err := r.prepareCache(ctx, p, *lease)
		if err != nil || found {
			return status, err
		}
	}
	if step != cloudAcquire && step != cloudSubmit {
		return cloudStatus(p, llm.ExecutionBudgetRequired, 0), nil
	}
	reservation, err := r.execution.admission.Reserve(ctx, call)
	if err != nil {
		return r.resumeAdmissionWinner(ctx, p, attempt, step, err)
	}
	if call.plan.RequiresReservation() && !reservation.Accepted {
		return cloudStatus(p, llm.ExecutionBudgetWait, reservation.RetryAfter), nil
	}
	if step == cloudAcquire {
		return cloudStatus(p, llm.ExecutionAcquired, 0), nil
	}
	var start func(context.Context) error
	if lease != nil {
		start = func(ctx context.Context) error {
			won, err := r.capabilities.ResponseFills.Start(ctx, *lease, r.now())
			if err != nil {
				return err
			}
			if !won {
				return cloudstate.ErrCorrupt
			}
			return nil
		}
	}
	result, err := r.execution.submit(ctx, call, reservation, start)
	if err != nil {
		return llm.ExecutionResultV1{}, err
	}
	return r.finishProviderStep(ctx, p, attempt, result)
}

// Admission is fenced as soon as another caller starts the same attempt. A
// read of "not started" can race either plan preparation or Reserve's recheck.
// Recover only with durable evidence of that exact attempt's execution, and
// only before this invocation has reached the dispatch path. Resume never
// submits or acquires a replacement paid attempt, even if the parent advanced.
func (r *CloudExecutionRuntime) resumeAdmissionWinner(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, step cloudStep, cause error) (llm.ExecutionResultV1, error) {
	var classified *provider.Error
	if !errors.As(cause, &classified) || classified.Code != provider.CodeOperationConflict || classified.Dispatch != provider.DispatchNotDispatched {
		return llm.ExecutionResultV1{}, cause
	}
	saved, err := r.store.LoadProviderExecution(ctx, p.Record.Request.Scope, attempt.ID)
	if err != nil {
		return llm.ExecutionResultV1{}, cause
	}
	return r.resumeAttempt(ctx, p, attempt, saved, step)
}

func (r *CloudExecutionRuntime) resumeAttempt(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, saved cloudstate.SavedProviderExecution, step cloudStep) (llm.ExecutionResultV1, error) {
	result := r.execution.result(saved)
	if step == cloudPoll || step == cloudSubmit || saved.Execution.Stage == cloudstate.ExecutionSucceeded || saved.Execution.Stage == cloudstate.ExecutionFailed {
		var err error
		result, err = r.execution.Resume(ctx, p.Record.Request.Scope, attempt.ID, p.GenerateReplay, p.CompactReplay)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
	}
	if result.Saved.Execution.Stage == cloudstate.ExecutionSucceeded && step == cloudComplete {
		checked, err := r.finishProviderStep(ctx, p, attempt, result)
		if err != nil || checked.State == llm.ExecutionFailed {
			return checked, err
		}
		return r.finishAttempt(ctx, p, attempt, result.Saved)
	}
	return r.finishProviderStep(ctx, p, attempt, result)
}

// A terminal failure also releases cache ownership, without replacing any
// older successful entry. Invalid summaries must never become checkpoints.
func (r *CloudExecutionRuntime) finishProviderStep(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, result ProviderExecutionResult) (llm.ExecutionResultV1, error) {
	outcome := cache.FillFailed
	failed := result.Saved.Execution.Stage == cloudstate.ExecutionFailed
	invalidSummary := false
	if p.Compact != nil && result.Saved.Execution.Stage == cloudstate.ExecutionSucceeded {
		_, err := compaction.PlainTextSummary(*result.Saved.Execution.Response, int(r.publication.limits.MaxBytes))
		invalidSummary = err != nil
		outcome = cache.FillNotCacheable
	}
	if failed || invalidSummary {
		if err := r.finishTerminalFill(ctx, p, attempt, result.Saved, outcome); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		failure := r.providerResult(p, result)
		if invalidSummary {
			failure = cloudStatus(p, llm.ExecutionFailed, 0)
			failure.FailureCode = "incomplete_response"
		}
		if err := r.store.FinishRequestFailure(ctx, p.Record.Request.Scope, p.Record.Request.ID, attempt.ID, failure, r.now()); err != nil {
			if errors.Is(err, contracts.ErrConflict) {
				current, loadErr := r.store.LoadRequestAttempt(ctx, p.Record.Request.Scope, p.Record.Request.ID)
				if loadErr != nil {
					return llm.ExecutionResultV1{}, cloudRuntimeError(loadErr, true)
				}
				if current.ID != attempt.ID {
					return llm.ExecutionResultV1{}, errCloudAttemptAdvanced
				}
			}
			return llm.ExecutionResultV1{}, cloudRuntimeError(err, true)
		}
		return failure, nil
	}
	return r.providerResult(p, result), nil
}

func (r *CloudExecutionRuntime) now() time.Time { return r.capabilities.Clock().UTC() }
func cloudStatus(p PreparedCloudRequest, status llm.ExecutionStateV1, wait time.Duration) llm.ExecutionResultV1 {
	result := llm.ExecutionResultV1{RequestID: string(p.Record.Request.ID), Kind: p.Record.Request.Kind, State: status}
	if status == llm.ExecutionBudgetWait || status == llm.ExecutionCacheWait || status == llm.ExecutionPending {
		result.RetryAfterSeconds = int32(math.Min(86400, math.Max(1, math.Ceil(wait.Seconds()))))
	}
	return result
}
func (r *CloudExecutionRuntime) providerResult(p PreparedCloudRequest, result ProviderExecutionResult) llm.ExecutionResultV1 {
	switch result.Saved.Execution.Stage {
	case cloudstate.ExecutionSucceeded:
		return cloudStatus(p, llm.ExecutionProviderCompleted, 0)
	case cloudstate.ExecutionFailed:
		failure := cloudStatus(p, llm.ExecutionFailed, 0)
		failure.FailureCode = "provider_error"
		failure.Retryable = result.Saved.Execution.Failure.Retryable
		return failure
	case cloudstate.ExecutionUnknown:
		if r.now().Before(result.Saved.Execution.RecoverAfter) {
			return cloudStatus(p, llm.ExecutionPending, result.Saved.Execution.RecoverAfter.Sub(r.now()))
		}
		return cloudStatus(p, llm.ExecutionOutcomeUnknown, 0)
	default:
		return cloudStatus(p, llm.ExecutionPending, result.RetryAfter)
	}
}
func (r *CloudExecutionRuntime) replay(ctx context.Context, p PreparedCloudRequest) (llm.ExecutionResultV1, bool, error) {
	var data []byte
	var err error
	if p.Record.Status == cloudstate.StatusFailed {
		var progress struct {
			Failure llm.ExecutionResultV1 `json:"execution_failure"`
		}
		if json.Unmarshal(p.Record.Progress, &progress) != nil || progress.Failure.Validate() != nil || progress.Failure.State != llm.ExecutionFailed || progress.Failure.Retryable || progress.Failure.RequestID != string(p.Record.Request.ID) || progress.Failure.Kind != p.Record.Request.Kind {
			return llm.ExecutionResultV1{}, true, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		return progress.Failure, true, nil
	}
	if p.Record.Status == cloudstate.StatusCompleted {
		var result cloudRuntimeResult
		if json.Unmarshal(p.Record.Progress, &result) != nil || result.Version != 1 {
			return llm.ExecutionResultV1{}, true, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
		data = result.Response
	} else {
		var found bool
		key := cloudPublicKey(p)
		data, found, err = r.capabilities.Finalizer.replay(ctx, p.Record.Request.Scope, p.Record, p.Record.Request.Kind, key, p.Record.Request.RequestIndex)
		if err != nil || !found {
			return llm.ExecutionResultV1{}, found, err
		}
		_, err = r.storeResult(ctx, p, data)
		if err != nil {
			return llm.ExecutionResultV1{}, true, err
		}
	}
	result, err := cloudCompleted(p, data)
	return result, true, err
}
func cloudPublicKey(p PreparedCloudRequest) string {
	if p.Generate != nil {
		return p.Generate.OperationKey
	}
	return p.Compact.OperationKey
}
func cloudCompleted(p PreparedCloudRequest, data []byte) (llm.ExecutionResultV1, error) {
	result := cloudStatus(p, llm.ExecutionCompleted, 0)
	if p.Generate != nil {
		result.Generate = new(llm.GenerateResponseV1)
		if json.Unmarshal(data, result.Generate) != nil || result.Generate.OperationID != result.RequestID || result.Generate.OperationKey != p.Generate.OperationKey {
			return llm.ExecutionResultV1{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
	} else {
		result.Compact = new(llm.CompactResponseV1)
		if json.Unmarshal(data, result.Compact) != nil || result.Compact.OperationID != result.RequestID || result.Compact.OperationKey != p.Compact.OperationKey {
			return llm.ExecutionResultV1{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
		}
	}
	return result, result.Validate()
}
func (r *CloudExecutionRuntime) storeResult(ctx context.Context, p PreparedCloudRequest, data []byte) (llm.ExecutionResultV1, error) {
	result, err := cloudCompleted(p, data)
	if err != nil {
		return result, err
	}
	progress, err := json.Marshal(cloudRuntimeResult{Version: 1, Response: data})
	if err != nil {
		return llm.ExecutionResultV1{}, cloudRuntimeError(cloudstate.ErrCorrupt, false)
	}
	final, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, err = r.store.CompleteOperation(final, p.Record.Request.Scope, p.Record.Request.ID, progress, r.now())
	if err != nil {
		return llm.ExecutionResultV1{}, cloudRuntimeError(err, true)
	}
	return result, nil
}
func (r *CloudExecutionRuntime) publicationIdentity(p PreparedCloudRequest, at time.Time) CheckpointPublicationIdentity {
	return CheckpointPublicationIdentity{Scope: p.Preparation.CheckpointScope, OperationID: state.OperationID(p.Record.Request.ID), CheckpointID: state.CheckpointID(strings.TrimPrefix(string(p.Record.Request.ID), cloudstate.RequestIDPrefix)), CreatedAt: at, ExpiresAt: at.Add(r.options.CheckpointTTL)}
}
