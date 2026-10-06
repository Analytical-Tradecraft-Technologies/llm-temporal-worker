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
	FinishRequestExhausted(context.Context, cloudstate.Scope, cloudstate.RequestID, cloudstate.RequestID, int, time.Time) (llm.ExecutionResultV1, error)
	CacheFingerprint(cache.Input) (cache.Fingerprint, error)
}

// CloudExecutionOptions supplies deployment-owned authorization and signing.
// No default resolver trusts the tenant/project fields of an incoming payload.
type CloudExecutionOptions struct {
	ResolveScope  CheckpointScopeResolver
	Keyring       *state.Keyring
	Limits        state.MaterializeLimits
	RequestLimits CloudRequestLimits
	// FinalizationTimeout bounds each detached write that records a result
	// after the caller may have gone (server.finalization_timeout). Zero
	// means 10 seconds.
	FinalizationTimeout time.Duration
	CheckpointTTL       time.Duration
	BudgetGeneration    durable.GenerationID
	MaxAttempts         int
	// ReloadGrace bounds how long unpaid work prepared under another
	// configuration waits for a compatible worker after this runtime starts.
	// Zero means 15 minutes.
	ReloadGrace time.Duration
}

// defaultCloudReloadGrace covers a rolling deployment: until it elapses a
// worker on the previous configuration may still serve the request.
const defaultCloudReloadGrace = 15 * time.Minute

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
	// startedAt is when this runtime's configuration began serving.
	startedAt time.Time
}

var _ activity.ExecutionRuntime = (*CloudExecutionRuntime)(nil)

func (c V1RuntimeCapabilities) NewCloudExecutionRuntime(ctx context.Context, options CloudExecutionOptions) (*CloudExecutionRuntime, error) {
	if options.MaxAttempts < 0 || !options.RequestLimits.valid() {
		return nil, executionError(provider.CodeConfiguration)
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 6
	}
	if options.FinalizationTimeout < 0 {
		return nil, executionError(provider.CodeConfiguration)
	}
	if options.FinalizationTimeout == 0 {
		options.FinalizationTimeout = defaultCloudFinalizationTimeout
	}
	if options.ReloadGrace < 0 {
		return nil, executionError(provider.CodeConfiguration)
	}
	if options.ReloadGrace == 0 {
		options.ReloadGrace = defaultCloudReloadGrace
	}
	store, ok := c.Requests.(cloudExecutionStore)
	if !ok || isNilCapability(store) || c.Finalizer == nil || isNilCapability(c.Responses) || isNilCapability(c.ResponseFills) || options.CheckpointTTL <= 0 || options.BudgetGeneration.Validate() != nil {
		return nil, executionError(provider.CodeConfiguration)
	}
	preparation, err := c.NewCloudRequestPreparation(options.ResolveScope, options.Limits)
	if err != nil {
		return nil, err
	}
	preparation.requestLimits = options.RequestLimits
	execution, err := c.NewCloudProviderExecution(ctx)
	if err != nil {
		return nil, err
	}
	execution.finalizationTimeout = options.FinalizationTimeout
	publication, err := c.NewCheckpointPublication(options.Keyring, options.Limits)
	if err != nil {
		return nil, err
	}
	return &CloudExecutionRuntime{capabilities: c, store: store, preparation: preparation, execution: execution, publication: publication, options: options, startedAt: c.Clock().UTC()}, nil
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
	return tracedStep(ctx, "prepare", func(ctx context.Context) (llm.ExecutionResultV1, error) {
		return r.prepareStep(ctx, input, cloudPrepare)
	})
}
func (r *CloudExecutionRuntime) GenerateStepV1(ctx context.Context, input llm.GenerateRequestV1) (llm.ExecutionResultV1, error) {
	return tracedStep(ctx, "generate", func(ctx context.Context) (llm.ExecutionResultV1, error) {
		return r.prepareStep(ctx, llm.PrepareExecutionV1{Generate: &input}, cloudSubmit)
	})
}
func (r *CloudExecutionRuntime) CompactStepV1(ctx context.Context, input llm.CompactRequestV1) (llm.ExecutionResultV1, error) {
	return tracedStep(ctx, "compact", func(ctx context.Context) (llm.ExecutionResultV1, error) {
		return r.prepareStep(ctx, llm.PrepareExecutionV1{Compact: &input}, cloudSubmit)
	})
}
func (r *CloudExecutionRuntime) AcquireBudgetV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return tracedStep(ctx, "acquire", func(ctx context.Context) (llm.ExecutionResultV1, error) {
		return r.referenceStep(ctx, ref, cloudAcquire)
	})
}
func (r *CloudExecutionRuntime) PollExecutionV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return tracedStep(ctx, "poll", func(ctx context.Context) (llm.ExecutionResultV1, error) { return r.referenceStep(ctx, ref, cloudPoll) })
}
func (r *CloudExecutionRuntime) CompleteExecutionV1(ctx context.Context, ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
	return tracedStep(ctx, "complete", func(ctx context.Context) (llm.ExecutionResultV1, error) {
		return r.referenceStep(ctx, ref, cloudComplete)
	})
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

// cloudConfigurationRetired reports unpaid work prepared under a
// configuration that no worker serves any more. Retrying the same operation key
// cannot help; the caller submits the request again under a new key.
func cloudConfigurationRetired() error {
	return provider.NewError(provider.CodeConfiguration, provider.PhasePlan, provider.DispatchNotDispatched, provider.RetryNever,
		"request was prepared under a configuration this worker no longer serves; submit it again with a new operation key")
}

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
	// Saved terminal provider results need no current adapter or routing config.
	// An attempt that may already have reached its provider is polled or
	// recovered, never submitted again, and only when recovery finds its route
	// and endpoint unchanged. Other work must wait for a compatible worker;
	// never poison its operation key or plan, admit or replace an attempt under
	// a different configuration during a rollout.
	if p.Preparation.ConfigDigest != r.preparation.digest {
		attempt, err := r.store.LoadRequestAttempt(ctx, root.Scope, root.ID)
		if err == nil {
			saved, loadErr := r.store.LoadProviderExecution(ctx, root.Scope, attempt.ID)
			replaces := step == cloudAcquire && loadErr == nil && saved.Execution.Stage == cloudstate.ExecutionUnknown && !r.now().Before(saved.Execution.RecoverAfter)
			// A retryable failure asks acquisition for a new attempt, which
			// needs a compatible worker. Returning the saved failure again
			// would loop the workflow on it (#1162).
			retries := step == cloudAcquire && loadErr == nil && saved.Execution.Stage == cloudstate.ExecutionFailed && saved.Execution.Failure.Retryable
			if loadErr == nil && !replaces && !retries {
				return r.resumeAttempt(ctx, p, attempt, saved, step)
			}
			if retries {
				// Settle the failure and release its cache ownership; that is
				// accounting only. The attempt limit belongs to the request's
				// own configuration, so exhaustion waits for a compatible worker.
				if _, err := r.resumeAttempt(ctx, p, attempt, saved, step); err != nil {
					return llm.ExecutionResultV1{}, err
				}
			}
			if loadErr == nil && saved.Execution.Stage == cloudstate.ExecutionUnknown {
				// Settling an expired unknown attempt is accounting only: it
				// charges the saved reservation and never calls a provider,
				// so it must not wait for a compatible worker. Otherwise a
				// drained rollout would hold the claim forever.
				if _, err := r.execution.settleUnknown(ctx, root.Scope, attempt.ID, saved); err != nil {
					return llm.ExecutionResultV1{}, err
				}
			}
			if loadErr != nil && !errors.Is(loadErr, cloudstate.ErrProviderExecutionMissing) && !errors.Is(loadErr, cloudstate.ErrBudgetPlanMissing) {
				return llm.ExecutionResultV1{}, cloudRuntimeError(loadErr, false)
			}
		} else if !errors.Is(err, cloudstate.ErrRequestAttemptMissing) {
			return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
		}
		// Nothing unfinished is paid. A request prepared before this worker's
		// configuration began serving belongs to an older configuration: wait
		// while a worker on it may still exist, then fail fast instead of
		// waiting forever (#1161). A request prepared later may belong to a
		// newer configuration rolling out, so an older worker always waits.
		// The request is not changed, so restoring its configuration resumes it.
		if p.Preparation.PreparedAt.Before(r.startedAt) && r.now().Sub(r.startedAt) >= r.options.ReloadGrace {
			return llm.ExecutionResultV1{}, cloudConfigurationRetired()
		}
		return llm.ExecutionResultV1{}, providerPlanningError(provider.CodeStateUnavailable, provider.PhasePlan, provider.RetrySameOperation)
	}
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
			// Charge the old attempt at its reservation before replacing it,
			// so its claim ages out of each window instead of being held.
			recovered, err := r.execution.Resume(ctx, root.Scope, attempt.ID, p.GenerateReplay, p.CompactReplay)
			if err != nil {
				return llm.ExecutionResultV1{}, err
			}
			// Polling or idempotent recovery may have resolved the attempt
			// (pending, succeeded or failed); finish that outcome instead of
			// replacing it, which only an unknown attempt permits.
			if recovered.Saved.Execution.Stage != cloudstate.ExecutionUnknown {
				return r.resumeAttempt(ctx, p, attempt, recovered.Saved, step)
			}
			if err := r.finishUnknownFill(ctx, p, attempt, saved); err != nil {
				return llm.ExecutionResultV1{}, err
			}
			if len(attempt.PriorCandidates)+1 >= r.options.MaxAttempts {
				failure, err := r.store.FinishRequestExhausted(ctx, root.Scope, root.ID, attempt.ID, r.options.MaxAttempts, r.now())
				if err != nil {
					return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
				}
				return failure, nil
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
			if len(attempt.PriorCandidates)+1 >= r.options.MaxAttempts {
				failure, err := r.store.FinishRequestExhausted(ctx, root.Scope, root.ID, attempt.ID, r.options.MaxAttempts, r.now())
				if err != nil {
					return llm.ExecutionResultV1{}, cloudRuntimeError(err, false)
				}
				return failure, nil
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
			// The lease was stamped by the quoting worker; a lagging clock starts
			// the fill as of acquisition rather than failing the paid attempt.
			now := r.now()
			if now.Before(lease.AcquiredAt) {
				now = lease.AcquiredAt
			}
			won, err := r.capabilities.ResponseFills.Start(ctx, *lease, now)
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
// Generate output that cannot extend the transcript is closed the same way as
// soon as the paid response is saved: publication would reject it on every
// retry, leaving the settled response stranded behind a running request.
func (r *CloudExecutionRuntime) finishProviderStep(ctx context.Context, p PreparedCloudRequest, attempt cloudstate.RequestAttempt, result ProviderExecutionResult) (llm.ExecutionResultV1, error) {
	outcome := cache.FillFailed
	failed := result.Saved.Execution.Stage == cloudstate.ExecutionFailed
	unpublishable := false
	if p.Compact != nil && result.Saved.Execution.Stage == cloudstate.ExecutionSucceeded {
		_, err := compaction.PlainTextSummary(*result.Saved.Execution.Response, int(r.publication.limits.MaxBytes))
		unpublishable = err != nil
		outcome = cache.FillNotCacheable
	}
	if p.Generate != nil && result.Saved.Execution.Stage == cloudstate.ExecutionSucceeded {
		prepared, err := PrepareGenerateInput(ctx, *p.Generate, p.GenerateReplay)
		if err != nil {
			return llm.ExecutionResultV1{}, err
		}
		_, err = state.ValidateTranscript(append(append([]llm.Item(nil), prepared.Request.Input...), result.Saved.Execution.Response.Output...))
		unpublishable = err != nil
		outcome = cache.FillNotCacheable
	}
	if failed || unpublishable {
		if err := r.finishTerminalFill(ctx, p, attempt, result.Saved, outcome); err != nil {
			return llm.ExecutionResultV1{}, err
		}
		failure := r.providerResult(p, result)
		if unpublishable {
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

// providerFailureResult reports a failed provider attempt with its stable
// error code and dispatch certainty. A definite rejection (the provider never
// accepted the request) is provider_rejected; anything that may have reached
// the provider stays provider_error.
func providerFailureResult(p PreparedCloudRequest, failure cloudstate.ExecutionFailure) llm.ExecutionResultV1 {
	result := cloudStatus(p, llm.ExecutionFailed, 0)
	result.FailureCode = "provider_error"
	if failure.Dispatch == provider.DispatchRejected || failure.Dispatch == provider.DispatchNotDispatched {
		result.FailureCode = "provider_rejected"
	}
	result.ErrorCode, result.Dispatch, result.Retryable = string(failure.Code), string(failure.Dispatch), failure.Retryable
	return result
}
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
		return providerFailureResult(p, *result.Saved.Execution.Failure)
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
	final, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.options.FinalizationTimeout)
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
