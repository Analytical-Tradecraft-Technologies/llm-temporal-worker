package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	blobstore "github.com/mfow/llm-temporal-worker/golang/storage/blob"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

const checkpointCompilerEpoch = "llmtw-runtime-v1"

// NewProductionGeneratePortsFactory builds the complete snapshot-owned durable
// Generate phase. Construction is side-effect free; every dependency is
// validated before the returned callbacks can be registered with Temporal.
func NewProductionGeneratePortsFactory() GeneratePortsFactory {
	return func(_ context.Context, capabilities V1RuntimeCapabilities) (durablestore.GeneratePorts, error) {
		binding, err := newProductionPhaseBinding(capabilities)
		if err != nil {
			return durablestore.GeneratePorts{}, err
		}
		return binding.generatePorts(), nil
	}
}

type productionPhaseBinding struct {
	cap         V1RuntimeCapabilities
	composition durablestore.Composition
	codec       state.CheckpointBlobCodec
}

func newProductionPhaseBinding(capabilities V1RuntimeCapabilities) (*productionPhaseBinding, error) {
	composition, ok := capabilities.DurableComposition()
	if !ok {
		return nil, errors.New("durable composition is unavailable")
	}
	if err := composition.Validate(); err != nil {
		return nil, fmt.Errorf("validate durable composition: %w", err)
	}
	if capabilities.Snapshot == nil || capabilities.Planner == nil || capabilities.Estimator.MaxOutput <= 0 || capabilities.Adapters == nil {
		return nil, errors.New("routing, pricing, estimator, and provider capabilities are required")
	}
	checkpoints := capabilities.Checkpoints
	if checkpoints.Repository == nil || checkpoints.Materializer == nil || checkpoints.Blobs == nil || checkpoints.BlobStore == nil || checkpoints.IssueHandle == nil || checkpoints.VerifyHandle == nil || checkpoints.BlobRepository.Pool == nil {
		return nil, errors.New("complete PostgreSQL/S3 checkpoint capabilities are required")
	}
	if capabilities.ResolveScope == nil {
		return nil, errors.New("PostgreSQL scope resolver is required")
	}
	if err := capabilities.BudgetGenerationID.Validate(); err != nil {
		return nil, err
	}
	if err := capabilities.BudgetIncarnationID.Validate(); err != nil {
		return nil, err
	}
	if capabilities.OperationRetention <= 0 || capabilities.CheckpointRetention <= 0 || capabilities.MaxRequestBytes <= 0 {
		return nil, errors.New("durable retention and request bounds are required")
	}
	return &productionPhaseBinding{cap: capabilities, composition: composition, codec: state.CheckpointBlobCodec{MaxBytes: int(capabilities.MaxRequestBytes), MaxDepth: llm.DefaultCanonicalMaxDepth}}, nil
}

func (binding *productionPhaseBinding) generatePorts() durablestore.GeneratePorts {
	return durablestore.GeneratePorts{
		Replay:             binding.replayGenerate,
		CacheLookup:        binding.generateCacheLookup,
		CompactionDecision: binding.generateCompactionDecision,
		Compact:            binding.compactForGenerate,
		Route:              binding.routeGenerate,
		Reserve:            binding.reserveGenerate,
		Journal:            binding.journalGenerate,
		Dispatch:           binding.dispatchGenerate,
		Finalize:           binding.finalizeGenerate,
		FinalizeCache:      binding.finalizeGenerateCache,
		Reconcile:          binding.reconcileGenerate,
	}
}

func operationIdentity(kind, operationKey string, digest [32]byte) durablestore.OperationID {
	material := make([]byte, 0, len(kind)+len(operationKey)+len(digest)+2)
	material = append(material, kind...)
	material = append(material, 0)
	material = append(material, operationKey...)
	material = append(material, 0)
	material = append(material, digest[:]...)
	return durablestore.OperationID(uuid.NewSHA1(uuid.NameSpaceOID, material).String())
}

func requestManifest(value any) ([]byte, [32]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, [32]byte{}, err
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return canonical, sha256.Sum256(canonical), nil
}

func rawScope(value llm.RequestContext) string { return value.Tenant + "\x00" + value.Project }

func (binding *productionPhaseBinding) beginOperation(ctx context.Context, kind, apiVersion, operationKey string, requestContext llm.RequestContext, manifest []byte, digest [32]byte) (durablestore.OperationID, admission.BeginResult, error) {
	operationID := operationIdentity(kind, operationKey, digest)
	now := binding.now()
	result, err := binding.composition.Operations.Begin(ctx, admission.BeginRequest{
		ID: string(operationID), ScopeKey: rawScope(requestContext), RequestDigest: digest,
		ConfigVersion: hex.EncodeToString(binding.cap.ConfigDigest[:]), ExpiresAt: now.Add(binding.cap.OperationRetention),
		OperationKind: kind, APIVersion: apiVersion, RequestSchemaVersion: 1,
		RequestManifest: manifest, ConfigDigest: binding.cap.ConfigDigest,
	})
	return operationID, result, err
}

func (binding *productionPhaseBinding) replayGenerate(ctx context.Context, request llm.GenerateRequestV1) (durablestore.GenerateReplay, error) {
	manifest, digest, err := requestManifest(request)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	_, begun, err := binding.beginOperation(ctx, "generate", llm.APIVersion, request.OperationKey, request.Context, manifest, digest)
	if err != nil {
		return durablestore.GenerateReplay{}, err
	}
	replay := durablestore.GenerateReplay{}
	if request.Parent != nil {
		scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
		if err != nil {
			return durablestore.GenerateReplay{}, err
		}
		materialized, err := binding.cap.Checkpoints.Materializer.MaterializeHandle(ctx, scopeID, string(*request.Parent), state.MaterializeLimits{MaxItems: 100000, MaxRows: 100000, MaxBytes: binding.cap.MaxRequestBytes, MaxDepth: 100000})
		if err != nil {
			return durablestore.GenerateReplay{}, err
		}
		replay.State = materialized
	}
	if begun.Existing {
		switch begun.Operation.State {
		case admission.StateCompleted:
			response, err := binding.composition.Results.Get(ctx, begun.Operation.ID)
			if err != nil {
				return durablestore.GenerateReplay{}, fmt.Errorf("load completed result: %w", err)
			}
			mapped, err := generateResponseFromNormalized(response, request.Parent, "miss_populated")
			if err != nil {
				return durablestore.GenerateReplay{}, err
			}
			route, err := binding.routeGenerate(ctx, request, replay, durablestore.CompactionDecision{})
			if err != nil {
				return durablestore.GenerateReplay{}, fmt.Errorf("reconstruct completed route: %w", err)
			}
			reservation, err := binding.reserveGenerate(ctx, request, route)
			if err != nil {
				return durablestore.GenerateReplay{}, fmt.Errorf("recover completed reservation: %w", err)
			}
			if err := binding.reconcileGenerate(ctx, request, route, reservation, durablestore.GenerateFinalization{Response: mapped}); err != nil {
				return durablestore.GenerateReplay{}, fmt.Errorf("reconcile completed operation: %w", err)
			}
			replay.Completed = &mapped
			return replay, nil
		case admission.StateAmbiguous, admission.StateDispatching, admission.StateProviderPending:
			return durablestore.GenerateReplay{}, errors.New("durable provider outcome requires operator-safe recovery")
		case admission.StateDefiniteFailed:
			return durablestore.GenerateReplay{}, errors.New("durable operation previously failed")
		}
	}
	return replay, nil
}

func (binding *productionPhaseBinding) generateCacheLookup(_ context.Context, request llm.GenerateRequestV1, _ durablestore.GenerateReplay) (durablestore.CacheDecision, error) {
	if request.Cache != nil {
		return durablestore.CacheDecision{}, errors.New("durable response cache is not configured for this snapshot")
	}
	return durablestore.CacheDecision{Disposition: durablestore.CacheDisabled}, nil
}

func (binding *productionPhaseBinding) generateCompactionDecision(_ context.Context, _ llm.GenerateRequestV1, replay durablestore.GenerateReplay, _ durablestore.CacheDecision) (durablestore.CompactionDecision, error) {
	if len(replay.State.Settings.CompactionPolicy) != 0 {
		return durablestore.CompactionDecision{}, errors.New("automatic compaction policy requires an explicit compact child")
	}
	return durablestore.CompactionDecision{}, nil
}

func (binding *productionPhaseBinding) compactForGenerate(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay) (durablestore.GenerateReplay, error) {
	return durablestore.GenerateReplay{}, errors.New("automatic compaction was not selected")
}

func requestFromReplay(request llm.GenerateRequestV1, replay durablestore.GenerateReplay) (llm.Request, state.ModelState, error) {
	base := replay.State.Settings
	if request.Parent == nil {
		base = state.RootModelState("")
	}
	patch, err := state.SettingsPatchFromV1(request.SettingsPatch)
	if err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	settings, err := state.ApplySettingsPatch(base, patch)
	if err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	if err := settings.Validate(); err != nil {
		return llm.Request{}, state.ModelState{}, err
	}
	input := make([]llm.Item, 0, len(replay.State.Items)+len(request.Append))
	input = append(input, replay.State.Items...)
	input = append(input, request.Append...)
	providerRequest := llm.Request{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, Context: request.Context,
		Model: settings.Model, ServiceClass: settings.ServiceClass, ServiceClassFallbacks: append([]llm.ServiceClass(nil), settings.ServiceClassFallbacks...),
		Portability: settings.Portability, Instructions: append([]llm.Instruction(nil), settings.Instructions...), Input: input,
		Tools: append([]llm.Tool(nil), settings.Tools...), ToolPolicy: settings.ToolPolicy, Output: settings.Output,
		Sampling:   &llm.SamplingSpec{Temperature: settings.Temperature},
		Reasoning:  &llm.ReasoningSpec{Effort: settings.ReasoningEffort, Summary: settings.ReasoningSummary},
		Extensions: cloneRawMessages(settings.Extensions),
	}
	return providerRequest, settings, nil
}

func cloneRawMessages(source map[string]json.RawMessage) map[string]json.RawMessage {
	if source == nil {
		return nil
	}
	result := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		result[key] = append(json.RawMessage(nil), value...)
	}
	return result
}

func (binding *productionPhaseBinding) routeGenerate(ctx context.Context, request llm.GenerateRequestV1, replay durablestore.GenerateReplay, _ durablestore.CompactionDecision) (durablestore.RoutePlan, error) {
	providerRequest, _, err := requestFromReplay(request, replay)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	normalized, err := llm.NormalizeRequest(providerRequest)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	snapshot, err := binding.cap.Snapshot.Current(ctx)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if snapshot.Prices == nil {
		return durablestore.RoutePlan{}, errors.New("pricing resolver is unavailable")
	}
	now := binding.now()
	plan, err := binding.cap.Planner.Plan(ctx, routing.Input{Request: normalized, Catalog: snapshot.Routes, Health: snapshot.Health, Now: now})
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	_, requestDigest, err := requestManifest(request)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	operationID := operationIdentity("generate", request.OperationKey, requestDigest)
	for _, candidate := range plan.Candidates {
		matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(normalized, candidate, snapshot.Environment))
		if snapshot.RequireBudgetMatch && len(matches) == 0 {
			continue
		}
		quote, quoteErr := snapshot.Prices.Resolve(pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID, Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now})
		if quoteErr != nil {
			continue
		}
		entry := quote.Entry
		if entry.Version == "" {
			entry.Version = quote.CatalogVersion
		}
		estimate, estimateErr := binding.cap.Estimator.EstimateCandidate(normalized, candidate, entry)
		if estimateErr != nil {
			continue
		}
		reservations := make([]admission.WindowReservation, 0, len(matches))
		for _, match := range matches {
			_, bucket := match.Window.Range(now)
			reservations = append(reservations, admission.WindowReservation{PolicyID: match.PolicyID, WindowID: match.Window.ID, Bucket: bucket, Amount: estimate.MicroUSD, Limit: match.Window.Limit, AmountUSD: estimate.CostUSD, LimitUSD: match.Window.LimitUSD, BucketNanos: match.Window.Bucket.Nanoseconds(), DurationNanos: match.Window.Duration.Nanoseconds()})
		}
		return durablestore.RoutePlan{OperationID: operationID, GenerationID: binding.cap.BudgetGenerationID, RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, Model: candidate.Model, PriceVersion: entry.Version, Execution: &durablestore.RouteExecution{Request: normalized, Candidate: candidate, Price: entry, Reservations: reservations, EstimatedUSD: estimate.CostUSD}}, nil
	}
	return durablestore.RoutePlan{}, errors.New("no eligible priced route")
}

func (binding *productionPhaseBinding) reserveGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan) (durablestore.ReserveResult, error) {
	if route.Execution == nil {
		return durablestore.ReserveResult{}, errors.New("route execution is unavailable")
	}
	return binding.composition.Materializer.Accept(ctx, durablestore.ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, Reservations: route.Execution.Reservations, ExpiresAt: binding.now().Add(binding.cap.OperationRetention)})
}

func (binding *productionPhaseBinding) journalGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult) (durablestore.JournalReceipt, error) {
	for _, event := range reservation.Events {
		if _, err := binding.composition.Journal.AppendReservation(ctx, event); err != nil {
			return durablestore.JournalReceipt{}, err
		}
	}
	return durablestore.JournalReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID}, nil
}

type productionDispatchObserver struct {
	binding   *productionPhaseBinding
	operation admission.Operation
	candidate routing.Candidate
	marked    bool
}

func (observer *productionDispatchObserver) BeforePossibleWrite(ctx context.Context) error {
	if observer.marked {
		return nil
	}
	err := observer.binding.composition.Operations.MarkDispatching(ctx, admission.DispatchRequest{OperationID: observer.operation.ID, DispatchToken: observer.operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: observer.candidate.RouteID, EndpointID: observer.candidate.EndpointID, Provider: observer.candidate.Provider, ResolvedModel: observer.candidate.Model, ServiceClass: string(observer.candidate.AttemptedClass), AttemptNumber: 1}, LeaseUntil: observer.binding.now().Add(time.Minute)})
	if err == nil {
		observer.marked = true
	}
	return err
}
func (*productionDispatchObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	return nil
}
func (*productionDispatchObserver) OnProgress(context.Context, provider.Progress) {}

func (binding *productionPhaseBinding) dispatchGenerate(ctx context.Context, _ llm.GenerateRequestV1, _ durablestore.GenerateReplay, route durablestore.RoutePlan, _ durablestore.JournalReceipt) (durablestore.DispatchResult, error) {
	if route.Execution == nil {
		return durablestore.DispatchResult{}, errors.New("route execution is unavailable")
	}
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	if err != nil {
		return durablestore.DispatchResult{}, err
	}
	candidate := route.Execution.Candidate
	adapter, err := binding.cap.Adapters.Adapter(ctx, candidate)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, admission.NotDispatched, err)
	}
	query := provider.CapabilityQuery{EndpointID: candidate.EndpointID, Family: provider.Family(candidate.Family), Model: candidate.Model, ServiceClass: candidate.AttemptedClass}
	capability, err := adapter.Capabilities(ctx, query)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, admission.NotDispatched, err)
	}
	digest, err := llm.RequestDigest(route.Execution.Request)
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, admission.NotDispatched, err)
	}
	call, err := adapter.Compile(ctx, provider.CompileInput{Request: route.Execution.Request, Query: query, Capability: capability, Strict: route.Execution.Request.Portability != llm.PortabilityBestEffort, Metadata: provider.CallMetadata{SchemaDigest: digest, CapabilityVersion: candidate.CapabilityVersion, ProviderTier: candidate.ProviderTier}})
	if err != nil {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, admission.NotDispatched, err)
	}
	observer := &productionDispatchObserver{binding: binding, operation: operation, candidate: candidate}
	result, err := adapter.Invoke(ctx, call, observer)
	if err != nil {
		certainty := admission.Ambiguous
		var mapped *provider.Error
		if errors.As(err, &mapped) && (mapped.Dispatch == provider.DispatchNotDispatched || mapped.Dispatch == provider.DispatchRejected) {
			certainty = admission.Rejected
		}
		if !observer.marked {
			certainty = admission.NotDispatched
		}
		_ = binding.composition.Operations.Fail(context.WithoutCancel(ctx), admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: certainty, Attempt: admission.AttemptFacts{RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider, ResolvedModel: candidate.Model, Dispatch: certainty, AttemptNumber: 1}, Reason: "provider_dispatch_failed"})
		if finalErr := binding.finalizeFailedBudget(ctx, route, certainty); finalErr != nil {
			return durablestore.DispatchResult{}, fmt.Errorf("provider dispatch failed; finalize durable budget outcome: %w", finalErr)
		}
		return durablestore.DispatchResult{}, err
	}
	if !observer.marked {
		return durablestore.DispatchResult{}, binding.failProviderAttempt(ctx, route, operation, admission.Ambiguous, errors.New("provider adapter returned without marking possible write"))
	}
	if result.Response.Cost.ActualCostUSD == nil {
		usageCost, costErr := pricing.CostFromUsage(route.Execution.Price, pricing.Usage{InputTokens: result.Response.Usage.InputTokens, OutputTokens: result.Response.Usage.OutputTokens, ReasoningTokens: result.Response.Usage.ReasoningTokens, CacheReadTokens: result.Response.Usage.CacheReadTokens, CacheWriteTokens: result.Response.Usage.CacheWriteTokens})
		if costErr == nil {
			actual := usageCost.USD
			result.Response.Cost.Status = llm.CostStatusKnown
			result.Response.Cost.ActualCostUSD = &actual
			result.Response.Cost.Method = string(usageCost.Method)
			result.Response.Cost.CatalogVersion = usageCost.CatalogVersion
		} else {
			result.Response.Cost.Status = llm.CostStatusUnknown
			result.Response.Cost.Method = ""
			result.Response.Cost.CatalogVersion = ""
		}
	}
	result.Response.OperationID = string(route.OperationID)
	result.Response.OperationKey = route.Execution.Request.OperationKey
	return durablestore.DispatchResult{Response: result.Response}, nil
}

func (binding *productionPhaseBinding) failProviderAttempt(ctx context.Context, route durablestore.RoutePlan, operation admission.Operation, certainty admission.DispatchCertainty, cause error) error {
	if operation.State == admission.StateReserved {
		if err := binding.composition.Operations.MarkDispatching(context.WithoutCancel(ctx), admission.DispatchRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, Dispatch: certainty, AttemptNumber: 1}, LeaseUntil: binding.now()}); err != nil {
			return fmt.Errorf("mark failed provider attempt: %w", err)
		}
	}
	failErr := binding.composition.Operations.Fail(context.WithoutCancel(ctx), admission.FailRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Certainty: certainty, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, Dispatch: certainty, AttemptNumber: 1}, Reason: "provider_dispatch_failed"})
	budgetErr := binding.finalizeFailedBudget(ctx, route, certainty)
	if failErr != nil {
		return fmt.Errorf("record failed provider attempt: %w", failErr)
	}
	if budgetErr != nil {
		return fmt.Errorf("finalize durable budget outcome: %w", budgetErr)
	}
	return cause
}

func (binding *productionPhaseBinding) finalizeFailedBudget(ctx context.Context, route durablestore.RoutePlan, certainty admission.DispatchCertainty) error {
	if route.Execution == nil {
		return errors.New("route execution is unavailable")
	}
	reservation, err := binding.composition.Materializer.Accept(context.WithoutCancel(ctx), durablestore.ReserveRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, Reservations: route.Execution.Reservations, ExpiresAt: binding.now().Add(binding.cap.OperationRetention)})
	if err != nil {
		return err
	}
	zero := pricing.MustUSD("0")
	events := make([]budget.CompletionEvent, 0, len(reservation.Events))
	for index, reserved := range reservation.Events {
		event := budget.CompletionEvent{EventID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("llmtw/budget-failed/v1\x00%s\x00%s\x00%d", route.OperationID, reserved.EventID, index))).String(), GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: reserved.WindowID, BucketStart: reserved.BucketStart, ReservationRevision: reserved.ReservationRevision, OccurredAt: binding.now()}
		if certainty == admission.Accepted || certainty == admission.Ambiguous {
			event.Kind, event.CostStatus, event.UnknownReasonCode = budget.JournalRetainAmbiguous, budget.CostUnknown, "ambiguous_dispatch"
		} else {
			event.Kind, event.CostStatus, event.ActualCostUSD = budget.JournalRelease, budget.CostExact, &zero
			event.ReservedDecreaseUSD = reserved.AmountUSD
		}
		events = append(events, event)
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(context.WithoutCancel(ctx), event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(context.WithoutCancel(ctx), durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}

func (binding *productionPhaseBinding) finalizeGenerate(ctx context.Context, request llm.GenerateRequestV1, replay durablestore.GenerateReplay, route durablestore.RoutePlan, reservation durablestore.ReserveResult, dispatch durablestore.DispatchResult) (durablestore.GenerateFinalization, error) {
	scopeID, err := binding.cap.ResolveScope(ctx, request.Context)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	checkpoint, handle, err := binding.publishCheckpoint(ctx, scopeID, request.Context.Tenant, request.Parent, route.OperationID, state.CheckpointGeneration, replay, request.Append, request.SettingsPatch, dispatch.Response.Output)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	dispatch.Response.Continuation = &llm.Continuation{Handle: handle}
	resultRef, err := binding.composition.Results.Put(ctx, string(route.OperationID), dispatch.Response)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	actualMicro := pricing.MicroUSD(0)
	if dispatch.Response.Cost.ActualCostUSD != nil {
		actualMicro, err = pricing.CeilMicroFromUSD(*dispatch.Response.Cost.ActualCostUSD)
		if err != nil {
			return durablestore.GenerateFinalization{}, err
		}
	}
	operation, err := binding.composition.Operations.Get(ctx, string(route.OperationID))
	actualUSD, unknownReason := operationCompletionCost(dispatch.Response.Cost)
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	if err := binding.composition.Operations.Complete(ctx, admission.CompleteRequest{OperationID: operation.ID, DispatchToken: operation.DispatchToken, Actual: actualMicro, ActualCostUSD: actualUSD, ResultRef: &resultRef, Attempt: admission.AttemptFacts{RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider, ResolvedModel: route.Model, Dispatch: admission.Accepted, AttemptNumber: 1}, CostStatus: costStatus(dispatch.Response.Cost), CostMethod: dispatch.Response.Cost.Method, UnknownReason: unknownReason}); err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	mapped, err := generateResponseFromNormalized(dispatch.Response, request.Parent, "miss_populated")
	if err != nil {
		return durablestore.GenerateFinalization{}, err
	}
	mapped.Checkpoint.Depth = checkpoint.Depth
	_ = reservation
	return durablestore.GenerateFinalization{Response: mapped}, nil
}

func costStatus(cost llm.Cost) string {
	if cost.ActualCostUSD == nil {
		return "unknown"
	}
	return "exact"
}

func operationCompletionCost(cost llm.Cost) (pricing.USD, string) {
	if cost.ActualCostUSD == nil {
		return pricing.USD{}, "provider_did_not_report_cost"
	}
	return *cost.ActualCostUSD, ""
}

func generateResponseFromNormalized(response llm.Response, parent *llm.CheckpointHandle, cacheDisposition string) (llm.GenerateResponseV1, error) {
	if response.Continuation == nil || strings.TrimSpace(response.Continuation.Handle) == "" {
		return llm.GenerateResponseV1{}, errors.New("stored response has no checkpoint handle")
	}
	handle := llm.CheckpointHandle(response.Continuation.Handle)
	cost := llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"}
	if response.Cost.ActualCostUSD != nil {
		value := response.Cost.ActualCostUSD.String()
		cost = llm.CostV1{Status: "exact", ActualCostUSD: &value, Method: response.Cost.Method, CatalogVersion: response.Cost.CatalogVersion}
	}
	return llm.GenerateResponseV1{APIVersion: llm.APIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID, Status: response.Status, Output: response.Output, Checkpoint: llm.CheckpointMetadata{Handle: handle, Parent: parent, Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: cacheDisposition}, Route: &response.Route, Usage: &response.Usage, Cost: cost, Diagnostics: response.Diagnostics}, nil
}

func (binding *productionPhaseBinding) finalizeGenerateCache(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay, durablestore.CacheDecision) (durablestore.GenerateFinalization, error) {
	return durablestore.GenerateFinalization{}, errors.New("cache finalization cannot run when durable cache is disabled")
}

func (binding *productionPhaseBinding) reconcileGenerate(ctx context.Context, _ llm.GenerateRequestV1, route durablestore.RoutePlan, reservation durablestore.ReserveResult, finalization durablestore.GenerateFinalization) error {
	events, err := completionEvents(route, reservation, finalization.Response.Cost, binding.now())
	if err != nil {
		return err
	}
	for _, event := range events {
		if _, err := binding.composition.Journal.AppendCompletion(ctx, event); err != nil {
			return err
		}
	}
	return binding.composition.Materializer.Reconcile(ctx, durablestore.ReconcileRequest{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID, Events: events})
}

func completionEvents(route durablestore.RoutePlan, reservation durablestore.ReserveResult, cost llm.CostV1, now time.Time) ([]budget.CompletionEvent, error) {
	if route.Execution == nil {
		return nil, errors.New("route execution is unavailable")
	}
	var actual *pricing.USD
	kind := budget.JournalFinalizeUnknown
	status := budget.CostUnknown
	unknown := cost.UnknownReason
	if cost.ActualCostUSD != nil {
		parsed, err := pricing.ParseUSD(*cost.ActualCostUSD)
		if err != nil {
			return nil, err
		}
		actual = &parsed
		kind = budget.JournalFinalizeExact
		status = budget.CostExact
		unknown = ""
	}
	events := make([]budget.CompletionEvent, 0, len(reservation.Events))
	for index, reserved := range reservation.Events {
		increase := reserved.AmountUSD
		if actual != nil {
			increase = *actual
		}
		eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("llmtw/budget-complete/v1\x00%s\x00%s\x00%d", route.OperationID, reserved.EventID, index))).String()
		events = append(events, budget.CompletionEvent{EventID: eventID, GenerationID: string(route.GenerationID), OperationID: string(route.OperationID), WindowID: reserved.WindowID, BucketStart: reserved.BucketStart, ReservationRevision: reserved.ReservationRevision, Kind: kind, ReservedDecreaseUSD: reserved.AmountUSD, AccountedIncreaseUSD: increase, ActualCostUSD: actual, CostStatus: status, UnknownReasonCode: unknown, OccurredAt: now})
	}
	return events, nil
}

func (binding *productionPhaseBinding) publishCheckpoint(ctx context.Context, scopeID, tenant string, parentHandle *llm.CheckpointHandle, operationID durablestore.OperationID, kind state.CheckpointKind, replay durablestore.GenerateReplay, delta []llm.Item, patchWire llm.SettingsPatchV1, output []llm.Item) (state.DurableCheckpoint, string, error) {
	parsedScope, err := uuid.Parse(scopeID)
	if err != nil {
		return state.DurableCheckpoint{}, "", errors.New("invalid PostgreSQL scope identity")
	}
	checkpointID := state.CheckpointID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("llmtw/checkpoint/v1\x00"+string(operationID))).String())
	handle, keyID, publicMAC, err := binding.cap.Checkpoints.IssueHandle(scopeID, checkpointID)
	if err != nil {
		return state.DurableCheckpoint{}, "", err
	}
	var patch state.SettingsPatch
	if kind == state.CheckpointCompaction {
		patch = state.SettingsPatchForModel(replay.State.Settings)
	} else {
		patch, err = state.SettingsPatchFromV1(patchWire)
		if err != nil {
			return state.DurableCheckpoint{}, "", err
		}
	}
	deltaBytes, err := binding.codec.EncodeDelta(delta)
	if err != nil {
		return state.DurableCheckpoint{}, "", err
	}
	responseBytes, err := binding.codec.EncodeResponse(output)
	if err != nil {
		return state.DurableCheckpoint{}, "", err
	}
	patchBytes, err := binding.codec.EncodeSettingsPatch(patch)
	if err != nil {
		return state.DurableCheckpoint{}, "", err
	}
	expires := binding.now().Add(binding.cap.CheckpointRetention)
	refs := make([]state.CheckpointBlobReference, 0, 3)
	for index, value := range []struct {
		kind string
		data []byte
	}{{"checkpoint-delta", deltaBytes}, {"checkpoint-response", responseBytes}, {"checkpoint-settings", patchBytes}} {
		ref, err := binding.putCheckpointBlob(ctx, parsedScope, tenant, value.kind, value.data, expires)
		if err != nil {
			return state.DurableCheckpoint{}, "", err
		}
		_ = index
		refs = append(refs, ref)
	}
	var parentID *state.CheckpointID
	depth := int32(0)
	if parentHandle != nil {
		resolved, err := binding.cap.Checkpoints.VerifyHandle(ctx, scopeID, string(*parentHandle))
		if err != nil {
			return state.DurableCheckpoint{}, "", err
		}
		parentID = &resolved
		depth = replay.State.Depth + 1
	}
	var materializedSnapshot *state.CheckpointBlobReference
	if kind == state.CheckpointCompaction {
		snapshotLineage := append([]state.Handle(nil), replay.State.Lineage...)
		snapshotLineage = append(snapshotLineage, state.Handle(checkpointID))
		snapshot := state.NewCheckpointSnapshot(state.MaterializedState{Items: append([]llm.Item(nil), output...), Settings: replay.State.Settings, Depth: depth, Lineage: snapshotLineage})
		encoded, err := binding.codec.EncodeSnapshot(*snapshot)
		if err != nil {
			return state.DurableCheckpoint{}, "", err
		}
		ref, err := binding.putCheckpointBlob(ctx, parsedScope, tenant, "checkpoint-materialized-snapshot", encoded, expires)
		if err != nil {
			return state.DurableCheckpoint{}, "", err
		}
		materializedSnapshot = &ref
	}
	lineageBytes, _ := json.Marshal(replay.State.Lineage)
	settingsBytes, _ := json.Marshal(replay.State.Settings)
	transcript := append(append(append([]llm.Item(nil), replay.State.Items...), delta...), output...)
	if kind == state.CheckpointCompaction {
		transcript = append([]llm.Item(nil), output...)
	}
	frontier, err := state.ValidateTranscript(transcript)
	if err != nil {
		return state.DurableCheckpoint{}, "", err
	}
	frontierBytes, _ := json.Marshal(frontier)
	now := binding.now()
	checkpoint := state.DurableCheckpoint{ID: checkpointID, ScopeID: scopeID, PublicIDHMAC: publicMAC, HandleKeyID: keyID, ParentID: parentID, Kind: kind, Depth: depth, OriginOperationID: state.OperationID(operationID), DeltaBlob: refs[0], ResponseBlob: refs[1], SettingsPatchBlob: refs[2], MaterializedSnapshotBlob: materializedSnapshot, CanonicalLineageDigest: sha256.Sum256(lineageBytes), MaterializedSettingsDigest: sha256.Sum256(settingsBytes), ToolFrontierDigest: sha256.Sum256(frontierBytes), SchemaVersion: 1, CompilerEpoch: checkpointCompilerEpoch, CreatedAt: now, ExpiresAt: expires}
	if kind == state.CheckpointCompaction {
		checkpoint.CompactedThroughID = parentID
	}
	err = state.WithCheckpointUnitOfWork(ctx, binding.cap.Checkpoints.Repository, func(ctx context.Context, unit state.CheckpointUnitOfWork) error {
		return unit.PutCheckpoint(ctx, state.CheckpointWrite{Checkpoint: checkpoint})
	})
	return checkpoint, handle, err
}

func (binding *productionPhaseBinding) putCheckpointBlob(ctx context.Context, scopeID uuid.UUID, tenant, payloadKind string, data []byte, expires time.Time) (state.CheckpointBlobReference, error) {
	ref, err := binding.cap.Checkpoints.BlobStore.Put(ctx, blobstore.PutRequest{Tenant: tenant, MediaType: "application/json", Data: data, ExpiresAt: expires})
	if err != nil {
		return state.CheckpointBlobReference{}, err
	}
	digestBytes, err := hex.DecodeString(ref.Digest)
	if err != nil || len(digestBytes) != sha256.Size {
		return state.CheckpointBlobReference{}, errors.New("object store returned invalid digest")
	}
	var digest [32]byte
	copy(digest[:], digestBytes)
	expiresAt := ref.ExpiresAt
	record, err := binding.cap.Checkpoints.BlobRepository.PutLocator(ctx, scopeID, payloadKind, postgresstore.BlobMetadata{StoreID: ref.Store, Digest: digest, ByteLength: ref.ByteLength, MediaType: ref.MediaType, ExpiresAt: &expiresAt}, []byte(ref.Locator))
	if err != nil {
		return state.CheckpointBlobReference{}, err
	}
	return state.CheckpointBlobReference{ID: state.BlobID(record.BlobID.String()), Digest: record.Digest, ByteLength: record.ByteLength, MediaType: record.MediaType}, nil
}

func (binding *productionPhaseBinding) now() time.Time {
	if binding.cap.Clock != nil {
		return binding.cap.Clock().UTC()
	}
	return time.Now().UTC()
}

var _ provider.Observer = (*productionDispatchObserver)(nil)
var _ engine.AdapterRegistry = (engine.AdapterMap)(nil)
