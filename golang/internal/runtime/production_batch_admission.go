package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func (binding *productionPhaseBinding) reserveBatch(ctx context.Context, request llm.ReserveBatchRequestV1) (llm.ReserveBatchResponseV1, error) {
	return binding.reserveBatchMaterialized(ctx, request, true)
}

// Planning adapters deliberately bypass content-addressed recovery: their direct
// request is synthetic, not the external escrow or allocation identity.
func (binding *productionPhaseBinding) reserveBatchMaterialized(ctx context.Context, request llm.ReserveBatchRequestV1, recover bool) (llm.ReserveBatchResponseV1, error) {
	if binding == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("reserve batch runtime is unavailable")
	}
	if len(request.Templates) > 0 {
		return binding.reserveBatchEscrow(ctx, request)
	}
	materializer, ok := binding.composition.Materializer.(durablestore.BatchBudgetMaterializer)
	if !ok || materializer == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("atomic batch budget materializer is unavailable")
	}
	if len(binding.cap.GrantHMACKey) < 32 || binding.cap.GrantKeyID == "" {
		return llm.ReserveBatchResponseV1{}, errors.New("reserve batch grant signing is unavailable")
	}
	if binding.cap.ResolveScope == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("reserve batch scope resolver is unavailable")
	}
	if _, err := binding.cap.ResolveScope(ctx, request.Context); err != nil {
		return llm.ReserveBatchResponseV1{}, fmt.Errorf("resolve reserve batch customer scope: %w", err)
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	contentDigest, err := decodeSHA256(requestSHA)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	grantKeyDigest := sha256.Sum256(binding.cap.GrantHMACKey)
	grantKeySHA := hex.EncodeToString(grantKeyDigest[:])
	// The batch ID is the canonical request digest. This makes the Redis batch
	// lookup content-addressed and prevents a caller-selected key from crossing
	// customer or tenant scope.
	batchID := requestSHA
	batchSHA := digestCanonical(map[string]any{
		"schema": "llmtw/reserve-batch-identity/v1", "batch_id": batchID,
		"request_sha256": requestSHA, "customer_id": request.CustomerID,
		"run_id": request.RunID, "budget_id": request.BudgetID,
	})
	response := llm.ReserveBatchResponseV1{
		APIVersion: llm.ReserveBatchAPIVersion, BatchID: batchID,
		RequestSHA256: requestSHA, BatchSHA256: batchSHA,
		Status: llm.ReserveBatchStatusDenied, Grants: []llm.ReserveBatchGrantV1{},
	}
	if recover {
		materialization, found, err := binding.loadBatchMaterialization(ctx, contentDigest)
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		if found {
			return binding.batchMaterializationResponse(request, batchID, batchSHA, requestSHA, materialization)
		}
	}

	snapshot, err := binding.cap.Snapshot.Current(ctx)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	if snapshot.Prices == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("pricing resolver is unavailable")
	}
	now := binding.now()
	if binding.cap.ReservationLease <= 0 {
		return llm.ReserveBatchResponseV1{}, errors.New("reserve batch lease is unavailable")
	}
	expiresAt := now.Add(binding.cap.ReservationLease).UTC()
	reserveRequests := make([]durablestore.ReserveRequest, len(request.Operations))
	var aggregate int64
	for index, descriptor := range request.Operations {
		normalized, err := llm.NormalizeRequest(llm.Request{
			APIVersion: llm.APIVersion, OperationKey: descriptor.OperationKey,
			Context: request.Context, Model: descriptor.Model,
			ServiceClass: descriptor.ServiceClass, ServiceClassFallbacks: append([]llm.ServiceClass(nil), descriptor.ServiceClassFallbacks...),
		})
		if err != nil {
			return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d: %w", index, err)
		}
		plan, err := binding.cap.Planner.Plan(ctx, routing.Input{Request: normalized, Catalog: snapshot.Routes, Health: snapshot.Health, Now: now})
		if err != nil {
			return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d: %w", index, err)
		}
		type pricedRoute struct {
			candidate routing.Candidate
			quote     pricing.Quote
			cost      pricing.USD
			matches   []budget.MatchedWindow
		}
		priced := make([]pricedRoute, 0, len(plan.Candidates))
		for _, candidate := range plan.Candidates {
			matches := budget.MatchPolicies(snapshot.BudgetPolicies, budget.ContextFor(normalized, candidate, snapshot.Environment))
			if snapshot.RequireBudgetMatch && len(matches) == 0 {
				continue
			}
			quote, quoteErr := snapshot.Prices.Resolve(pricing.Query{Provider: candidate.Provider, Family: candidate.Family, EndpointID: candidate.EndpointID, Region: candidate.Region, Model: candidate.Model, ProviderTier: candidate.ProviderTier, At: now})
			if quoteErr != nil {
				return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d route %q has no immutable price: %w", index, candidate.RouteID, quoteErr)
			}
			if quote.CatalogVersion != request.PricingGenerationID || quote.CatalogDigest != request.PricingManifestSHA256 {
				return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch pricing snapshot does not match route %q", candidate.RouteID)
			}
			cost, priceErr := priceDescriptorMaximum(descriptor, quote.Entry)
			if priceErr != nil {
				return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d route %q: %w", index, candidate.RouteID, priceErr)
			}
			priced = append(priced, pricedRoute{candidate: candidate, quote: quote, cost: cost, matches: matches})
		}
		if len(priced) == 0 {
			return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d has no eligible priced route", index)
		}
		selected := priced[0]
		maximum := selected.cost
		for _, route := range priced[1:] {
			if route.cost.Cmp(maximum) > 0 {
				maximum = route.cost
			}
		}
		maximumMicro, err := pricing.CeilMicroFromUSD(maximum)
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		// Price exactly, then reserve the public signed microunit ceiling in
		// both logical accounting and every enforcement window.
		maximum, err = pricing.USDFromMicro(maximumMicro)
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		if int64(maximumMicro) > math.MaxInt64-aggregate {
			return llm.ReserveBatchResponseV1{}, errors.New("reserve batch aggregate cost overflows")
		}
		aggregate += int64(maximumMicro)
		reservations := make([]admission.WindowReservation, 0, len(selected.matches))
		for _, match := range selected.matches {
			_, bucket := match.Window.Range(now)
			reservations = append(reservations, admission.WindowReservation{
				PolicyID: match.PolicyID, WindowID: match.Window.ID, Bucket: bucket,
				Amount: maximumMicro, Limit: match.Window.Limit, AmountUSD: maximum, LimitUSD: match.Window.LimitUSD,
				BucketNanos: match.Window.Bucket.Nanoseconds(), DurationNanos: match.Window.Duration.Nanoseconds(),
			})
		}
		if len(reservations) == 0 {
			return llm.ReserveBatchResponseV1{}, fmt.Errorf("reserve batch operation %d has no matched budget window", index)
		}
		operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "generate", llm.APIVersion, descriptor.OperationKey)
		entryVersion := selected.quote.Entry.Version
		if entryVersion == "" {
			entryVersion = selected.quote.CatalogVersion
		}
		reserveRequests[index] = durablestore.ReserveRequest{
			OperationID: operationID, GenerationID: binding.cap.BudgetGenerationID,
			IncarnationID: binding.cap.BudgetIncarnationID, Reservations: reservations, LogicalCostUSD: maximum,
			OccurredAt: now.UTC(), ExpiresAt: expiresAt,
			Route: durablestore.DispatchRouteFacts{RouteID: selected.candidate.RouteID, EndpointID: selected.candidate.EndpointID, Provider: selected.candidate.Provider, ResolvedModel: selected.candidate.Model, ServiceClass: string(selected.candidate.Class()), PriceVersion: entryVersion},
		}
		operationSHA, err := descriptor.OperationSHA256()
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		reserveRequests[index].Bounds = durablestore.ReservationBounds{
			OperationSHA256: operationSHA, Model: descriptor.Model,
			GrantKeyID: binding.cap.GrantKeyID, GrantKeySHA256: grantKeySHA,
			MaxInputTokens: descriptor.MaxInputTokens, MaxOutputTokens: descriptor.MaxOutputTokens,
			MaxReasoningTokens: descriptor.MaxReasoningTokens,
			MaxCacheReadTokens: descriptor.MaxCacheReadTokens, MaxCacheWriteTokens: descriptor.MaxCacheWriteTokens,
		}
	}
	if aggregate > request.RemainingMaxCostMicrounits {
		return response, nil
	}
	results, err := materializer.AcceptBatch(ctx, contentDigest, reserveRequests)
	if err != nil {
		if recover && errors.Is(err, durablestore.ErrBatchMaterializationConflict) {
			materialization, found, loadErr := binding.loadBatchMaterialization(ctx, contentDigest)
			if loadErr != nil {
				return llm.ReserveBatchResponseV1{}, loadErr
			}
			if found {
				return binding.batchMaterializationResponse(request, batchID, batchSHA, requestSHA, materialization)
			}
		}
		return llm.ReserveBatchResponseV1{}, err
	}
	return binding.batchMaterializationResponse(request, batchID, batchSHA, requestSHA, durablestore.BatchMaterialization{Requests: reserveRequests, Results: results})
}

func (binding *productionPhaseBinding) loadBatchMaterialization(ctx context.Context, digest [32]byte) (durablestore.BatchMaterialization, bool, error) {
	reader, ok := binding.composition.Materializer.(durablestore.BatchMaterializationReader)
	if !ok || reader == nil {
		return durablestore.BatchMaterialization{}, false, errors.New("immutable batch materialization reader is unavailable")
	}
	materialization, found, err := reader.LoadBatchMaterialization(ctx, digest)
	if err != nil || !found {
		return materialization, found, err
	}
	if err := durablestore.ValidateBatchReserveRequest(digest, materialization.Requests); err != nil {
		return durablestore.BatchMaterialization{}, false, err
	}
	if err := durablestore.ValidateBatchReserveResult(materialization.Requests, materialization.Results); err != nil {
		return durablestore.BatchMaterialization{}, false, err
	}
	for _, request := range materialization.Requests {
		if request.GenerationID != binding.cap.BudgetGenerationID || request.IncarnationID != binding.cap.BudgetIncarnationID {
			return durablestore.BatchMaterialization{}, false, errors.New("batch materialization budget identity does not match active snapshot")
		}
	}
	return materialization, true, nil
}

func (binding *productionPhaseBinding) batchMaterializationResponse(request llm.ReserveBatchRequestV1, batchID, batchSHA, requestSHA string, materialization durablestore.BatchMaterialization) (llm.ReserveBatchResponseV1, error) {
	response := llm.ReserveBatchResponseV1{APIVersion: llm.ReserveBatchAPIVersion, BatchID: batchID, RequestSHA256: requestSHA, BatchSHA256: batchSHA, Status: llm.ReserveBatchStatusDenied, Grants: []llm.ReserveBatchGrantV1{}}
	if len(materialization.Requests) == 0 || len(materialization.Requests) != len(request.Operations) {
		return llm.ReserveBatchResponseV1{}, errors.New("batch materialization operation count does not match request")
	}
	if err := durablestore.ValidateBatchReserveResult(materialization.Requests, materialization.Results); err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	grants := make([]llm.ReserveBatchGrantV1, len(request.Operations))
	var aggregate int64
	grantKeyDigest := sha256.Sum256(binding.cap.GrantHMACKey)
	grantKeySHA := hex.EncodeToString(grantKeyDigest[:])
	for index, descriptor := range request.Operations {
		reserved := materialization.Requests[index]
		if reserved.Bounds.GrantKeyID != binding.cap.GrantKeyID || reserved.Bounds.GrantKeySHA256 != grantKeySHA {
			return llm.ReserveBatchResponseV1{}, errors.New("batch materialization signing key does not match active snapshot")
		}
		operationSHA, err := descriptor.OperationSHA256()
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		bounds := durablestore.ReservationBounds{OperationSHA256: operationSHA, Model: descriptor.Model, MaxInputTokens: descriptor.MaxInputTokens, MaxOutputTokens: descriptor.MaxOutputTokens, MaxReasoningTokens: descriptor.MaxReasoningTokens, MaxCacheReadTokens: descriptor.MaxCacheReadTokens, MaxCacheWriteTokens: descriptor.MaxCacheWriteTokens}
		operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "generate", llm.APIVersion, descriptor.OperationKey)
		descriptorBounds := reserved.Bounds
		descriptorBounds.GrantKeyID = ""
		descriptorBounds.GrantKeySHA256 = ""
		if reserved.OperationID != operationID || descriptorBounds != bounds || reserved.GenerationID != binding.cap.BudgetGenerationID || reserved.IncarnationID != binding.cap.BudgetIncarnationID {
			return llm.ReserveBatchResponseV1{}, errors.New("batch materialization operation identity does not match request")
		}
		maximumMicro, err := pricing.CeilMicroFromUSD(reserved.LogicalCostUSD)
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		if int64(maximumMicro) > math.MaxInt64-aggregate {
			return llm.ReserveBatchResponseV1{}, errors.New("reserve batch aggregate cost overflows")
		}
		aggregate += int64(maximumMicro)
		grantID := string(batchResourceIdentity(rawScope(request.Context), "reserve-batch-grant:"+batchID, descriptor.OperationKey))
		grantSHA := digestCanonical(map[string]any{
			"schema": "llmtw/reserve-batch-grant/v1", "batch_id": batchID,
			"operation_key": descriptor.OperationKey, "operation_sha256": operationSHA,
			"operation_id": string(operationID), "grant_id": grantID,
			"max_cost_microunits":        int64(maximumMicro),
			"reservation_generation_id":  string(reserved.GenerationID),
			"reservation_incarnation_id": string(reserved.IncarnationID),
			"reservation_expires_at":     reserved.ExpiresAt.Format(time.RFC3339Nano),
		})
		grants[index] = llm.ReserveBatchGrantV1{
			OperationKey: descriptor.OperationKey, OperationSHA256: operationSHA,
			OperationID: string(operationID), GrantID: grantID, GrantSHA256: grantSHA,
			GrantKeyID: binding.cap.GrantKeyID, MaxCostMicrounits: int64(maximumMicro),
			ReservationGenerationID: string(reserved.GenerationID), ReservationIncarnationID: string(reserved.IncarnationID), ReservationExpiresAt: reserved.ExpiresAt,
		}
		grants[index].GrantHMACSHA256 = binding.signGrant(request, batchID, batchSHA, requestSHA, grants[index])
	}
	if !materialization.Results[0].Accepted {
		return response, nil
	}
	if aggregate > request.RemainingMaxCostMicrounits {
		return llm.ReserveBatchResponseV1{}, errors.New("batch materialization exceeds request cost ceiling")
	}
	response.Status = llm.ReserveBatchStatusReserved
	response.ReservedCostMicrounits = aggregate
	response.Grants = grants
	return response, nil
}

type captureBatchMaterializer struct {
	durablestore.BudgetMaterializer
	requests []durablestore.ReserveRequest
}

func (materializer *captureBatchMaterializer) AcceptBatch(_ context.Context, _ [32]byte, requests []durablestore.ReserveRequest) ([]durablestore.ReserveResult, error) {
	materializer.requests = append([]durablestore.ReserveRequest(nil), requests...)
	results := make([]durablestore.ReserveResult, len(requests))
	for index, request := range requests {
		planned, err := durablestore.PlannedReserveResult(request)
		if err != nil {
			return nil, err
		}
		results[index] = planned
	}
	return results, nil
}

func (binding *productionPhaseBinding) reserveBatchEscrow(ctx context.Context, request llm.ReserveBatchRequestV1) (llm.ReserveBatchResponseV1, error) {
	escrow, ok := binding.composition.Materializer.(durablestore.BatchEscrowMaterializer)
	if !ok || escrow == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("atomic batch escrow materializer is unavailable")
	}
	if binding.cap.ResolveScope == nil {
		return llm.ReserveBatchResponseV1{}, errors.New("reserve batch scope resolver is unavailable")
	}
	if _, err := binding.cap.ResolveScope(ctx, request.Context); err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	batchID := requestSHA
	batchSHA := digestCanonical(map[string]any{"schema": "llmtw/reserve-batch-identity/v1", "batch_id": batchID, "request_sha256": requestSHA, "customer_id": request.CustomerID, "run_id": request.RunID, "budget_id": request.BudgetID})
	denied := llm.ReserveBatchResponseV1{APIVersion: llm.ReserveBatchAPIVersion, BatchID: batchID, RequestSHA256: requestSHA, BatchSHA256: batchSHA, Status: llm.ReserveBatchStatusDenied, Grants: []llm.ReserveBatchGrantV1{}}
	escrowSHA := digestCanonical(map[string]any{"schema": "llmtw/reserve-batch-escrow/v1", "batch_id": batchID, "batch_sha256": batchSHA, "phase_key": request.PhaseKey, "templates": request.Templates})
	escrowDigest, err := decodeSHA256(escrowSHA)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	materialization, found, err := binding.loadBatchMaterialization(ctx, escrowDigest)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	if found {
		return binding.escrowMaterializationResponse(request, denied, escrowSHA, materialization)
	}
	operations := make([]llm.ReserveBatchOperationV1, 0)
	for _, template := range request.Templates {
		for index := int32(0); index < template.Count; index++ {
			operations = append(operations, llm.ReserveBatchOperationV1{OperationKey: fmt.Sprintf("escrow/%s/%d", template.TemplateKey, index), Model: template.Model, ServiceClass: template.ServiceClass, ServiceClassFallbacks: append([]llm.ServiceClass(nil), template.ServiceClassFallbacks...), MaxInputTokens: template.MaxInputTokens, MaxOutputTokens: template.MaxOutputTokens, MaxReasoningTokens: template.MaxReasoningTokens, MaxCacheReadTokens: template.MaxCacheReadTokens, MaxCacheWriteTokens: template.MaxCacheWriteTokens})
		}
	}
	capture := &captureBatchMaterializer{BudgetMaterializer: binding.composition.Materializer}
	planningBinding := *binding
	planningBinding.composition.Materializer = capture
	planningRequest := llm.ReserveBatchRequestV1{APIVersion: llm.ReserveBatchAPIVersion, Context: request.Context, CustomerID: request.CustomerID, RunID: request.RunID, BudgetID: request.BudgetID, BatchKey: digestCanonical(map[string]any{"batch_id": batchID, "purpose": "escrow-plan"}), PricingGenerationID: request.PricingGenerationID, PricingManifestSHA256: request.PricingManifestSHA256, RemainingMaxCostMicrounits: request.RemainingMaxCostMicrounits, Operations: operations}
	planned, err := planningBinding.reserveBatchMaterialized(ctx, planningRequest, false)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	if planned.Status == llm.ReserveBatchStatusDenied {
		return denied, nil
	}
	type reservationKey struct {
		policy, window string
		bucket         int64
	}
	aggregated := make(map[reservationKey]admission.WindowReservation)
	logicalCost := pricing.MustUSD("0")
	for _, child := range capture.requests {
		logicalCost, err = logicalCost.Add(child.LogicalCostUSD)
		if err != nil {
			return llm.ReserveBatchResponseV1{}, err
		}
		for _, reservation := range child.Reservations {
			key := reservationKey{reservation.PolicyID, reservation.WindowID, reservation.Bucket}
			current, exists := aggregated[key]
			if !exists {
				current = reservation
				current.Amount = 0
				current.AmountUSD = pricing.MustUSD("0")
			} else if current.LimitUSD.Cmp(reservation.LimitUSD) != 0 || current.BucketNanos != reservation.BucketNanos || current.DurationNanos != reservation.DurationNanos {
				return llm.ReserveBatchResponseV1{}, errors.New("escrow operations resolved incompatible budget windows")
			}
			current.Amount, err = current.Amount.Add(reservation.Amount)
			if err != nil {
				return llm.ReserveBatchResponseV1{}, err
			}
			current.AmountUSD, err = current.AmountUSD.Add(reservation.AmountUSD)
			if err != nil {
				return llm.ReserveBatchResponseV1{}, err
			}
			aggregated[key] = current
		}
	}
	reservations := make([]admission.WindowReservation, 0, len(aggregated))
	for _, reservation := range aggregated {
		reservations = append(reservations, reservation)
	}
	sort.Slice(reservations, func(i, j int) bool {
		if reservations[i].PolicyID != reservations[j].PolicyID {
			return reservations[i].PolicyID < reservations[j].PolicyID
		}
		if reservations[i].WindowID != reservations[j].WindowID {
			return reservations[i].WindowID < reservations[j].WindowID
		}
		return reservations[i].Bucket < reservations[j].Bucket
	})
	if len(reservations) == 0 {
		return llm.ReserveBatchResponseV1{}, errors.New("escrow has no budget windows")
	}
	escrowID := batchResourceIdentity(rawScope(request.Context), "reserve-batch-escrow:"+batchID, request.PhaseKey)
	now := binding.now().UTC()
	if !request.PhaseExpiresAt.After(now) || binding.cap.OperationRetention <= 0 ||
		request.PhaseExpiresAt.After(now.Add(binding.cap.OperationRetention)) {
		return llm.ReserveBatchResponseV1{}, errors.New("phase escrow expiry is outside the configured durable bound")
	}
	escrowRequest := durablestore.ReserveRequest{OperationID: escrowID, GenerationID: binding.cap.BudgetGenerationID, IncarnationID: binding.cap.BudgetIncarnationID, Reservations: reservations, LogicalCostUSD: logicalCost, OccurredAt: now, ExpiresAt: request.PhaseExpiresAt.UTC(), Route: durablestore.DispatchRouteFacts{RouteID: batchSHA, EndpointID: request.PricingManifestSHA256, Provider: "internal", ResolvedModel: request.PricingGenerationID, ServiceClass: "escrow", PriceVersion: "v1"}, Bounds: durablestore.ReservationBounds{OperationSHA256: escrowScopeSHA(request.CustomerID, request.RunID, request.BudgetID, request.PhaseKey), Model: request.PhaseKey}}
	result, err := escrow.AcceptBatchEscrow(ctx, escrowDigest, escrowRequest)
	if err != nil {
		if errors.Is(err, durablestore.ErrBatchMaterializationConflict) {
			materialization, found, loadErr := binding.loadBatchMaterialization(ctx, escrowDigest)
			if loadErr != nil {
				return llm.ReserveBatchResponseV1{}, loadErr
			}
			if found {
				return binding.escrowMaterializationResponse(request, denied, escrowSHA, materialization)
			}
		}
		return llm.ReserveBatchResponseV1{}, err
	}
	return binding.escrowMaterializationResponse(request, denied, escrowSHA, durablestore.BatchMaterialization{Requests: []durablestore.ReserveRequest{escrowRequest}, Results: []durablestore.ReserveResult{result}})
}

func (binding *productionPhaseBinding) escrowMaterializationResponse(request llm.ReserveBatchRequestV1, response llm.ReserveBatchResponseV1, escrowSHA string, materialization durablestore.BatchMaterialization) (llm.ReserveBatchResponseV1, error) {
	if len(materialization.Requests) != 1 {
		return llm.ReserveBatchResponseV1{}, errors.New("escrow materialization operation count is invalid")
	}
	if err := durablestore.ValidateBatchReserveResult(materialization.Requests, materialization.Results); err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	reserved := materialization.Requests[0]
	escrowID := batchResourceIdentity(rawScope(request.Context), "reserve-batch-escrow:"+response.BatchID, request.PhaseKey)
	route := durablestore.DispatchRouteFacts{RouteID: response.BatchSHA256, EndpointID: request.PricingManifestSHA256, Provider: "internal", ResolvedModel: request.PricingGenerationID, ServiceClass: "escrow", PriceVersion: "v1"}
	bounds := durablestore.ReservationBounds{OperationSHA256: escrowScopeSHA(request.CustomerID, request.RunID, request.BudgetID, request.PhaseKey), Model: request.PhaseKey}
	if reserved.OperationID != escrowID || reserved.Route != route || reserved.Bounds != bounds || !reserved.ExpiresAt.Equal(request.PhaseExpiresAt) {
		return llm.ReserveBatchResponseV1{}, errors.New("escrow materialization identity does not match request")
	}
	if !materialization.Results[0].Accepted {
		return response, nil
	}
	micro, err := pricing.CeilMicroFromUSD(reserved.LogicalCostUSD)
	if err != nil {
		return llm.ReserveBatchResponseV1{}, err
	}
	response.Status = llm.ReserveBatchStatusEscrowed
	response.EscrowID = string(escrowID)
	response.EscrowSHA256 = escrowSHA
	response.ReservedCostMicrounits = int64(micro)
	return response, nil
}

func escrowScopeSHA(customerID, runID, budgetID, phaseKey string) string {
	return digestCanonical(map[string]any{"schema": "llmtw/reserve-batch-escrow-scope/v1", "customer_id": customerID, "run_id": runID, "budget_id": budgetID, "phase_key": phaseKey})
}

func (binding *productionPhaseBinding) allocateBatchGrants(ctx context.Context, request llm.AllocateBatchGrantsRequestV1) (llm.AllocateBatchGrantsResponseV1, error) {
	escrow, ok := binding.composition.Materializer.(durablestore.BatchEscrowMaterializer)
	if !ok || escrow == nil {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("atomic batch escrow materializer is unavailable")
	}
	if len(binding.cap.GrantHMACKey) < 32 || binding.cap.GrantKeyID == "" {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("reserve batch grant signing is unavailable")
	}
	if binding.cap.ResolveScope == nil {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocate batch grants scope resolver is unavailable")
	}
	if _, err := binding.cap.ResolveScope(ctx, request.Context); err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	if string(batchResourceIdentity(rawScope(request.Context), "reserve-batch-escrow:"+request.BatchID, request.PhaseKey)) != request.EscrowID {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocate batch customer scope does not match escrow")
	}
	escrowDigest, err := decodeSHA256(request.EscrowSHA256)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	// Parent identity is immutable too: closing or consuming the escrow must not
	// erase a previously committed allocation receipt.
	parent, found, err := binding.loadBatchMaterialization(ctx, escrowDigest)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	if !found || len(parent.Requests) != 1 || !parent.Results[0].Accepted {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("batch escrow is not accepted")
	}
	escrowRequest := parent.Requests[0]
	if string(escrowRequest.OperationID) != request.EscrowID || escrowRequest.Bounds.OperationSHA256 != escrowScopeSHA(request.CustomerID, request.RunID, request.BudgetID, request.PhaseKey) || escrowRequest.Bounds.Model != request.PhaseKey {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocate batch customer/run/budget identity does not match escrow")
	}
	if escrowRequest.Route.RouteID != request.BatchSHA256 || escrowRequest.Route.ServiceClass != "escrow" {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocation identity does not match escrow")
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	contentDigest, err := decodeSHA256(requestSHA)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	direct := llm.ReserveBatchRequestV1{APIVersion: llm.ReserveBatchAPIVersion, Context: request.Context, CustomerID: request.CustomerID, RunID: request.RunID, BudgetID: request.BudgetID, BatchKey: digestCanonical(map[string]any{"allocate": request.BatchID, "phase": request.PhaseKey, "wave": request.WaveKey, "sequence": request.AllocationSequence}), PricingGenerationID: escrowRequest.Route.ResolvedModel, PricingManifestSHA256: escrowRequest.Route.EndpointID, RemainingMaxCostMicrounits: math.MaxInt64, Operations: request.Operations}
	materialization, found, err := binding.loadBatchMaterialization(ctx, contentDigest)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	if found {
		return binding.allocationMaterializationResponse(request, direct, requestSHA, materialization)
	}
	now := binding.now().UTC()
	if !request.GrantExpiresAt.After(now) || request.GrantExpiresAt.After(escrowRequest.ExpiresAt) {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("grant expiry is outside the escrow lifetime")
	}
	// Live escrow checks belong only to a new allocation, not receipt recovery.
	current, result, err := escrow.LoadBatchEscrow(ctx, escrowDigest, escrowRequest.OperationID)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	if !result.Accepted {
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("batch escrow is not accepted")
	}
	capture := &captureBatchMaterializer{BudgetMaterializer: binding.composition.Materializer}
	allocating := *binding
	allocating.composition.Materializer = capture
	allocating.cap.ReservationLease = request.GrantExpiresAt.Sub(now)
	if _, err := allocating.reserveBatchMaterialized(ctx, direct, false); err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	type escrowWindowKey struct {
		policy, window string
	}
	escrowWindows := make(map[escrowWindowKey]admission.WindowReservation, len(escrowRequest.Reservations))
	for _, reservation := range escrowRequest.Reservations {
		key := escrowWindowKey{reservation.PolicyID, reservation.WindowID}
		if _, exists := escrowWindows[key]; exists {
			return llm.AllocateBatchGrantsResponseV1{}, errors.New("escrow contains ambiguous budget window bindings")
		}
		escrowWindows[key] = reservation
	}
	for index := range capture.requests {
		// The external expiry is absolute; planning's later clock cannot extend it.
		capture.requests[index].ExpiresAt = request.GrantExpiresAt.UTC()
		for windowIndex := range capture.requests[index].Reservations {
			child := &capture.requests[index].Reservations[windowIndex]
			parentWindow, exists := escrowWindows[escrowWindowKey{child.PolicyID, child.WindowID}]
			if !exists {
				return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocation budget window does not match escrow")
			}
			if err := validateGrantedWindowReservation(*child, parentWindow); err != nil {
				return llm.AllocateBatchGrantsResponseV1{}, fmt.Errorf("allocation escrow window: %w", err)
			}
			// Allocation transfers existing capacity; it cannot move that capacity
			// to the current clock's bucket or reserve a new enforcement window.
			child.Bucket = parentWindow.Bucket
		}
	}
	results, err := escrow.AllocateBatchGrants(ctx, contentDigest, current, request.AllocationSequence, capture.requests)
	if err != nil && !errors.Is(err, durablestore.ErrBatchMaterializationConflict) {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	if err == nil {
		if resultErr := durablestore.ValidateBatchReserveResult(capture.requests, results); resultErr != nil {
			return llm.AllocateBatchGrantsResponseV1{}, resultErr
		}
	}
	materialization, found, loadErr := binding.loadBatchMaterialization(ctx, contentDigest)
	if loadErr != nil {
		return llm.AllocateBatchGrantsResponseV1{}, loadErr
	}
	if !found {
		if err != nil {
			return llm.AllocateBatchGrantsResponseV1{}, err
		}
		return llm.AllocateBatchGrantsResponseV1{}, errors.New("committed allocation materialization is unavailable")
	}
	return binding.allocationMaterializationResponse(request, direct, requestSHA, materialization)
}

func (binding *productionPhaseBinding) allocationMaterializationResponse(request llm.AllocateBatchGrantsRequestV1, direct llm.ReserveBatchRequestV1, requestSHA string, materialization durablestore.BatchMaterialization) (llm.AllocateBatchGrantsResponseV1, error) {
	for _, reserved := range materialization.Requests {
		if !reserved.ExpiresAt.Equal(request.GrantExpiresAt) {
			return llm.AllocateBatchGrantsResponseV1{}, errors.New("allocation materialization expiry does not match request")
		}
	}
	planned, err := binding.batchMaterializationResponse(direct, request.BatchID, request.BatchSHA256, requestSHA, materialization)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	remaining, err := pricing.CeilMicroFromUSD(materialization.RemainingEscrowCostUSD)
	if err != nil {
		return llm.AllocateBatchGrantsResponseV1{}, err
	}
	return llm.AllocateBatchGrantsResponseV1{APIVersion: llm.AllocateBatchGrantsAPIVersion, BatchID: request.BatchID, RequestSHA256: requestSHA, BatchSHA256: request.BatchSHA256, EscrowID: request.EscrowID, EscrowSHA256: request.EscrowSHA256, AllocationSequence: request.AllocationSequence, Status: planned.Status, ReservedCostMicrounits: planned.ReservedCostMicrounits, RemainingEscrowCostMicrounits: int64(remaining), Grants: planned.Grants}, nil
}

func (binding *productionPhaseBinding) closeBatch(ctx context.Context, request llm.CloseBatchRequestV1) (llm.CloseBatchResponseV1, error) {
	escrow, ok := binding.composition.Materializer.(durablestore.BatchEscrowMaterializer)
	if !ok || escrow == nil {
		return llm.CloseBatchResponseV1{}, errors.New("atomic batch escrow materializer is unavailable")
	}
	if binding.cap.ResolveScope == nil {
		return llm.CloseBatchResponseV1{}, errors.New("close batch scope resolver is unavailable")
	}
	if _, err := binding.cap.ResolveScope(ctx, request.Context); err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	if string(batchResourceIdentity(rawScope(request.Context), "reserve-batch-escrow:"+request.BatchID, request.PhaseKey)) != request.EscrowID {
		return llm.CloseBatchResponseV1{}, errors.New("close batch customer scope does not match escrow")
	}
	escrowDigest, err := decodeSHA256(request.EscrowSHA256)
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	escrowRequest, _, err := escrow.LoadBatchEscrow(ctx, escrowDigest, durablestore.OperationID(request.EscrowID))
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	if escrowRequest.Bounds.OperationSHA256 != escrowScopeSHA(request.CustomerID, request.RunID, request.BudgetID, request.PhaseKey) || escrowRequest.Bounds.Model != request.PhaseKey {
		return llm.CloseBatchResponseV1{}, errors.New("close batch customer/run/budget identity does not match escrow")
	}
	if escrowRequest.Route.RouteID != request.BatchSHA256 {
		return llm.CloseBatchResponseV1{}, errors.New("close batch identity does not match escrow")
	}
	closeSHA, err := request.RequestSHA256()
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	closeDigest, err := decodeSHA256(closeSHA)
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	refund, existing, err := escrow.CloseBatchEscrow(ctx, escrowDigest, escrowRequest, closeDigest)
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	micro, err := pricing.CeilMicroFromUSD(refund)
	if err != nil {
		return llm.CloseBatchResponseV1{}, err
	}
	status := llm.CloseBatchStatusClosed
	if existing {
		status = llm.CloseBatchStatusAlreadyClosed
	}
	return llm.CloseBatchResponseV1{APIVersion: llm.CloseBatchAPIVersion, BatchID: request.BatchID, EscrowID: request.EscrowID, Status: status, RefundedCostMicrounits: int64(micro)}, nil
}

func priceDescriptorMaximum(descriptor llm.ReserveBatchOperationV1, entry pricing.Entry) (pricing.USD, error) {
	components := []struct {
		component   pricing.PriceComponent
		price       pricing.DecimalUSD
		units       int64
		denominator int64
	}{
		{pricing.PriceComponentInput, entry.Prices.InputPerMillion, descriptor.MaxInputTokens, 1_000_000},
		{pricing.PriceComponentOutput, entry.Prices.OutputPerMillion, descriptor.MaxOutputTokens, 1_000_000},
		{pricing.PriceComponentReasoning, entry.Prices.ReasoningPerMillion, descriptor.MaxReasoningTokens, 1_000_000},
		{pricing.PriceComponentCacheRead, entry.Prices.CacheReadPerMillion, descriptor.MaxCacheReadTokens, 1_000_000},
		{pricing.PriceComponentCacheWrite, entry.Prices.CacheWritePerMillion, descriptor.MaxCacheWriteTokens, 1_000_000},
		{pricing.PriceComponentPerRequest, entry.Prices.PerRequest, 1, 1},
	}
	total := pricing.MustUSD("0")
	for _, component := range components {
		if component.units > 0 && entry.ComponentUnknown(component.component) {
			return pricing.USD{}, fmt.Errorf("price component %q is unknown", component.component)
		}
		value, err := pricing.CeilUSD(component.price, component.units, component.denominator)
		if err != nil {
			return pricing.USD{}, err
		}
		total, err = total.Add(value)
		if err != nil {
			return pricing.USD{}, err
		}
	}
	return total, nil
}

func digestCanonical(value any) string {
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func decodeSHA256(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, errors.New("invalid SHA-256 identity")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func (binding *productionPhaseBinding) signGrant(request llm.ReserveBatchRequestV1, batchID, batchSHA, materializationRequestSHA string, grant llm.ReserveBatchGrantV1) string {
	payload := grantAuthenticationPayload(request.Context, request.CustomerID, request.RunID, request.BudgetID, request.PricingGenerationID, request.PricingManifestSHA256, batchID, batchSHA, materializationRequestSHA, grant.OperationKey, grant.GrantID, grant.GrantSHA256, grant.GrantKeyID, grant.MaxCostMicrounits)
	mac := hmac.New(sha256.New, binding.cap.GrantHMACKey)
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func grantAuthenticationPayload(requestContext llm.RequestContext, customerID, runID, budgetID, pricingGeneration, pricingDigest, batchID, batchSHA, materializationRequestSHA, operationKey, grantID, grantSHA, grantKeyID string, maximum int64) []byte {
	encoded, _ := json.Marshal(map[string]any{
		"schema": "llmtw/reserve-batch-grant-auth/v1", "tenant": requestContext.Tenant,
		"project": requestContext.Project, "actor": requestContext.Actor, "customer_id": customerID,
		"run_id": runID, "budget_id": budgetID, "pricing_generation_id": pricingGeneration,
		"pricing_manifest_sha256": pricingDigest, "batch_id": batchID, "batch_sha256": batchSHA,
		"grant_materialization_request_sha256": materializationRequestSHA,
		"operation_key":                        operationKey, "grant_id": grantID, "grant_sha256": grantSHA,
		"grant_key_id": grantKeyID, "max_cost_microunits": maximum,
	})
	return encoded
}

func (binding *productionPhaseBinding) prepareGrantedReservationRoute(ctx context.Context, request llm.GenerateRequestV1, route durablestore.RoutePlan) (durablestore.RoutePlan, error) {
	admitted := request.CostAdmission
	if admitted == nil || admitted.BatchID == "" {
		return durablestore.RoutePlan{}, errors.New("batch grant identity is unavailable")
	}
	if admitted.GrantKeyID != binding.cap.GrantKeyID || len(binding.cap.GrantHMACKey) < 32 {
		return durablestore.RoutePlan{}, errors.New("batch grant signing key does not match active snapshot")
	}
	expectedGrantID := string(batchResourceIdentity(rawScope(request.Context), "reserve-batch-grant:"+admitted.BatchID, request.OperationKey))
	if admitted.GrantID != expectedGrantID {
		return durablestore.RoutePlan{}, errors.New("batch grant identity does not match operation")
	}
	payload := grantAuthenticationPayload(request.Context, admitted.CustomerID, admitted.RunID, admitted.BudgetID, admitted.PricingGenerationID, admitted.PricingManifestSHA256, admitted.BatchID, admitted.BatchSHA256, admitted.GrantMaterializationRequestSHA256, request.OperationKey, admitted.GrantID, admitted.GrantSHA256, admitted.GrantKeyID, admitted.RemainingMaxCostMicrounits)
	mac := hmac.New(sha256.New, binding.cap.GrantHMACKey)
	_, _ = mac.Write(payload)
	provided, err := hex.DecodeString(admitted.GrantHMACSHA256)
	if err != nil || subtle.ConstantTimeCompare(provided, mac.Sum(nil)) != 1 {
		return durablestore.RoutePlan{}, errors.New("batch grant authentication failed")
	}
	contentDigest, err := decodeSHA256(admitted.GrantMaterializationRequestSHA256)
	if err != nil {
		return durablestore.RoutePlan{}, errors.New("batch grant has an invalid content identity")
	}
	materializer, ok := binding.composition.Materializer.(durablestore.BatchGrantMaterializer)
	if !ok || materializer == nil {
		return durablestore.RoutePlan{}, errors.New("batch grant materializer is unavailable")
	}
	reserveRequest, reservation, err := materializer.ConfirmBatchGrant(ctx, contentDigest, route.OperationID)
	if err != nil {
		return durablestore.RoutePlan{}, err
	}
	if err := durablestore.ValidatePlannedReserveResult(reserveRequest, reservation); err != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("validate confirmed batch grant: %w", err)
	}
	expectedRoute := durablestore.DispatchRouteFacts{
		RouteID: route.RouteID, EndpointID: route.EndpointID, Provider: route.Provider,
		ResolvedModel: route.Model, ServiceClass: string(route.Execution.Candidate.Class()),
		PriceVersion: route.PriceVersion,
	}
	if reserveRequest.GenerationID != route.GenerationID || reserveRequest.IncarnationID != binding.cap.BudgetIncarnationID ||
		reserveRequest.Route != expectedRoute || len(reserveRequest.Reservations) != len(route.Execution.Reservations) {
		return durablestore.RoutePlan{}, errors.New("batch grant reservation does not match resolved Generate route")
	}
	// Policy matching preserves configuration order, whereas materializers may
	// canonicalize it. Match window identities, not positions or current buckets.
	type grantWindowKey struct {
		policy, window string
	}
	grantedWindows := make(map[grantWindowKey]int, len(reserveRequest.Reservations))
	for index, granted := range reserveRequest.Reservations {
		key := grantWindowKey{granted.PolicyID, granted.WindowID}
		if _, exists := grantedWindows[key]; exists {
			return durablestore.RoutePlan{}, errors.New("batch grant contains ambiguous budget window bindings")
		}
		grantedWindows[key] = index
	}
	for _, actual := range route.Execution.Reservations {
		key := grantWindowKey{actual.PolicyID, actual.WindowID}
		index, exists := grantedWindows[key]
		if !exists {
			return durablestore.RoutePlan{}, errors.New("Generate route budget window identity does not match its batch grant")
		}
		if err := validateGrantedWindowReservation(actual, reserveRequest.Reservations[index]); err != nil {
			return durablestore.RoutePlan{}, err
		}
		delete(grantedWindows, key)
	}
	if len(reserveRequest.Reservations) == 0 {
		return durablestore.RoutePlan{}, errors.New("batch grant has no budget reservation")
	}
	bounds := reserveRequest.Bounds
	grantKeyDigest := sha256.Sum256(binding.cap.GrantHMACKey)
	if bounds.GrantKeyID != binding.cap.GrantKeyID || bounds.GrantKeySHA256 != hex.EncodeToString(grantKeyDigest[:]) {
		return durablestore.RoutePlan{}, errors.New("batch grant reservation signing key does not match active snapshot")
	}
	if bounds.OperationSHA256 == "" || bounds.Model != route.Execution.Request.Model {
		return durablestore.RoutePlan{}, errors.New("batch grant operation identity or model does not match resolved Generate route")
	}
	grantedMaximum := reserveRequest.LogicalCostUSD
	grantedMicro, err := pricing.CeilMicroFromUSD(grantedMaximum)
	if err != nil || int64(grantedMicro) != admitted.RemainingMaxCostMicrounits ||
		route.Execution.EstimatedUSD.Cmp(grantedMaximum) > 0 {
		return durablestore.RoutePlan{}, errors.New("Generate estimate exceeds its signed batch grant ceiling")
	}
	route.Execution.Reservations = append([]admission.WindowReservation(nil), reserveRequest.Reservations...)
	route.Execution.Bounds = bounds
	route.Execution.EstimatedUSD = grantedMaximum
	route.ReservationRecovered = true
	route.ReservationExpiresAt = reserveRequest.ExpiresAt
	route.Reservation = &reservation
	if err := binding.persistReservationFacts(ctx, "generate", llm.APIVersion, request.OperationKey, request.Context, request, route, reservation); err != nil {
		return durablestore.RoutePlan{}, err
	}
	if err := binding.cap.Estimator.ValidateCandidateTokenLimits(route.Execution.Request, route.Execution.Candidate); err != nil {
		return route, fmt.Errorf("validate granted Generate token ceilings: %w", err)
	}
	estimator := binding.cap.Estimator
	estimator.MaxInput = 0
	estimator.MaxOutput = 0
	estimator.MaxReasoning = 0
	actualEstimate, estimateErr := estimator.EstimateCandidate(route.Execution.Request, route.Execution.Candidate, route.Execution.Price)
	if estimateErr != nil {
		return durablestore.RoutePlan{}, fmt.Errorf("estimate granted Generate components: %w", estimateErr)
	}
	if actualEstimate.InputTokens > bounds.MaxInputTokens ||
		actualEstimate.OutputTokens > bounds.MaxOutputTokens ||
		actualEstimate.ReasoningTokens > bounds.MaxReasoningTokens ||
		actualEstimate.CacheReadTokens > bounds.MaxCacheReadTokens ||
		actualEstimate.CacheWriteTokens > bounds.MaxCacheWriteTokens {
		return route, fmt.Errorf("%w: Generate token components exceed the signed batch grant", budget.ErrTokenLimit)
	}
	return route, nil
}

func validateGrantedWindowReservation(actual, granted admission.WindowReservation) error {
	if actual.PolicyID != granted.PolicyID || actual.WindowID != granted.WindowID {
		return errors.New("Generate route budget policy identity changed from its batch grant")
	}
	if actual.BucketNanos != granted.BucketNanos || actual.DurationNanos != granted.DurationNanos {
		return errors.New("Generate route budget window geometry changed from its batch grant")
	}
	if actual.Limit != granted.Limit ||
		(!actual.LimitUSD.IsZero() && actual.LimitUSD.Cmp(granted.LimitUSD) != 0) {
		return errors.New("Generate route budget window limit changed from its batch grant")
	}
	if actual.AmountUSD.Cmp(granted.AmountUSD) > 0 {
		return errors.New("Generate route amount exceeds its batch grant reservation")
	}
	return nil
}
