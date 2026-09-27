package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// grantedTierMaterializer confirms one pre-built batch grant and records the
// settlement. Every other materializer method is the nil embedded interface,
// so a claim, dispatch fence or new acceptance would panic the test.
type grantedTierMaterializer struct {
	durablestore.BudgetLeaser
	request    durablestore.ReserveRequest
	result     durablestore.ReserveResult
	reconciled []durablestore.ReconcileRequest
}

func (materializer *grantedTierMaterializer) ConfirmBatchGrant(_ context.Context, _ [32]byte, operationID durablestore.OperationID) (durablestore.ReserveRequest, durablestore.ReserveResult, error) {
	if operationID != materializer.request.OperationID {
		return durablestore.ReserveRequest{}, durablestore.ReserveResult{}, errors.New("unexpected grant operation")
	}
	return materializer.request, materializer.result, nil
}

func (materializer *grantedTierMaterializer) Reconcile(_ context.Context, request durablestore.ReconcileRequest) error {
	materializer.reconciled = append(materializer.reconciled, request)
	return nil
}

// grantedTierStore accepts the immutable reservation facts and otherwise
// behaves like the reserved operation store used by the no-dispatch tests.
type grantedTierStore struct {
	transitionAdmissionStore
	facts int
}

func (store *grantedTierStore) Begin(_ context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	store.facts++
	operation := store.operation.Clone()
	operation.ImmutableFacts = append([]byte(nil), request.ImmutableFacts...)
	return admission.BeginResult{Operation: operation, Existing: true}, nil
}

// An over-tier prompt on a batch-granted Generate (Gemini 3.1 Pro is billed at
// a higher rate above 200,000 prompt tokens) must end as the token-limit
// no-dispatch terminal, release its grant through finalizeFailedBudget and
// never reach a provider.
func TestRouteGenerateReleasesOverTierGrantedPromptWithoutDispatch(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	entry := testPriceEntry("openrouter-google-gemini31-pro", "google/gemini-3.1-pro-preview", "default")
	entry.Version, entry.MaxPromptTokens = "prices-v1", 200
	catalog, err := pricing.CompileUSD("prices-v1", []pricing.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	manifest := hex.EncodeToString(catalog.Digest[:])
	routes, err := routing.CompileCatalog("config-v1", map[string]routing.Model{"gemini": {Name: "gemini", Routes: []routing.Route{{
		ID: "route-gemini", EndpointID: entry.EndpointID, Provider: entry.Provider, Family: entry.Family, Region: entry.Region,
		Model: entry.Model, Classes: []llm.ServiceClass{llm.ServiceClassStandard},
		ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "default"}, PriceAvailable: true, PriceVersion: entry.Version,
		Capabilities: routing.CapabilitySet{Version: "capabilities-v1", Features: map[routing.Feature]routing.Capability{routing.FeatureText: {State: routing.CapabilityNative}}},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	window := budget.Window{ID: "hour", Duration: time.Hour, Bucket: time.Minute, Limit: 100_000_000, LimitUSD: pricing.MustUSD("100")}
	grantKey := []byte(strings.Repeat("h", 32))
	grantKeyDigest := sha256.Sum256(grantKey)

	requestContext := llm.RequestContext{Tenant: "tenant", Project: "forecast", Actor: "actor", Tags: map[string]string{llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}}
	model := "gemini"
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "gemini-over-tier", Context: requestContext,
		// The byte-count estimator makes this prompt far larger than 200 tokens.
		Append:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: strings.Repeat("evidence ", 100)}}}},
		SettingsPatch: llm.SettingsPatchV1{Model: llm.Patch[string]{Set: &model}},
	}
	operationID := strictOperationIdentity(rawScope(requestContext), requestContext.Actor, "generate", llm.APIVersion, request.OperationKey)
	grantMaximum := pricing.MustUSD("0.000600")
	reserve := durablestore.ReserveRequest{
		OperationID: operationID, GenerationID: "generation-grant", IncarnationID: "incarnation-grant",
		Reservations: []admission.WindowReservation{{PolicyID: "policy", WindowID: "hour", Bucket: now.Unix() / 60, Amount: 600, Limit: window.Limit, AmountUSD: grantMaximum, LimitUSD: window.LimitUSD, BucketNanos: int64(time.Minute), DurationNanos: int64(time.Hour)}},
		ExpiresAt:    now.Add(30 * time.Minute), OccurredAt: now.Add(-time.Minute),
		Route: durablestore.DispatchRouteFacts{RouteID: "route-gemini", EndpointID: entry.EndpointID, Provider: entry.Provider, ResolvedModel: entry.Model, ServiceClass: string(llm.ServiceClassStandard), PriceVersion: entry.Version},
		Bounds: durablestore.ReservationBounds{OperationSHA256: strings.Repeat("a", 64), Model: model, MaxInputTokens: 200, MaxOutputTokens: 100,
			GrantKeyID: "grant-key", GrantKeySHA256: hex.EncodeToString(grantKeyDigest[:])},
		LogicalCostUSD: grantMaximum,
	}
	planned, err := durablestore.PlannedReserveResult(reserve)
	if err != nil {
		t.Fatal(err)
	}
	materializer := &grantedTierMaterializer{request: reserve, result: planned}

	materializationSHA := strings.Repeat("b", 64)
	admitted := llm.CostAdmissionV1{APIVersion: llm.CostAdmissionAPIVersion, BudgetID: "budget-1", CustomerID: "customer-1", RunID: "run-1", OperationKey: request.OperationKey,
		GatewayAttemptOrdinal: 1, BatchID: "batch-1", BatchSHA256: strings.Repeat("c", 64), GrantMaterializationRequestSHA256: materializationSHA,
		GrantID: string(batchResourceIdentity(rawScope(requestContext), "reserve-batch-grant:batch-1", request.OperationKey)), GrantSHA256: strings.Repeat("d", 64), GrantKeyID: "grant-key",
		PricingGenerationID: catalog.Version, PricingManifestSHA256: manifest, RemainingMaxCostMicrounits: 600}
	mac := hmac.New(sha256.New, grantKey)
	_, _ = mac.Write(grantAuthenticationPayload(requestContext, admitted.CustomerID, admitted.RunID, admitted.BudgetID, admitted.PricingGenerationID, admitted.PricingManifestSHA256, admitted.BatchID, admitted.BatchSHA256, admitted.GrantMaterializationRequestSHA256, admitted.OperationKey, admitted.GrantID, admitted.GrantSHA256, admitted.GrantKeyID, admitted.RemainingMaxCostMicrounits))
	admitted.GrantHMACSHA256 = hex.EncodeToString(mac.Sum(nil))
	request.CostAdmission = &admitted

	store := &grantedTierStore{transitionAdmissionStore: transitionAdmissionStore{operation: admission.Operation{ID: string(operationID), State: admission.StateReserved, DispatchToken: "dispatch-token"}}}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			Clock: func() time.Time { return now }, Planner: routing.DeterministicPlanner{}, ReservationLease: 30 * time.Minute, OperationRetention: 2 * time.Hour,
			BudgetGenerationID: "generation-grant", BudgetIncarnationID: "incarnation-grant", GrantKeyID: "grant-key", GrantHMACKey: grantKey,
			Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Routes: routes, Prices: pricing.NewResolver(catalog), RequireBudgetMatch: true,
				BudgetPolicies: []budget.Policy{{ID: "policy", Windows: []budget.Window{window}}}}},
		},
		composition: durablestore.Composition{Materializer: materializer, Operations: store},
	}

	_, err = binding.routeGenerate(context.Background(), request, durablestore.GenerateReplay{OperationID: operationID}, durablestore.CompactionDecision{})
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Code != provider.CodeInvalidArgument || failure.Phase != provider.PhasePlan || failure.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("routeGenerate error = %#v, want the token-limit no-dispatch failure", err)
	}
	if store.operation.State != admission.StateDefiniteFailed || store.operation.FailureReason != tokenBoundsFailureReason || store.failure.Certainty != admission.NotDispatched || store.facts != 1 {
		t.Fatalf("operation = %#v, failure = %#v, facts = %d; want the token-bound no-dispatch terminal", store.operation, store.failure, store.facts)
	}
	for _, transition := range store.transitions {
		if transition == "dispatching" {
			t.Fatal("over-tier granted prompt was marked dispatching")
		}
	}
	if len(materializer.reconciled) != 1 || len(materializer.reconciled[0].Events) != 1 {
		t.Fatalf("grant settlements = %#v, want one release", materializer.reconciled)
	}
	release := materializer.reconciled[0].Events[0]
	if release.OperationID != string(operationID) || release.Kind != budget.JournalFinalizeExact || release.ActualCostUSD == nil || !release.ActualCostUSD.IsZero() || release.ReservedDecreaseUSD.Cmp(grantMaximum) != 0 {
		t.Fatalf("grant release = %#v, want exact zero cost releasing %s", release, grantMaximum)
	}
}
