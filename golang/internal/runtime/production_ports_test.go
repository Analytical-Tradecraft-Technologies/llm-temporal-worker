package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

func TestProductionPhaseFactoriesRejectIncompleteSnapshotBeforeCallbacks(t *testing.T) {
	if _, err := NewProductionGeneratePortsFactory()(context.Background(), V1RuntimeCapabilities{}); err == nil {
		t.Fatal("Generate factory accepted an incomplete durable snapshot")
	}
	if _, err := NewProductionCompactPortsFactory()(context.Background(), V1RuntimeCapabilities{}); err == nil {
		t.Fatal("Compact factory accepted an incomplete durable snapshot")
	}
}

func TestProductionOperationIdentityBindsCompleteLogicalKeyNotRequestDigest(t *testing.T) {
	_, _, firstDigest, err := requestManifest(map[string]any{"prompt": "first"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, secondDigest, err := requestManifest(map[string]any{"prompt": "changed"})
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest == secondDigest {
		t.Fatal("changed request bodies produced one fingerprint")
	}
	first := strictOperationIdentity("tenant\x00project", "actor", "generate", llm.APIVersion, "operation")
	if first != strictOperationIdentity("tenant\x00project", "actor", "generate", llm.APIVersion, "operation") {
		t.Fatal("operation identity is not deterministic")
	}
	if first == strictOperationIdentity("tenant\x00project", "other-actor", "generate", llm.APIVersion, "operation") {
		t.Fatal("authenticated actors collided")
	}
	if first == strictOperationIdentity("tenant\x00project", "actor", "compact", llm.CompactAPIVersion, "operation") {
		t.Fatal("Generate and Compact operation identities collided")
	}
	if first == strictOperationIdentity("tenant\x00project", "actor", "generate", "other-version", "operation") {
		t.Fatal("API versions collided")
	}
	if first == strictOperationIdentity("other\x00project", "actor", "generate", llm.APIVersion, "operation") {
		t.Fatal("authenticated scopes collided")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("operation identity is invalid: %v", err)
	}
	releasedFirst := releasedOperationIdentity("generate", "operation", firstDigest)
	if releasedFirst == releasedOperationIdentity("generate", "operation", secondDigest) {
		t.Fatal("released-v1 compatibility identity did not bind the request digest")
	}
	if releasedFirst == first {
		t.Fatal("released-v1 and strict operation identities unexpectedly matched")
	}
}
func TestGenerateManifestIsContentFreeAndEncryptedPayloadReplaysExactly(t *testing.T) {
	model := "settings-model-secret"
	tools := []llm.Tool{{
		Name:        "secret_tool",
		Description: "tool-description-secret",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"secret_argument":{"type":"string"}}}`),
	}}
	request := llm.GenerateRequestV1{
		APIVersion:   llm.APIVersion,
		OperationKey: "operation-secret",
		Context: llm.RequestContext{
			Tenant: "tenant-plaintext-secret", Project: "project-plaintext-secret", Actor: "actor-secret",
		},
		Append: []llm.Item{llm.Message{
			Actor:   llm.ActorHuman,
			Content: []llm.Part{llm.TextPart{Text: "prompt-plaintext-secret"}},
		}},
		SettingsPatch: llm.SettingsPatchV1{
			Model: llm.Patch[string]{Set: &model},
			Tools: llm.Patch[[]llm.Tool]{Set: &tools},
		},
	}
	store := &beginCaptureStore{}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour,
			Clock:              func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
			CheckpointLimits:   state.MaterializeLimits{MaxDepth: 4, MaxRows: 4, MaxItems: 8, MaxBytes: 1024},
		},
		composition: durablestore.Composition{Operations: store},
	}
	if _, err := binding.replayGenerate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got, want := store.request.ReleasedID, string(releasedOperationIdentity("generate", request.OperationKey, store.request.RequestDigest)); got != want {
		t.Fatalf("released-v1 upgrade identity = %q, want %q", got, want)
	}
	if store.request.OperationKey != request.OperationKey || store.request.Actor != request.Context.Actor {
		t.Fatalf("strict operation identity fields = key %q actor %q", store.request.OperationKey, store.request.Actor)
	}
	for _, plaintext := range []string{
		"tenant-plaintext-secret",
		"project-plaintext-secret",
		"prompt-plaintext-secret",
		"settings-model-secret",
		"secret_tool",
		"tool-description-secret",
		"secret_argument",
	} {
		if bytes.Contains(store.request.RequestManifest, []byte(plaintext)) {
			t.Fatalf("request manifest leaked plaintext %q: %s", plaintext, store.request.RequestManifest)
		}
	}
	var manifest map[string]any
	if err := json.Unmarshal(store.request.RequestManifest, &manifest); err != nil {
		t.Fatalf("decode content-free request manifest: %v", err)
	}
	allowed := map[string]bool{"schema_version": true, "payload_sha256": true, "payload_bytes": true, "payload_reference": true}
	if len(manifest) != len(allowed) {
		t.Fatalf("request manifest contains unexpected metadata: %#v", manifest)
	}
	for field := range manifest {
		if !allowed[field] {
			t.Fatalf("request manifest contains content-bearing field %q", field)
		}
	}
	if bytes.Equal(store.request.RequestManifest, store.request.RequestPayload) {
		t.Fatal("content-free manifest was reused as the encrypted request payload")
	}
	if got := sha256.Sum256(store.request.RequestPayload); got != store.request.RequestDigest {
		t.Fatalf("request payload digest = %x, want %x", got, store.request.RequestDigest)
	}
	var reconstructed llm.GenerateRequestV1
	if err := json.Unmarshal(store.request.RequestPayload, &reconstructed); err != nil {
		t.Fatalf("decode canonical encrypted payload: %v", err)
	}
	reencoded, err := json.Marshal(reconstructed)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := llm.CanonicalJSON(reencoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replayed, store.request.RequestPayload) {
		t.Fatalf("reconstructed request changed canonical payload:\n got %s\nwant %s", replayed, store.request.RequestPayload)
	}
}

func TestPersistedReservationFactsRecoverOriginalGenerationIncarnationRouteAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	expires := now.Add(time.Hour)
	reserved := pricing.MustUSD("0.2")
	zero := pricing.MustDecimalUSD("0")
	bounds := durablestore.ReservationBounds{
		OperationSHA256: strings.Repeat("a", 64), Model: "requested-model",
		MaxInputTokens: 4096, MaxOutputTokens: 1024, MaxReasoningTokens: 512,
		MaxCacheReadTokens: 2048, MaxCacheWriteTokens: 256,
	}
	route := durablestore.RoutePlan{
		OperationID: "operation-1", GenerationID: "generation-1",
		RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1",
		Model: "resolved-model", PriceVersion: "price-1", ReservationExpiresAt: expires,
		ResourceCapacityGenerationID:   "capacity-generation-1",
		ResourceCapacityManifestSHA256: strings.Repeat("c", 64),
		Execution: &durablestore.RouteExecution{
			Request: llm.Request{OperationKey: "key-1", Model: "requested-model"},
			Candidate: routing.Candidate{
				RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1",
				Family: "responses", Model: "resolved-model",
				RequestedClass: llm.ServiceClassStandard, AttemptedClass: llm.ServiceClassPriority,
				FallbackIndex: 1,
			},
			Price: pricing.Entry{Version: "price-1", Prices: pricing.UnitPrices{
				InputPerMillion: zero, OutputPerMillion: zero, CacheReadPerMillion: zero,
				CacheWritePerMillion: zero, ReasoningPerMillion: zero, PerRequest: zero,
			}},
			Reservations: []admission.WindowReservation{{PolicyID: "policy-a", WindowID: "window-a", Bucket: now.Unix(), Amount: 200000, Limit: 1000000, AmountUSD: reserved, LimitUSD: pricing.MustUSD("1"), BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour)}},
			EstimatedUSD: reserved,
			Bounds:       bounds,
		},
	}
	reserveRequest := durablestore.ReserveRequest{
		OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: "incarnation-1",
		Reservations: route.Execution.Reservations, ExpiresAt: expires, OccurredAt: now, Bounds: bounds,
	}
	reservation, err := durablestore.PlannedReserveResult(reserveRequest)
	if err != nil {
		t.Fatal(err)
	}
	route.Reservation = &reservation
	facts, err := persistedFactsForRoute(route, reservation)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := routeFromPersistedFacts(encoded, route.Execution.Request)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.GenerationID != route.GenerationID || recovered.ReservationExpiresAt != expires || recovered.Reservation == nil || recovered.Reservation.IncarnationID != reservation.IncarnationID {
		t.Fatalf("recovered reservation changed immutable facts: %#v", recovered)
	}
	if recovered.ResourceCapacityGenerationID != route.ResourceCapacityGenerationID || recovered.ResourceCapacityManifestSHA256 != route.ResourceCapacityManifestSHA256 {
		t.Fatalf("recovered route changed immutable signed capacity: %#v", recovered)
	}
	if recovered.Execution.Candidate.RouteID != route.RouteID || recovered.Execution.Candidate.AttemptedClass != llm.ServiceClassPriority || recovered.Execution.Price.Version != "price-1" {
		t.Fatalf("recovered route/pricing changed: %#v", recovered.Execution)
	}
	if recovered.Execution.Bounds != bounds {
		t.Fatalf("recovered route changed signed reservation bounds: %#v", recovered.Execution.Bounds)
	}
	materializer := &replayReservationMaterializer{result: reservation}
	got, err := (&productionPhaseBinding{composition: durablestore.Composition{Materializer: materializer}}).reserveGenerate(context.Background(), llm.GenerateRequestV1{}, recovered)
	if err != nil || got.IncarnationID != reservation.IncarnationID || materializer.confirms != 1 || materializer.accepts != 0 {
		t.Fatalf("recovered reservation was not confirmed without re-acceptance: %#v, %v", got, err)
	}
	if materializer.confirmRequest.Bounds != bounds {
		t.Fatalf("confirmation dropped signed reservation bounds: %#v", materializer.confirmRequest.Bounds)
	}
}

func TestReservationFactsCommitBeforeAcceptAndUseConfiguredLease(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	calls := []string{}
	store := &immutableCaptureStore{calls: &calls}
	materializer := &orderingMaterializer{calls: &calls}
	binding := &productionPhaseBinding{
		cap:         V1RuntimeCapabilities{BudgetIncarnationID: "incarnation-1", ReservationLease: 2 * time.Minute, OperationRetention: time.Hour, Clock: func() time.Time { return now }},
		composition: durablestore.Composition{Operations: store, Materializer: materializer},
	}
	reserved := pricing.MustUSD("0.2")
	route := durablestore.RoutePlan{
		OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1", PriceVersion: "price-1",
		Execution: &durablestore.RouteExecution{
			Candidate:    routing.Candidate{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1"},
			Reservations: []admission.WindowReservation{{PolicyID: "policy-1", WindowID: "window-1", Bucket: now.Unix(), Amount: 200000, Limit: 1000000, AmountUSD: reserved, LimitUSD: pricing.MustUSD("1"), BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour)}},
			EstimatedUSD: reserved,
		},
	}
	requestContext := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}
	request := llm.GenerateRequestV1{APIVersion: llm.APIVersion, OperationKey: "key-1", Context: requestContext}
	prepared, err := binding.prepareReservationRoute(context.Background(), "generate", llm.APIVersion, "key-1", requestContext, request, route)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.ReservationExpiresAt.Equal(now.Add(2*time.Minute)) || prepared.ReservationExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("reservation deadline = %v, want configured lease", prepared.ReservationExpiresAt)
	}
	materializer.result = *prepared.Reservation
	if _, err := binding.reserveGenerate(context.Background(), request, prepared); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"persist", "accept"}) {
		t.Fatalf("ordering = %v, want PostgreSQL facts before Redis acceptance", calls)
	}
}

func TestConfirmReservationFailsClosedAfterImmutableLeaseDeadline(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	materializer := &replayReservationMaterializer{}
	route := durableRouteFixture(t, now, now.Add(time.Minute))
	materializer.result = *route.Reservation
	binding := &productionPhaseBinding{cap: V1RuntimeCapabilities{Clock: func() time.Time { return now.Add(time.Minute) }}, composition: durablestore.Composition{Materializer: materializer}}
	if _, err := binding.confirmReservation(context.Background(), route); err == nil {
		t.Fatal("expired immutable reservation was confirmed")
	}
	if materializer.confirms != 0 {
		t.Fatal("Redis confirmation ran after local immutable lease expiry")
	}
}

func TestBeforePossibleWriteFencesRedisBeforePersistingDispatch(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	route := durableRouteFixture(t, now, now.Add(time.Second))
	route.Execution.Candidate.AttemptedClass = llm.ServiceClassStandard
	store := &transitionAdmissionStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateReserved, DispatchToken: "dispatch-token",
		ExpiresAt: now.Add(time.Hour),
	}}
	materializer := &replayReservationMaterializer{}
	materializer.onFence = func() {
		if store.operation.State != admission.StateReserved {
			t.Fatalf("Redis fence must precede PostgreSQL dispatch persistence: %s", store.operation.State)
		}
	}
	observer := &productionDispatchObserver{
		binding: &productionPhaseBinding{
			cap: V1RuntimeCapabilities{Clock: func() time.Time { return now }},
			composition: durablestore.Composition{
				Operations: store, Materializer: materializer,
			},
		},
		operation: store.operation, route: route, candidate: route.Execution.Candidate,
	}
	if err := observer.BeforePossibleWrite(context.Background()); err != nil {
		t.Fatalf("before possible write = %v", err)
	}
	if !observer.marked || !reflect.DeepEqual(store.transitions, []string{"dispatching"}) || len(materializer.fences) != 1 {
		t.Fatalf("dispatch fence ordering: marked=%t transitions=%v fences=%d", observer.marked, store.transitions, len(materializer.fences))
	}
	fence := materializer.fences[0]
	if fence.Reservation.OperationID != route.OperationID || fence.Reservation.GenerationID != route.GenerationID ||
		fence.Reservation.IncarnationID != route.Reservation.IncarnationID ||
		fence.Reservation.Route.RouteID != route.RouteID || fence.Reservation.Route.EndpointID != route.EndpointID ||
		fence.Reservation.Route.Provider != route.Provider || fence.Reservation.Route.ResolvedModel != route.Model ||
		fence.RetainUntil != store.operation.ExpiresAt {
		t.Fatalf("dispatch fence lost immutable facts: %#v", fence)
	}
	if err := observer.BeforePossibleWrite(context.Background()); err != nil || len(materializer.fences) != 1 {
		t.Fatalf("idempotent observer retry = %v, fences=%d", err, len(materializer.fences))
	}
}

func TestProviderCapacityLimitsAtomicallyBindSignedGlobalAndRouteQuotas(t *testing.T) {
	capacity := config.VerifiedResourceCapacity{GenerationID: "capacity-1", Limits: config.ResourceCapacityLimits{
		LLMGlobalMaxInflight: 48, LLMGlobalMaxRequestsPerWindow: 120, LLMGlobalWindowSeconds: 60,
		ProviderCallQueueSeconds: 30,
		ProviderMaxInflight:      []config.ProviderInflightLimit{{RouteID: "bedrock-rater", MaxInflight: 24, MaxRequestsPerWindow: 60, WindowSeconds: 60}},
	}}
	limits, queue, err := providerCapacityLimits(capacity, "bedrock-rater", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if queue != 30*time.Second || len(limits) != 4 {
		t.Fatalf("capacity limits = %#v queue=%s", limits, queue)
	}
	wantKinds := []redisstore.ThrottleKind{redisstore.ThrottleConcurrency, redisstore.ThrottleRequests, redisstore.ThrottleConcurrency, redisstore.ThrottleRequests}
	wantCaps := []int64{48, 120, 24, 60}
	for index := range limits {
		if limits[index].Kind != wantKinds[index] || limits[index].Limit != wantCaps[index] || limits[index].Amount != 1 {
			t.Fatalf("limit %d = %#v", index, limits[index])
		}
	}
	if _, _, err := providerCapacityLimits(capacity, "unverified-route", time.Minute); err == nil {
		t.Fatal("route absent from signed capacity was admitted")
	}
}

func TestProviderCapacityBindingRejectsSnapshotDriftBeforeLeaseAcquisition(t *testing.T) {
	manifest := strings.Repeat("a", 64)
	route := durablestore.RoutePlan{
		ResourceCapacityGenerationID:   "capacity-generation-1",
		ResourceCapacityManifestSHA256: manifest,
	}
	capacity := config.VerifiedResourceCapacity{
		GenerationID:   route.ResourceCapacityGenerationID,
		ManifestSHA256: manifest,
	}
	if err := validateProviderCapacityBinding(route, capacity); err != nil {
		t.Fatalf("matching immutable capacity was rejected: %v", err)
	}
	staleGeneration := capacity
	staleGeneration.GenerationID = "capacity-generation-2"
	if err := validateProviderCapacityBinding(route, staleGeneration); err == nil {
		t.Fatal("capacity generation drift was accepted")
	}
	staleManifest := capacity
	staleManifest.ManifestSHA256 = strings.Repeat("b", 64)
	if err := validateProviderCapacityBinding(route, staleManifest); err == nil {
		t.Fatal("capacity manifest drift was accepted")
	}
	if err := validateProviderCapacityBinding(route, config.VerifiedResourceCapacity{}); err == nil {
		t.Fatal("signed immutable capacity was accepted without a verified snapshot")
	}
	if err := validateProviderCapacityBinding(durablestore.RoutePlan{}, capacity); err == nil {
		t.Fatal("unbound route was admitted under signed capacity")
	}
}

func TestProviderCapacityLeaseIdentityBindsOperationAttemptRouteAndSignedGeneration(t *testing.T) {
	route := durablestore.RoutePlan{
		RouteID:                        "route-1",
		ResourceCapacityGenerationID:   "capacity-generation-1",
		ResourceCapacityManifestSHA256: strings.Repeat("a", 64),
	}
	operation := admission.Operation{ID: "operation-1", DispatchToken: "dispatch-token-1"}
	base := providerCapacityLeaseID(route, operation, 1)
	if base == providerCapacityLeaseID(route, operation, 2) {
		t.Fatal("provider capacity lease identity omitted attempt ordinal")
	}
	changedOperation := operation
	changedOperation.ID = "operation-2"
	if base == providerCapacityLeaseID(route, changedOperation, 1) {
		t.Fatal("provider capacity lease identity omitted operation")
	}
	changedToken := operation
	changedToken.DispatchToken = "dispatch-token-2"
	if base == providerCapacityLeaseID(route, changedToken, 1) {
		t.Fatal("provider capacity lease identity omitted dispatch token")
	}
	changedRoute := route
	changedRoute.RouteID = "route-2"
	if base == providerCapacityLeaseID(changedRoute, operation, 1) {
		t.Fatal("provider capacity lease identity omitted route")
	}
	changedCapacity := route
	changedCapacity.ResourceCapacityGenerationID = "capacity-generation-2"
	if base == providerCapacityLeaseID(changedCapacity, operation, 1) {
		t.Fatal("provider capacity lease identity omitted signed capacity generation")
	}
	changedManifest := route
	changedManifest.ResourceCapacityManifestSHA256 = strings.Repeat("b", 64)
	if base == providerCapacityLeaseID(changedManifest, operation, 1) {
		t.Fatal("provider capacity lease identity omitted signed capacity manifest")
	}
}

func TestStageTwoCapacityUsesSignedSemaphoreWaitNotActivityQueue(t *testing.T) {
	limit, wait, err := resourceClassLimit(config.ResourceCapacityLimits{PythonStage2MaxInflight: 2, StageTwoQueueSeconds: 30, StageTwoSemaphoreWaitSeconds: 30}, llm.ResourceClassPythonStage2)
	if err != nil || limit != 2 || wait != 30*time.Second {
		t.Fatalf("Stage2 capacity = limit %d wait %s error %v", limit, wait, err)
	}
}

func TestForecastEventCapacityUsesSignedGlobalLimitAndFailFastWait(t *testing.T) {
	limit, wait, err := resourceClassLimit(config.ResourceCapacityLimits{ForecastEventMaxInflight: 2, ForecastEventAdmissionWaitSeconds: 5}, llm.ResourceClassForecastEvent)
	if err != nil || limit != 2 || wait != 5*time.Second {
		t.Fatalf("forecast event capacity = limit %d wait %s error %v", limit, wait, err)
	}
}

func TestForecastEventCapacityValidatesSignedDeadlineBeforeLeaseIdentity(t *testing.T) {
	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	request := llm.ResourceCapacityAcquireRequestV1{
		APIVersion:     llm.ResourceCapacityAcquireAPIVersion,
		Context:        llm.RequestContext{Tenant: "customer", Project: "metaculus", Actor: "forecast-workflow"},
		ResourceClass:  llm.ResourceClassForecastEvent,
		LeaseKey:       "forecast-event:" + strings.Repeat("a", 64),
		GenerationID:   "capacity-1",
		ManifestSHA256: strings.Repeat("b", 64),
		QueueDeadline:  now.Add(5 * time.Second),
	}
	validated, err := boundResourceCapacityRequest(request, now, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resourceCapacityLeaseID(validated) != resourceCapacityLeaseID(request) {
		t.Fatal("canonical UTC deadline changed the lease identity")
	}
	extended := request
	extended.QueueDeadline = now.Add(time.Minute)
	if _, err := boundResourceCapacityRequest(extended, now, 5*time.Second); err == nil {
		t.Fatal("caller-extended forecast event admission deadline was accepted")
	}
	if _, err := boundResourceCapacityRequest(request, now.Add(2*time.Minute), 5*time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired forecast event request error = %v, want deadline exceeded", err)
	}
}

func TestRenewResourceCapacityRejectsExpiredLeaseBeforeRedisMutation(t *testing.T) {
	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	manifest := strings.Repeat("b", 64)
	acquire := llm.ResourceCapacityAcquireRequestV1{
		APIVersion:     llm.ResourceCapacityAcquireAPIVersion,
		Context:        llm.RequestContext{Tenant: "customer", Project: "metaculus", Actor: "forecast-workflow"},
		ResourceClass:  llm.ResourceClassForecastEvent,
		LeaseKey:       "forecast-event:" + strings.Repeat("a", 64),
		GenerationID:   "capacity-1",
		ManifestSHA256: manifest,
		QueueDeadline:  now.Add(-time.Minute),
	}
	lease := llm.ResourceCapacityLeaseV1{
		APIVersion:     llm.ResourceCapacityAcquireAPIVersion,
		Context:        acquire.Context,
		ResourceClass:  acquire.ResourceClass,
		LeaseKey:       acquire.LeaseKey,
		LeaseID:        resourceCapacityLeaseID(acquire),
		GenerationID:   acquire.GenerationID,
		ManifestSHA256: acquire.ManifestSHA256,
		QueueDeadline:  acquire.QueueDeadline,
		AcquiredAt:     now.Add(-time.Minute),
		ExpiresAt:      now,
	}
	binding := &productionPhaseBinding{cap: V1RuntimeCapabilities{
		Clock: func() time.Time { return now },
		ResourceCapacity: config.VerifiedResourceCapacity{
			GenerationID: lease.GenerationID, ManifestSHA256: manifest,
			Limits: config.ResourceCapacityLimits{ForecastEventMaxInflight: 2, ForecastEventAdmissionWaitSeconds: 5},
		},
		// An empty store would panic on Lookup; the expiry gate must return
		// before any Redis read or mutation is attempted.
		CapacityLeases: &redisstore.ThrottleStore{},
	}}
	_, err := binding.renewResourceCapacity(context.Background(), llm.ResourceCapacityRenewRequestV1{APIVersion: llm.ResourceCapacityRenewAPIVersion, Lease: lease})
	if err == nil || !strings.Contains(err.Error(), "expired before renewal") {
		t.Fatalf("expired resource capacity lease error = %v", err)
	}
}

func TestReleaseResourceCapacityIgnoresCallerCancellationAndReleasesOnce(t *testing.T) {
	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	manifest := strings.Repeat("b", 64)
	acquire := llm.ResourceCapacityAcquireRequestV1{
		APIVersion:     llm.ResourceCapacityAcquireAPIVersion,
		Context:        llm.RequestContext{Tenant: "customer", Project: "metaculus", Actor: "forecast-workflow"},
		ResourceClass:  llm.ResourceClassForecastEvent,
		LeaseKey:       "forecast-event:" + strings.Repeat("a", 64),
		GenerationID:   "capacity-1",
		ManifestSHA256: manifest,
		QueueDeadline:  now.Add(5 * time.Second),
	}
	lease := llm.ResourceCapacityLeaseV1{
		APIVersion:     llm.ResourceCapacityAcquireAPIVersion,
		Context:        acquire.Context,
		ResourceClass:  acquire.ResourceClass,
		LeaseKey:       acquire.LeaseKey,
		LeaseID:        resourceCapacityLeaseID(acquire),
		GenerationID:   acquire.GenerationID,
		ManifestSHA256: acquire.ManifestSHA256,
		QueueDeadline:  acquire.QueueDeadline,
		AcquiredAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	redis := &cancellationReleaseRedis{id: lease.LeaseID}
	leases, err := redisstore.NewThrottleStore(redisstore.ThrottleOptions{
		Invoker: redis, Reader: redis,
		Keys: redisstore.KeyOptions{Prefix: "worker", HashTag: "capacity", KeySecret: []byte(strings.Repeat("k", 32))},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding := &productionPhaseBinding{cap: V1RuntimeCapabilities{
		Clock: func() time.Time { return now },
		ResourceCapacity: config.VerifiedResourceCapacity{
			GenerationID: lease.GenerationID, ManifestSHA256: manifest,
			Limits: config.ResourceCapacityLimits{ForecastEventMaxInflight: 2, ForecastEventAdmissionWaitSeconds: 5},
		},
		CapacityLeases: leases,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response, err := binding.releaseResourceCapacity(ctx, llm.ResourceCapacityReleaseRequestV1{APIVersion: llm.ResourceCapacityReleaseAPIVersion, Lease: lease})
	if err != nil || !response.Released || response.LeaseID != lease.LeaseID {
		t.Fatalf("canceled release response = %#v error=%v", response, err)
	}
	if redis.lookups != 1 || redis.releases != 1 {
		t.Fatalf("canceled capacity lifecycle lookups=%d releases=%d, want exactly one each", redis.lookups, redis.releases)
	}
}

type cancellationReleaseRedis struct {
	id       string
	lookups  int
	releases int
}

func (redis *cancellationReleaseRedis) Get(ctx context.Context, _ string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	redis.lookups++
	encoded, err := json.Marshal(map[string]any{
		"schema": "throttle/v1", "id": redis.id, "digest": "digest",
		"limits": []map[string]any{{"kind": "concurrency", "key_digest": "key", "amount": 1}},
	})
	return string(encoded), err
}

func (redis *cancellationReleaseRedis) Run(ctx context.Context, _ string, _ []string, args ...string) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(args) == 0 || args[0] != "release" {
		return nil, errors.New("unexpected resource capacity mutation")
	}
	redis.releases++
	return []any{"released", ""}, nil
}

func TestBeforePossibleWriteFailsClosedWhenRedisFenceRejects(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	route := durableRouteFixture(t, now, now.Add(time.Second))
	route.Execution.Candidate.AttemptedClass = llm.ServiceClassStandard
	store := &transitionAdmissionStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateReserved, DispatchToken: "dispatch-token",
		ExpiresAt: now.Add(time.Hour),
	}}
	fenceErr := durablestore.ErrReservationNotFound
	materializer := &replayReservationMaterializer{fenceErr: fenceErr}
	observer := &productionDispatchObserver{
		binding: &productionPhaseBinding{
			cap: V1RuntimeCapabilities{Clock: func() time.Time { return now }},
			composition: durablestore.Composition{
				Operations: store, Materializer: materializer,
			},
		},
		operation: store.operation, route: route, candidate: route.Execution.Candidate,
	}
	err := observer.BeforePossibleWrite(context.Background())
	if !errors.Is(err, fenceErr) || observer.marked || len(materializer.fences) != 1 {
		t.Fatalf("rejected Redis fence did not fail closed: err=%v marked=%t fences=%d", err, observer.marked, len(materializer.fences))
	}
	if store.operation.State != admission.StateReserved || len(store.transitions) != 0 {
		t.Fatalf("rejected fence advanced dispatch state: state=%s transitions=%v", store.operation.State, store.transitions)
	}
}

func TestBeforePossibleWriteReleasesCapacityWhenReservationExpiresInQueue(t *testing.T) {
	now := time.Date(2026, 8, 11, 6, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Second)
	route := durableRouteFixture(t, now, expiresAt)
	route.Execution.Candidate.AttemptedClass = llm.ServiceClassStandard
	manifest := strings.Repeat("d", 64)
	route.ResourceCapacityGenerationID = "capacity-generation-1"
	route.ResourceCapacityManifestSHA256 = manifest
	invoker := &capacityExpiryInvoker{onAcquire: func() { now = expiresAt }}
	leases, err := redisstore.NewThrottleStore(redisstore.ThrottleOptions{
		Invoker: invoker, Reader: invoker,
		Keys: redisstore.KeyOptions{Prefix: "worker", HashTag: "capacity", KeySecret: []byte(strings.Repeat("k", 32))},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &transitionAdmissionStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateReserved, DispatchToken: "dispatch-token",
		ExpiresAt: expiresAt.Add(time.Hour),
	}}
	materializer := &replayReservationMaterializer{}
	observer := &productionDispatchObserver{
		binding: &productionPhaseBinding{
			cap: V1RuntimeCapabilities{
				Clock: nowClock(&now), ReservationLease: 3 * time.Minute,
				ResourceCapacity: config.VerifiedResourceCapacity{
					GenerationID: "capacity-generation-1", ManifestSHA256: manifest,
					Limits: config.ResourceCapacityLimits{
						LLMGlobalMaxInflight: 48, LLMGlobalMaxRequestsPerWindow: 120, LLMGlobalWindowSeconds: 60,
						ProviderCallQueueSeconds: 30,
						ProviderMaxInflight:      []config.ProviderInflightLimit{{RouteID: route.RouteID, MaxInflight: 24, MaxRequestsPerWindow: 60, WindowSeconds: 60}},
					},
				},
				CapacityLeases: leases,
			},
			composition: durablestore.Composition{Operations: store, Materializer: materializer},
		},
		operation: store.operation, route: route, candidate: route.Execution.Candidate,
	}
	err = observer.BeforePossibleWrite(context.Background())
	if err == nil || !strings.Contains(err.Error(), "expired while awaiting provider capacity") {
		t.Fatalf("capacity queue expiry error = %v", err)
	}
	if observer.marked || observer.capacity.ID != "" || len(store.transitions) != 0 {
		t.Fatalf("expired queue advanced dispatch: marked=%t capacity=%#v transitions=%v", observer.marked, observer.capacity, store.transitions)
	}
	if !reflect.DeepEqual(invoker.actions, []string{"acquire_fair", "release"}) {
		t.Fatalf("capacity lifecycle = %v, want acquire then one release", invoker.actions)
	}
}

type capacityExpiryInvoker struct {
	actions   []string
	onAcquire func()
}

func (invoker *capacityExpiryInvoker) Run(_ context.Context, _ string, _ []string, args ...string) ([]any, error) {
	if len(args) == 0 {
		return nil, errors.New("capacity test invocation has no action")
	}
	invoker.actions = append(invoker.actions, args[0])
	switch args[0] {
	case "acquire_fair":
		if invoker.onAcquire != nil {
			invoker.onAcquire()
		}
		return []any{"created", args[1]}, nil
	case "release":
		return []any{"released", ""}, nil
	default:
		return nil, errors.New("unexpected capacity test action")
	}
}

func (*capacityExpiryInvoker) Get(context.Context, string) (string, error) {
	return "", errors.New("unexpected capacity test lookup")
}

func nowClock(now *time.Time) func() time.Time {
	return func() time.Time { return *now }
}

func TestCompletedReconciliationRetryReusesPersistedTimestampAndEvents(t *testing.T) {
	completedAt := time.Date(2026, 8, 10, 12, 1, 0, 0, time.UTC)
	route := durableRouteFixture(t, completedAt.Add(-time.Minute), completedAt.Add(time.Minute))
	journal := &captureJournal{}
	materializer := &replayReservationMaterializer{result: *route.Reservation}
	store := &transitionAdmissionStore{operation: admission.Operation{ID: string(route.OperationID), State: admission.StateCompleted, CompletedAt: completedAt}}
	binding := &productionPhaseBinding{composition: durablestore.Composition{Operations: store, Journal: journal, Materializer: materializer}}
	actual := "0.1"
	finalization := durablestore.GenerateFinalization{Response: llm.GenerateResponseV1{Cost: llm.CostV1{Status: "exact", ActualCostUSD: &actual}}}
	for range 2 {
		if err := binding.reconcileGenerate(context.Background(), llm.GenerateRequestV1{}, route, *route.Reservation, finalization); err != nil {
			t.Fatal(err)
		}
	}
	if materializer.reconciles != 2 || len(journal.completions) != 2 || !reflect.DeepEqual(journal.completions[0], journal.completions[1]) || !journal.completions[0].OccurredAt.Equal(completedAt) {
		t.Fatalf("replayed completion changed deterministic facts: %#v", journal.completions)
	}
}

func TestFailedCompensationRetryReusesPersistedTimestampAndEvents(t *testing.T) {
	completedAt := time.Date(2026, 8, 10, 12, 1, 0, 0, time.UTC)
	route := durableRouteFixture(t, completedAt.Add(-time.Minute), completedAt.Add(time.Minute))
	journal := &captureJournal{}
	materializer := &replayReservationMaterializer{result: *route.Reservation}
	binding := &productionPhaseBinding{composition: durablestore.Composition{Journal: journal, Materializer: materializer}}
	operation := admission.Operation{ID: string(route.OperationID), State: admission.StateDefiniteFailed, CostStatus: "exact", CostMethod: "worker_cache_zero", CompletedAt: completedAt}
	for range 2 {
		if err := binding.finalizeFailedBudget(context.Background(), route, *route.Reservation, operation); err != nil {
			t.Fatal(err)
		}
	}
	if materializer.reconciles != 2 || len(journal.completions) != 2 || !reflect.DeepEqual(journal.completions[0], journal.completions[1]) || journal.completions[0].ReservationRevision != 2 {
		t.Fatalf("replayed compensation changed deterministic facts: %#v", journal.completions)
	}
}

func TestGenerateTerminalReplayDoesNotMaterializeExpiredParent(t *testing.T) {
	occurredAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	completedAt := occurredAt.Add(time.Minute)
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "terminal-generate",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  terminalCheckpointHandle("expired-parent"),
	}
	route, immutableFacts := terminalGenerateRouteFixture(t, request, occurredAt)
	expiredParent := &failingGenerateCheckpointMaterializer{err: errors.New("checkpoint expired")}
	journal := &captureJournal{}
	budgetState := &replayReservationMaterializer{}
	store := &terminalGenerateOperationStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateCompleted,
		ImmutableFacts: immutableFacts, CompletedAt: completedAt, ExpiresAt: occurredAt.Add(24 * time.Hour),
	}}
	actual := pricing.MustUSD("0.125")
	results := terminalGenerateResultStore{response: llm.Response{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: string(route.OperationID),
		Status: llm.ResponseStatusCompleted, Continuation: &llm.Continuation{Handle: "committed-checkpoint"},
		Cost: llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &actual, Method: "provider_reported"},
	}}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour, Clock: func() time.Time { return completedAt.Add(time.Hour) },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
				t.Fatal("terminal replay resolved an expired parent scope")
				return "", errors.New("unexpected scope resolution")
			},
			Checkpoints: CheckpointCapabilities{Materializer: expiredParent},
		},
		composition: durablestore.Composition{Operations: store, Results: results, Journal: journal, Materializer: budgetState},
	}
	first, err := durablestore.GenerateV1(context.Background(), request, binding.generatePorts())
	if err != nil {
		t.Fatalf("first terminal replay: %v", err)
	}
	second, err := durablestore.GenerateV1(context.Background(), request, binding.generatePorts())
	if err != nil {
		t.Fatalf("second terminal replay: %v", err)
	}
	if !reflect.DeepEqual(first, second) || first.Checkpoint.Handle != "committed-checkpoint" || first.Checkpoint.Parent == nil || *first.Checkpoint.Parent != *request.Parent {
		t.Fatalf("terminal replay changed committed result: first=%#v second=%#v", first, second)
	}
	if expiredParent.calls != 0 {
		t.Fatalf("expired parent materialization calls = %d, want 0", expiredParent.calls)
	}
	if budgetState.reconciles != 2 || len(journal.completions) != 2 || !reflect.DeepEqual(journal.completions[0], journal.completions[1]) {
		t.Fatalf("terminal replay did not idempotently reconcile: %#v", journal.completions)
	}
}

func TestGenerateTerminalFailureReplaysWithoutExpiredParent(t *testing.T) {
	occurredAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	completedAt := occurredAt.Add(time.Minute)
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "failed-generate",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  terminalCheckpointHandle("expired-parent"),
	}
	route, immutableFacts := terminalGenerateRouteFixture(t, request, occurredAt)
	expiredParent := &failingGenerateCheckpointMaterializer{err: errors.New("checkpoint expired")}
	journal := &captureJournal{}
	budgetState := &replayReservationMaterializer{}
	actual := pricing.MustUSD("0.125")
	store := &terminalGenerateOperationStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateDefiniteFailed, ImmutableFacts: immutableFacts,
		CompletedAt: completedAt, ExpiresAt: occurredAt.Add(24 * time.Hour), CostStatus: "exact",
		CostMethod: "provider_reported", ActualCostUSD: &actual, FailureReason: "provider_dispatch_failed",
	}}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour, Clock: func() time.Time { return completedAt.Add(time.Hour) },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
				t.Fatal("terminal failure resolved an expired parent scope")
				return "", errors.New("unexpected scope resolution")
			},
			Checkpoints: CheckpointCapabilities{Materializer: expiredParent},
		},
		composition: durablestore.Composition{Operations: store, Journal: journal, Materializer: budgetState},
	}
	var first string
	for attempt := range 2 {
		_, err := durablestore.GenerateV1(context.Background(), request, binding.generatePorts())
		if err == nil {
			t.Fatal("terminal failure replay returned success")
		}
		if attempt == 0 {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("terminal failure changed across replay:\nfirst: %s\nsecond: %s", first, err)
		}
	}
	if expiredParent.calls != 0 {
		t.Fatalf("expired parent materialization calls = %d, want 0", expiredParent.calls)
	}
	if budgetState.reconciles != 2 || len(journal.completions) != 2 || !reflect.DeepEqual(journal.completions[0], journal.completions[1]) {
		t.Fatalf("terminal failure did not idempotently reconcile: %#v", journal.completions)
	}
	if journal.completions[0].ActualCostUSD == nil || journal.completions[0].ActualCostUSD.Cmp(actual) != 0 {
		t.Fatalf("terminal failure lost persisted exact cost: %#v", journal.completions[0])
	}
}

func TestGenerateRootTerminalFailureReconcilesWithoutParentMaterialization(t *testing.T) {
	occurredAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	completedAt := occurredAt.Add(time.Minute)
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "failed-root-generate",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
	}
	route, immutableFacts := terminalGenerateRouteFixture(t, request, occurredAt)
	store := &terminalGenerateOperationStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateDefiniteFailed, ImmutableFacts: immutableFacts,
		CompletedAt: completedAt, ExpiresAt: occurredAt.Add(24 * time.Hour),
		CostStatus: "exact", CostMethod: "worker_cache_zero", FailureReason: "provider_dispatch_failed",
	}}
	journal := &captureJournal{}
	materializer := &replayReservationMaterializer{}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour, Clock: func() time.Time { return completedAt.Add(time.Hour) },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
				t.Fatal("root terminal failure attempted parent scope resolution")
				return "", errors.New("unexpected scope resolution")
			},
		},
		composition: durablestore.Composition{Operations: store, Journal: journal, Materializer: materializer},
	}
	if _, err := binding.replayGenerate(context.Background(), request); err == nil {
		t.Fatal("root terminal failure replay returned success")
	}
	if materializer.reconciles != 1 || len(journal.completions) != 1 || journal.completions[0].Kind != budget.JournalRelease {
		t.Fatalf("root terminal reconciliation = %#v, reconciles=%d", journal.completions, materializer.reconciles)
	}
}

func TestGenerateDispatchCapableReplayStillRequiresLiveParent(t *testing.T) {
	occurredAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "reserved-generate",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Parent:  terminalCheckpointHandle("expired-parent"),
	}
	route, immutableFacts := terminalGenerateRouteFixture(t, request, occurredAt)
	expiredParent := &failingGenerateCheckpointMaterializer{err: errors.New("checkpoint expired")}
	store := &terminalGenerateOperationStore{operation: admission.Operation{
		ID: string(route.OperationID), State: admission.StateReserved, ImmutableFacts: immutableFacts,
		ExpiresAt: occurredAt.Add(time.Hour),
	}}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour, Clock: func() time.Time { return occurredAt },
			ResolveScope: func(context.Context, llm.RequestContext) (string, error) { return "scope-1", nil },
			Checkpoints:  CheckpointCapabilities{Materializer: expiredParent},
		},
		composition: durablestore.Composition{Operations: store},
	}
	if _, err := binding.replayGenerate(context.Background(), request); err == nil || !strings.Contains(err.Error(), "checkpoint expired") {
		t.Fatalf("dispatch-capable replay bypassed live parent validation: %v", err)
	}
	if expiredParent.calls != 1 {
		t.Fatalf("dispatch-capable parent materialization calls = %d, want 1", expiredParent.calls)
	}
}

func TestBindRouteProvenanceOverridesProviderSelectedRouteFacts(t *testing.T) {
	response := llm.Response{
		Route:   llm.RouteFacts{RouteID: "provider-route", EndpointID: "provider-endpoint"},
		Service: llm.ServiceFacts{Requested: llm.ServiceClassEconomy, Attempted: llm.ServiceClassEconomy},
	}
	candidate := routing.Candidate{
		RouteID: "planned-route", EndpointID: "planned-endpoint", Family: "responses",
		Model: "resolved-model", RequestedClass: llm.ServiceClassStandard,
		AttemptedClass: llm.ServiceClassPriority, FallbackIndex: 2,
	}
	bindRouteProvenance(&response, llm.Request{Model: "requested-model"}, candidate)
	if response.Route.RouteID != candidate.RouteID || response.Route.EndpointID != candidate.EndpointID || response.Route.APIFamily != candidate.Family || response.Route.RequestedModel != "requested-model" || response.Route.ResolvedModel != candidate.Model {
		t.Fatalf("route provenance is incomplete: %#v", response.Route)
	}
	if response.Service.Requested != candidate.RequestedClass || response.Service.Attempted != candidate.AttemptedClass || response.Service.FallbackIndex != candidate.FallbackIndex {
		t.Fatalf("service provenance is incomplete: %#v", response.Service)
	}
}

func TestDurableDispatchRejectsHostedContinuationProviderNeutrally(t *testing.T) {
	request := llm.Request{Extensions: map[string]json.RawMessage{
		"openai.responses": json.RawMessage(`{"include":["reasoning.encrypted_content"]}`),
	}}
	prepared, err := prepareDurableDispatchRequest(request, routing.Candidate{Family: string(provider.FamilyOpenAIResponses)})
	if err != nil {
		t.Fatal(err)
	}
	if string(prepared.Extensions["openai.responses"]) != string(request.Extensions["openai.responses"]) {
		t.Fatal("provider-neutral hosted-state policy mutated adapter extensions")
	}
	request.Continuation = &llm.Continuation{Handle: "provider-owned-response-or-thread-id"}
	for _, family := range []provider.Family{provider.FamilyOpenAIResponses, provider.FamilyAnthropicMessages} {
		if _, err := prepareDurableDispatchRequest(request, routing.Candidate{Family: string(family)}); err == nil {
			t.Fatalf("durable %s dispatch accepted provider-hosted continuation state", family)
		}
	}
}

func TestCheckpointPublicationRejectsConfiguredItemAndDepthBoundsBeforeUpload(t *testing.T) {
	scopeID := "00000000-0000-0000-0000-000000000001"
	binding := &productionPhaseBinding{cap: V1RuntimeCapabilities{CheckpointLimits: state.MaterializeLimits{MaxItems: 1, MaxRows: 2, MaxBytes: 1024, MaxDepth: 1}}}
	if _, _, _, err := binding.publishCheckpoint(context.Background(), scopeID, nil, "operation-1", state.CheckpointGeneration, durablestore.GenerateReplay{}, nil, llm.SettingsPatchV1{}, []llm.Item{llm.Message{}, llm.Message{}}); err == nil {
		t.Fatal("configured checkpoint item bound was ignored")
	}
	parent := llm.CheckpointHandle("parent")
	replay := durablestore.GenerateReplay{State: state.MaterializedState{Depth: 1}}
	if _, _, _, err := binding.publishCheckpoint(context.Background(), scopeID, &parent, "operation-1", state.CheckpointGeneration, replay, nil, llm.SettingsPatchV1{}, nil); err == nil {
		t.Fatal("configured continuation depth bound was ignored")
	}
	binding.cap.CheckpointLimits.MaxDepth = int32(^uint32(0) >> 1)
	replay.State.Depth = binding.cap.CheckpointLimits.MaxDepth
	if _, _, _, err := binding.publishCheckpoint(context.Background(), scopeID, &parent, "operation-1", state.CheckpointGeneration, replay, nil, llm.SettingsPatchV1{}, nil); err == nil {
		t.Fatal("maximum continuation depth overflowed before the configured bound was enforced")
	}
}

func TestBoundedCheckpointItemCountRejectsArithmeticOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if count, err := boundedCheckpointItemCount(maxInt, maxInt); err != nil || count != maxInt {
		t.Fatalf("maximum valid item count = %d, %v", count, err)
	}
	if _, err := boundedCheckpointItemCount(maxInt, maxInt, 1); err == nil {
		t.Fatal("item count overflow was accepted")
	}
}

func TestGenerateCheckpointPreflightRejectsKnownBoundsBeforeRouteReservationJournalOrDispatch(t *testing.T) {
	parent := llm.CheckpointHandle("parent")
	limits := state.MaterializeLimits{MaxDepth: 2, MaxRows: 2, MaxItems: 2, MaxBytes: 1024}
	item := llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "bounded input"}}}
	tests := []struct {
		name    string
		request llm.GenerateRequestV1
		state   state.MaterializedState
	}{
		{

			name: "depth",
			request: llm.GenerateRequestV1{
				APIVersion: llm.APIVersion, OperationKey: "depth", Parent: &parent,
				Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
			},
			state: state.MaterializedState{Depth: limits.MaxDepth},
		},
		{
			name: "rows",
			request: llm.GenerateRequestV1{
				APIVersion: llm.APIVersion, OperationKey: "rows", Parent: &parent,
				Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
			},
			state: state.MaterializedState{Lineage: []state.Handle{"one", "two"}},
		},
		{
			name: "items",
			request: llm.GenerateRequestV1{
				APIVersion: llm.APIVersion, OperationKey: "items", Parent: &parent,
				Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
				Append:  []llm.Item{item},
			},
			state: state.MaterializedState{Items: []llm.Item{item, item}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &beginCaptureStore{}
			checkpoints := &staticGenerateCheckpointMaterializer{state: test.state}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					OperationRetention: time.Hour, CheckpointLimits: limits,
					Clock: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
					ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
						return "00000000-0000-0000-0000-000000000001", nil
					},
					Checkpoints: CheckpointCapabilities{Materializer: checkpoints},
				},
				composition: durablestore.Composition{Operations: store},
			}
			ports := binding.generatePorts()
			var routeCalls, reservationCalls, journalCalls, dispatchCalls int
			ports.Route = func(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay, durablestore.CompactionDecision) (durablestore.RoutePlan, error) {
				routeCalls++
				return durablestore.RoutePlan{}, errors.New("unexpected route")
			}
			ports.Reserve = func(context.Context, llm.GenerateRequestV1, durablestore.RoutePlan) (durablestore.ReserveResult, error) {
				reservationCalls++
				return durablestore.ReserveResult{}, errors.New("unexpected reservation")
			}
			ports.Journal = func(context.Context, llm.GenerateRequestV1, durablestore.RoutePlan, durablestore.ReserveResult) (durablestore.JournalReceipt, error) {
				journalCalls++
				return durablestore.JournalReceipt{}, errors.New("unexpected journal")
			}
			ports.Dispatch = func(context.Context, llm.GenerateRequestV1, durablestore.GenerateReplay, durablestore.RoutePlan, durablestore.JournalReceipt) (durablestore.DispatchResult, error) {
				dispatchCalls++
				return durablestore.DispatchResult{}, errors.New("unexpected provider dispatch")
			}
			_, firstErr := durablestore.GenerateV1(context.Background(), test.request, ports)
			if firstErr == nil {
				t.Fatal("known checkpoint bound violation succeeded")
			}
			var firstFailure *provider.Error
			if !errors.As(firstErr, &firstFailure) || firstFailure.Code != provider.CodeInvalidArgument ||
				firstFailure.Phase != provider.PhaseStateLoad || firstFailure.Dispatch != provider.DispatchNotDispatched ||
				firstFailure.Retry != provider.RetryNever || firstFailure.OperationID != store.operation.ID {
				t.Fatalf("checkpoint failure = %#v, %v", firstFailure, firstErr)
			}
			if store.operation.State != admission.StateDefiniteFailed || store.operation.FailureReason != checkpointBoundsFailureReason ||
				store.operation.CostStatus != "exact" || store.operation.CostMethod != "worker_cache_zero" ||
				store.failure.Certainty != admission.NotDispatched || store.failure.Attempt.Provider != "" {
				t.Fatalf("durable no-dispatch receipt = operation %#v failure %#v", store.operation, store.failure)
			}
			receipt := store.failure
			_, replayErr := durablestore.GenerateV1(context.Background(), test.request, ports)
			var replayFailure *provider.Error
			if !errors.As(replayErr, &replayFailure) ||
				replayFailure.Code != firstFailure.Code || replayFailure.Phase != firstFailure.Phase ||
				replayFailure.Dispatch != firstFailure.Dispatch || replayFailure.Retry != firstFailure.Retry ||
				replayFailure.OperationID != firstFailure.OperationID {
				t.Fatalf("replayed failure changed: first=%#v replay=%#v err=%v", firstFailure, replayFailure, replayErr)
			}
			if !reflect.DeepEqual(store.failure, receipt) || !reflect.DeepEqual(store.transitions, []string{"atomic_failed"}) {
				t.Fatalf("replay mutated durable receipt: transitions=%v receipt=%#v", store.transitions, store.failure)
			}
			if routeCalls != 0 || reservationCalls != 0 || journalCalls != 0 || dispatchCalls != 0 {
				t.Fatalf("post-preflight work ran: route=%d reservation=%d journal=%d dispatch=%d", routeCalls, reservationCalls, journalCalls, dispatchCalls)
			}
		})
	}
}
func TestGenerateProviderPayloadLimitPersistsReplayStableNoDispatchReceipt(t *testing.T) {
	model := "logical-model"
	request := llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: "payload-bound",
		Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
		Append: []llm.Item{llm.Message{
			Actor:   llm.ActorHuman,
			Content: []llm.Part{llm.TextPart{Text: strings.Repeat("bounded-history-", 32)}},
		}},
		SettingsPatch: llm.SettingsPatchV1{Model: llm.Patch[string]{Set: &model}},
	}
	store := &beginCaptureStore{}
	binding := &productionPhaseBinding{
		cap: V1RuntimeCapabilities{
			OperationRetention: time.Hour, MaxRequestBytes: 128,
			CheckpointLimits: state.MaterializeLimits{MaxDepth: 4, MaxRows: 4, MaxItems: 8, MaxBytes: 4096},
			Clock:            func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
		},
		composition: durablestore.Composition{Operations: store},
	}
	_, firstErr := binding.replayGenerate(context.Background(), request)
	var firstFailure *provider.Error
	if !errors.As(firstErr, &firstFailure) || firstFailure.Code != provider.CodeInvalidArgument ||
		firstFailure.Phase != provider.PhaseNormalize || firstFailure.Dispatch != provider.DispatchNotDispatched ||
		firstFailure.Retry != provider.RetryNever {
		t.Fatalf("payload failure = %#v, %v", firstFailure, firstErr)
	}
	receipt := store.failure
	_, replayErr := binding.replayGenerate(context.Background(), request)
	var replayFailure *provider.Error
	if !errors.As(replayErr, &replayFailure) || replayFailure.OperationID != firstFailure.OperationID ||
		replayFailure.Code != firstFailure.Code || replayFailure.Phase != firstFailure.Phase ||
		replayFailure.Dispatch != firstFailure.Dispatch || replayFailure.Retry != firstFailure.Retry {
		t.Fatalf("payload replay changed: first=%#v replay=%#v err=%v", firstFailure, replayFailure, replayErr)
	}
	if !reflect.DeepEqual(store.failure, receipt) || len(store.transitions) != 1 ||
		store.operation.CostStatus != "exact" || store.operation.CostMethod != "worker_cache_zero" ||
		store.operation.Attempt.Provider != "" {
		t.Fatalf("payload replay mutated no-provider receipt: operation=%#v failure=%#v transitions=%v", store.operation, store.failure, store.transitions)
	}
}

func TestOutputOnlyCheckpointViolationPersistsCostReconcilesAndReplaysStably(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	exact := pricing.MustUSD("0.125")
	tests := []struct {
		name       string
		cost       llm.Cost
		wantStatus string
		wantKind   budget.JournalEventKind
	}{
		{
			name:       "exact",
			cost:       llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &exact, Method: "provider_reported"},
			wantStatus: "exact", wantKind: budget.JournalFinalizeExact,
		},
		{
			name:       "unknown",
			cost:       llm.Cost{Status: llm.CostStatusUnknown},
			wantStatus: "unknown", wantKind: budget.JournalFinalizeUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := llm.GenerateRequestV1{
				APIVersion: llm.APIVersion, OperationKey: "output-limit-" + test.name,
				Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
			}
			route, immutableFacts := terminalGenerateRouteFixture(t, request, now)
			store := &postResponseOperationStore{transitionAdmissionStore: transitionAdmissionStore{operation: admission.Operation{
				ID: string(route.OperationID), State: admission.StateDispatching, DispatchToken: "dispatch-token",
				ImmutableFacts: immutableFacts, ExpiresAt: now.Add(time.Hour),
			}}}
			journal := &captureJournal{}
			materializer := &replayReservationMaterializer{result: *route.Reservation}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					OperationRetention: time.Hour, CheckpointRetention: time.Hour,
					CheckpointLimits: state.MaterializeLimits{MaxDepth: 2, MaxRows: 2, MaxItems: 1, MaxBytes: 1024},
					Clock:            func() time.Time { return now },
					ResolveScope: func(context.Context, llm.RequestContext) (string, error) {
						return "00000000-0000-0000-0000-000000000001", nil
					},
				},
				composition: durablestore.Composition{Operations: store, Journal: journal, Materializer: materializer},
			}
			dispatch := durablestore.DispatchResult{Response: llm.Response{
				Cost: test.cost,
				Output: []llm.Item{
					llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "first"}}},
					llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "second"}}},
				},
			}}
			if _, err := binding.finalizeGenerate(context.Background(), request, durablestore.GenerateReplay{}, route, *route.Reservation, dispatch); err == nil {
				t.Fatal("response-only checkpoint bound violation succeeded")
			}
			if store.operation.State != admission.StateDefiniteFailed || store.failure.Certainty != admission.Accepted || !store.failure.PostResponse || store.failure.CostStatus != test.wantStatus {
				t.Fatalf("post-response terminal failure = %#v, operation=%#v", store.failure, store.operation)
			}
			if materializer.reconciles != 1 || len(journal.completions) != 1 || journal.completions[0].Kind != test.wantKind {
				t.Fatalf("initial accounting was not reconciled: %#v", journal.completions)
			}
			accounted := journal.completions[0]
			if test.wantStatus == "exact" {
				if accounted.ActualCostUSD == nil || accounted.ActualCostUSD.Cmp(exact) != 0 || accounted.AccountedIncreaseUSD.Cmp(exact) != 0 {
					t.Fatalf("exact provider result was not accounted: %#v", accounted)
				}
			} else if accounted.ActualCostUSD != nil || accounted.AccountedIncreaseUSD.Cmp(route.Execution.EstimatedUSD) != 0 || accounted.UnknownReasonCode == "" {
				t.Fatalf("unknown provider result was not conservatively accounted: %#v", accounted)
			}
			if _, err := binding.replayGenerate(context.Background(), request); err == nil {
				t.Fatal("terminal output-bound failure replay returned success")
			}
			if store.failCalls != 1 || materializer.reconciles != 2 || len(journal.completions) != 2 || !reflect.DeepEqual(journal.completions[0], journal.completions[1]) {
				t.Fatalf("terminal accounting replay changed: failCalls=%d reconciles=%d events=%#v", store.failCalls, materializer.reconciles, journal.completions)
			}
		})
	}
}

func TestPreWriteInvokeFailurePersistsAtomicTerminalBeforeCompensation(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	reserved := pricing.MustUSD("0.2")
	store := &transitionAdmissionStore{operation: admission.Operation{ID: "operation-1", State: admission.StateReserved, DispatchToken: "token"}}
	materializer := &transitionMaterializer{}
	reservation := durablestore.ReserveResult{
		OperationID: "operation-1", Accepted: true, GenerationID: "generation-1", IncarnationID: "incarnation-1",
		Events: []budget.ReservationEvent{(budgetReservationFixture{}).event("event-a", "window-a", reserved, now)},
	}
	route := durablestore.RoutePlan{
		OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1",
		EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1",
		Reservation: &reservation, ReservationExpiresAt: now.Add(time.Hour),
		Execution: &durablestore.RouteExecution{EstimatedUSD: reserved},
	}
	binding := &productionPhaseBinding{
		cap:         V1RuntimeCapabilities{Clock: func() time.Time { return now }},
		composition: durablestore.Composition{Operations: store, Journal: builderJournal{}, Materializer: materializer},
	}
	cause := errors.New("invoke failed before write")
	err := binding.failProviderAttempt(context.Background(), route, store.operation, 0, admission.NotDispatched, cause)
	if !errors.Is(err, cause) {
		t.Fatalf("failure cause = %v, want %v", err, cause)
	}
	if !reflect.DeepEqual(store.transitions, []string{"atomic_failed"}) || store.failure.Attempt.RouteID != route.RouteID || store.failure.Attempt.EndpointID != route.EndpointID || store.failure.Attempt.Provider != route.Provider || store.failure.Certainty != admission.NotDispatched || materializer.reconciliations != 1 {
		t.Fatalf("atomic failure/compensation = %#v, transitions=%v, reconciliations=%d", store.failure, store.transitions, materializer.reconciliations)
	}
	rejected := &transitionAdmissionStore{operation: admission.Operation{ID: "operation-1", State: admission.StateReserved, DispatchToken: "token"}}
	rejectedMaterializer := &transitionMaterializer{}
	binding.composition.Operations = rejected
	binding.composition.Materializer = rejectedMaterializer
	if err := binding.failProviderAttempt(context.Background(), route, rejected.operation, 0, admission.Rejected, cause); !errors.Is(err, cause) {
		t.Fatalf("rejected pre-write failure cause = %v, want %v", err, cause)
	}
	if !reflect.DeepEqual(rejected.transitions, []string{"atomic_failed"}) || rejected.failure.Certainty != admission.Rejected || rejectedMaterializer.reconciliations != 1 {
		t.Fatalf("rejected atomic failure = %#v, transitions=%v, reconciliations=%d", rejected.failure, rejected.transitions, rejectedMaterializer.reconciliations)
	}

	store = &transitionAdmissionStore{operation: admission.Operation{ID: "operation-1", State: admission.StateReserved, DispatchToken: "token"}, failErr: errors.New("persist failed")}
	materializer = &transitionMaterializer{}
	binding.composition.Operations = store
	binding.composition.Materializer = materializer
	if err := binding.failProviderAttempt(context.Background(), route, store.operation, 0, admission.NotDispatched, cause); err == nil || !errors.Is(err, store.failErr) {
		t.Fatalf("persistence failure was ignored: %v", err)
	}
	if materializer.reconciliations != 0 {
		t.Fatal("budget compensation ran before failed transition persisted")
	}
}

func TestDispatchPreflightFailuresAtomicallyTerminalizeWithoutInvokingProvider(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	lookupErr := errors.New("adapter lookup failed")
	capabilityErr := errors.New("capability lookup failed")
	compileErr := errors.New("compile failed")
	tests := []struct {
		name          string
		registryErr   error
		capabilityErr error
		compileErr    error
		want          error
	}{
		{name: "adapter lookup", registryErr: lookupErr, want: lookupErr},
		{name: "capabilities", capabilityErr: capabilityErr, want: capabilityErr},
		{name: "compile", compileErr: compileErr, want: compileErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := durableRouteFixture(t, now, now.Add(time.Hour))
			route.Execution.Request = llm.Request{OperationKey: "operation-key", Model: route.Model, ServiceClass: llm.ServiceClassStandard}
			store := &transitionAdmissionStore{operation: admission.Operation{ID: string(route.OperationID), State: admission.StateReserved, DispatchToken: "token"}}
			materializer := &transitionMaterializer{}
			adapter := &preflightFailureAdapter{capabilityErr: test.capabilityErr, compileErr: test.compileErr}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					Clock:    func() time.Time { return now },
					Adapters: preflightAdapterRegistry{adapter: adapter, err: test.registryErr},
				},
				composition: durablestore.Composition{Operations: store, Journal: builderJournal{}, Materializer: materializer},
			}
			dispatchRequest := llm.GenerateRequestV1{CostAdmission: &llm.CostAdmissionV1{GatewayAttemptOrdinal: 41}}
			_, err := binding.dispatchGenerate(context.Background(), dispatchRequest, durablestore.GenerateReplay{}, route, durablestore.JournalReceipt{})
			if !errors.Is(err, test.want) {
				t.Fatalf("dispatchGenerate() error = %v, want %v", err, test.want)
			}
			if !reflect.DeepEqual(store.transitions, []string{"atomic_failed"}) || store.operation.State != admission.StateDefiniteFailed || store.failure.Attempt.AttemptNumber != 41 || adapter.invokes != 0 || materializer.reconciliations != 1 {
				t.Fatalf("preflight outcome: transitions=%v operation=%#v attempt=%d invokes=%d reconciliations=%d", store.transitions, store.operation, store.failure.Attempt.AttemptNumber, adapter.invokes, materializer.reconciliations)
			}
		})
	}
}

func TestDispatchReservationProofFailuresAtomicallyTerminalizeAndReconcile(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	confirmErr := errors.New("reservation confirmation failed")
	fenceErr := durablestore.ErrReservationNotFound
	tests := []struct {
		name        string
		confirmErr  error
		fenceErr    error
		want        error
		wantInvokes int
	}{
		{name: "confirmation", confirmErr: confirmErr, want: confirmErr},
		{name: "dispatch fence", fenceErr: fenceErr, want: fenceErr, wantInvokes: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := durableRouteFixture(t, now, now.Add(time.Hour))
			route.Execution.Candidate.AttemptedClass = llm.ServiceClassStandard
			route.Execution.Request = llm.Request{
				APIVersion: llm.APIVersion, OperationKey: "operation-key",
				Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"},
				Model:   "requested-model", ServiceClass: llm.ServiceClassStandard,
				Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "question"}}}},
			}
			store := &transitionAdmissionStore{operation: admission.Operation{
				ID: string(route.OperationID), State: admission.StateReserved, DispatchToken: "token",
				ExpiresAt: now.Add(2 * time.Hour),
			}}
			materializer := &replayReservationMaterializer{
				result: *route.Reservation, confirmErr: test.confirmErr, fenceErr: test.fenceErr,
			}
			adapter := &observerInvokingAdapter{}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					Clock:    func() time.Time { return now },
					Adapters: preflightAdapterRegistry{adapter: adapter},
				},
				composition: durablestore.Composition{
					Operations: store, Journal: builderJournal{}, Materializer: materializer,
				},
			}
			_, err := binding.dispatchGenerate(context.Background(), llm.GenerateRequestV1{}, durablestore.GenerateReplay{}, route, durablestore.JournalReceipt{})
			if !errors.Is(err, test.want) {
				t.Fatalf("dispatch reservation proof error = %v, want %v", err, test.want)
			}
			if adapter.compileInput.Request.Model != route.Model || adapter.compileInput.Query.Model != route.Model {
				t.Fatalf("provider compile models: request=%q query=%q want resolved %q", adapter.compileInput.Request.Model, adapter.compileInput.Query.Model, route.Model)
			}
			if route.Execution.Request.Model != "requested-model" {
				t.Fatalf("provider compile mutated requested model provenance: %q", route.Execution.Request.Model)
			}
			if !reflect.DeepEqual(store.transitions, []string{"atomic_failed"}) ||
				store.operation.State != admission.StateDefiniteFailed ||
				adapter.invokes != test.wantInvokes || materializer.reconciles != 1 {
				t.Fatalf("proof failure outcome: transitions=%v state=%s invokes=%d reconciles=%d", store.transitions, store.operation.State, adapter.invokes, materializer.reconciles)
			}
		})
	}
}

type observerInvokingAdapter struct {
	invokes      int
	compileInput provider.CompileInput
}

func (*observerInvokingAdapter) Name() string { return "observer-invoking" }

func (*observerInvokingAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, nil
}

func (adapter *observerInvokingAdapter) Compile(_ context.Context, input provider.CompileInput) (provider.Call, error) {
	adapter.compileInput = input
	return provider.Call{}, nil
}

func (adapter *observerInvokingAdapter) Invoke(ctx context.Context, _ provider.Call, observer provider.Observer) (provider.Result, error) {
	adapter.invokes++
	if err := observer.BeforePossibleWrite(ctx); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{}, errors.New("unexpected successful dispatch fence")
}

type preflightAdapterRegistry struct {
	adapter provider.Adapter
	err     error
}

func (registry preflightAdapterRegistry) Adapter(context.Context, routing.Candidate) (provider.Adapter, error) {
	return registry.adapter, registry.err
}

type preflightFailureAdapter struct {
	capabilityErr error
	compileErr    error
	invokes       int
}

func (*preflightFailureAdapter) Name() string { return "preflight-failure" }

func (adapter *preflightFailureAdapter) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, adapter.capabilityErr
}

func (adapter *preflightFailureAdapter) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{}, adapter.compileErr
}

func (adapter *preflightFailureAdapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	adapter.invokes++
	return provider.Result{}, errors.New("unexpected provider invocation")
}

type beginCaptureStore struct {
	transitionAdmissionStore
	request admission.BeginRequest
}

func (store *beginCaptureStore) Begin(_ context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	store.request = request
	if store.operation.ID != "" {
		return admission.BeginResult{Operation: store.operation.Clone(), Existing: true}, nil
	}
	store.operation = admission.Operation{
		ID: request.ID, State: admission.StateReserved, DispatchToken: "dispatch-token",
		ExpiresAt: request.ExpiresAt,
	}
	return admission.BeginResult{Operation: store.operation.Clone()}, nil
}

type transitionAdmissionStore struct {
	operation   admission.Operation
	transitions []string
	failErr     error
	failure     admission.FailRequest
}

func (store *transitionAdmissionStore) Begin(context.Context, admission.BeginRequest) (admission.BeginResult, error) {
	return admission.BeginResult{}, errors.New("unexpected Begin")
}

func (store *transitionAdmissionStore) MarkDispatching(_ context.Context, request admission.DispatchRequest) error {
	if store.operation.State != admission.StateReserved || request.DispatchToken != store.operation.DispatchToken {
		return admission.ErrInvalidTransition
	}
	store.operation.State = admission.StateDispatching
	store.transitions = append(store.transitions, "dispatching")
	return nil
}

func (store *transitionAdmissionStore) Continue(context.Context, admission.ContinueRequest) (admission.ContinueResult, error) {
	return admission.ContinueResult{}, errors.New("unexpected Continue")
}

func (store *transitionAdmissionStore) Complete(context.Context, admission.CompleteRequest) error {
	return errors.New("unexpected Complete")
}

func (store *transitionAdmissionStore) Fail(_ context.Context, request admission.FailRequest) error {
	if store.operation.State != admission.StateDispatching || request.DispatchToken != store.operation.DispatchToken {
		return admission.ErrInvalidTransition
	}
	if store.failErr != nil {
		return store.failErr
	}
	store.operation.State = admission.StateDefiniteFailed
	store.operation.CompletedAt = time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	store.operation.CostStatus = "exact"
	store.operation.CostMethod = "worker_cache_zero"
	store.transitions = append(store.transitions, "failed")
	return nil
}

func (store *transitionAdmissionStore) FailBeforeDispatch(_ context.Context, request admission.FailRequest) error {
	if store.operation.State != admission.StateReserved || request.DispatchToken != store.operation.DispatchToken {
		return admission.ErrInvalidTransition
	}
	if request.Certainty != admission.NotDispatched && request.Certainty != admission.Rejected {
		return admission.ErrInvalidTransition
	}
	if store.failErr != nil {
		return store.failErr
	}
	store.failure = request
	store.operation.State = admission.StateDefiniteFailed
	store.operation.CompletedAt = time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	store.operation.CostStatus = "exact"
	store.operation.CostMethod = "worker_cache_zero"
	store.operation.FailureReason = request.Reason
	store.operation.Attempt = request.Attempt
	zero := pricing.MustUSD("0")
	store.operation.IncurredCostUSD = &zero
	store.operation.ActualCostUSD = &zero
	store.transitions = append(store.transitions, "atomic_failed")
	return nil
}

func (store *transitionAdmissionStore) Get(context.Context, string) (admission.Operation, error) {
	return store.operation, nil
}

type postResponseOperationStore struct {
	transitionAdmissionStore
	failure   admission.FailRequest
	failCalls int
}

func (store *postResponseOperationStore) Begin(context.Context, admission.BeginRequest) (admission.BeginResult, error) {
	return admission.BeginResult{Operation: store.operation.Clone(), Existing: true}, nil
}

func (store *postResponseOperationStore) Fail(_ context.Context, request admission.FailRequest) error {
	if store.operation.State != admission.StateDispatching || request.DispatchToken != store.operation.DispatchToken {
		return admission.ErrInvalidTransition
	}
	if !request.PostResponse || request.Certainty != admission.Accepted {
		return errors.New("post-response accepted failure is required")
	}
	store.failure = request
	store.failCalls++
	store.operation.State = admission.StateDefiniteFailed
	store.operation.CompletedAt = time.Date(2026, 8, 10, 12, 0, 1, 0, time.UTC)
	store.operation.CostStatus = request.CostStatus
	store.operation.CostMethod = request.CostMethod
	store.operation.CostUnknownReason = request.UnknownReason
	store.operation.FailureReason = request.Reason
	if request.CostStatus == "exact" {
		actual := request.IncurredCostUSD
		store.operation.ActualCostUSD = &actual
	}
	return nil
}

type staticGenerateCheckpointMaterializer struct {
	state state.MaterializedState
}

func (materializer *staticGenerateCheckpointMaterializer) Materialize(context.Context, string, state.CheckpointID, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.state, nil
}

func (materializer *staticGenerateCheckpointMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	return materializer.state, nil
}

type transitionMaterializer struct {
	reconciliations int
}

func (*transitionMaterializer) Accept(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	return durablestore.ReserveResult{}, errors.New("recovered reservation was not reused")
}

func (*transitionMaterializer) Confirm(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	return durablestore.ReserveResult{}, errors.New("recovered reservation confirmation is unavailable")
}

func (*transitionMaterializer) FenceDispatch(context.Context, durablestore.DispatchFenceRequest) error {
	return errors.New("unexpected dispatch fence")
}
func (materializer *transitionMaterializer) Reconcile(context.Context, durablestore.ReconcileRequest) error {
	materializer.reconciliations++
	return nil
}

func TestCompletionEventsPreserveExactUSDForEveryBudgetWindow(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	actual := "0.125"
	reserved := pricing.MustUSD("0.2")
	actualUSD := pricing.MustUSD(actual)
	route := durablestore.RoutePlan{OperationID: "operation-1", GenerationID: "generation-1", Execution: &durablestore.RouteExecution{EstimatedUSD: reserved}}
	reservation := durablestore.ReserveResult{Events: []budget.ReservationEvent{(budgetReservationFixture{}).event("event-a", "window-a", reserved, now), (budgetReservationFixture{}).event("event-b", "window-b", reserved, now)}}
	events, err := completionEvents(route, reservation, llm.CostV1{Status: "exact", ActualCostUSD: &actual, Method: "provider_reported"}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("completionEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("completion event count = %d, want 2", len(events))
	}
	for _, event := range events {
		if event.Kind != budget.JournalFinalizeExact || event.ActualCostUSD == nil || event.ActualCostUSD.Cmp(actualUSD) != 0 || event.AccountedIncreaseUSD.Cmp(actualUSD) != 0 || event.ReservedDecreaseUSD.Cmp(reserved) != 0 {
			t.Fatalf("completion event lost exact accounting: %#v", event)
		}
		if event.ReservationRevision != 2 || !event.OccurredAt.Equal(now.Add(time.Second)) {
			t.Fatalf("completion event lost stable revision/timestamp: %#v", event)
		}
		if err := event.Validate(); err != nil {
			t.Fatalf("completion event invalid: %v", err)
		}
	}
}

type budgetReservationFixture struct{}

func (budgetReservationFixture) event(id, window string, amount pricing.USD, at time.Time) budget.ReservationEvent {
	return budget.ReservationEvent{EventID: id, GenerationID: "generation-1", OperationID: "operation-1", WindowID: window, BucketStart: at, ReservationRevision: 1, AmountUSD: amount, OccurredAt: at}
}

type replayReservationMaterializer struct {
	result         durablestore.ReserveResult
	accepts        int
	confirms       int
	confirmErr     error
	reconciles     int
	fences         []durablestore.DispatchFenceRequest
	fenceErr       error
	onFence        func()
	confirmRequest durablestore.ReserveRequest
}

func (materializer *replayReservationMaterializer) Accept(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	materializer.accepts++
	return materializer.result, nil
}

func (materializer *replayReservationMaterializer) Confirm(_ context.Context, request durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	materializer.confirms++
	materializer.confirmRequest = request
	return materializer.result, materializer.confirmErr
}

func (materializer *replayReservationMaterializer) FenceDispatch(_ context.Context, request durablestore.DispatchFenceRequest) error {
	if materializer.onFence != nil {
		materializer.onFence()
	}
	materializer.fences = append(materializer.fences, request)
	return materializer.fenceErr
}

func (materializer *replayReservationMaterializer) Reconcile(context.Context, durablestore.ReconcileRequest) error {
	materializer.reconciles++
	return nil
}

type immutableCaptureStore struct {
	transitionAdmissionStore
	calls *[]string
}

func (store *immutableCaptureStore) Begin(_ context.Context, request admission.BeginRequest) (admission.BeginResult, error) {
	*store.calls = append(*store.calls, "persist")
	store.operation = admission.Operation{ID: request.ID, ImmutableFacts: append([]byte(nil), request.ImmutableFacts...), ExpiresAt: request.ExpiresAt}
	return admission.BeginResult{Operation: store.operation, Existing: true}, nil
}

type orderingMaterializer struct {
	calls  *[]string
	result durablestore.ReserveResult
}

func (materializer *orderingMaterializer) Accept(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	*materializer.calls = append(*materializer.calls, "accept")
	return materializer.result, nil
}

func (materializer *orderingMaterializer) Confirm(context.Context, durablestore.ReserveRequest) (durablestore.ReserveResult, error) {
	*materializer.calls = append(*materializer.calls, "confirm")
	return materializer.result, nil
}

func (*orderingMaterializer) FenceDispatch(context.Context, durablestore.DispatchFenceRequest) error {
	return nil
}

func (*orderingMaterializer) Reconcile(context.Context, durablestore.ReconcileRequest) error {
	return nil
}

type captureJournal struct {
	completions []budget.CompletionEvent
}

func (*captureJournal) AppendReservation(context.Context, budget.ReservationEvent) (postgresstore.JournalRecord, error) {
	return postgresstore.JournalRecord{}, nil
}

func (journal *captureJournal) AppendCompletion(_ context.Context, event budget.CompletionEvent) (postgresstore.JournalRecord, error) {
	journal.completions = append(journal.completions, event)
	return postgresstore.JournalRecord{}, nil
}

type terminalGenerateOperationStore struct {
	transitionAdmissionStore
	operation admission.Operation
}

func (store *terminalGenerateOperationStore) Begin(context.Context, admission.BeginRequest) (admission.BeginResult, error) {
	return admission.BeginResult{Operation: store.operation.Clone(), Existing: true}, nil
}

func (store *terminalGenerateOperationStore) Get(context.Context, string) (admission.Operation, error) {
	return store.operation.Clone(), nil
}

type terminalGenerateResultStore struct {
	response llm.Response
}

func (store terminalGenerateResultStore) Get(context.Context, string) (llm.Response, error) {
	return store.response, nil
}

func (terminalGenerateResultStore) Put(context.Context, string, llm.Response) (state.BlobRef, error) {
	return state.BlobRef{}, errors.New("terminal replay unexpectedly persisted a result")
}

type failingGenerateCheckpointMaterializer struct {
	err   error
	calls int
}

func (materializer *failingGenerateCheckpointMaterializer) Materialize(context.Context, string, state.CheckpointID, state.MaterializeLimits) (state.MaterializedState, error) {
	materializer.calls++
	return state.MaterializedState{}, materializer.err
}

func (materializer *failingGenerateCheckpointMaterializer) MaterializeHandle(context.Context, string, string, state.MaterializeLimits) (state.MaterializedState, error) {
	materializer.calls++
	return state.MaterializedState{}, materializer.err
}

func terminalCheckpointHandle(value string) *llm.CheckpointHandle {
	handle := llm.CheckpointHandle(value)
	return &handle
}

func terminalGenerateRouteFixture(t *testing.T, request llm.GenerateRequestV1, occurredAt time.Time) (durablestore.RoutePlan, []byte) {
	t.Helper()
	expiresAt := occurredAt.Add(time.Hour)
	reserved := pricing.MustUSD("0.2")
	operationID := strictOperationIdentity(rawScope(request.Context), request.Context.Actor, "generate", llm.APIVersion, request.OperationKey)
	reservations := []admission.WindowReservation{{
		PolicyID: "policy-1", WindowID: "window-1", Bucket: occurredAt.Unix(),
		Amount: 200000, Limit: 1000000, AmountUSD: reserved, LimitUSD: pricing.MustUSD("1"),
		BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour),
	}}
	reservation, err := durablestore.PlannedReserveResult(durablestore.ReserveRequest{
		OperationID: operationID, GenerationID: "generation-1", IncarnationID: "incarnation-1",
		Reservations: reservations, ExpiresAt: expiresAt, OccurredAt: occurredAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := routing.Candidate{
		RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1",
		Model: "model-1", AttemptedClass: llm.ServiceClassStandard,
	}
	route := durablestore.RoutePlan{
		OperationID: operationID, GenerationID: "generation-1",
		RouteID: candidate.RouteID, EndpointID: candidate.EndpointID, Provider: candidate.Provider,
		Model: candidate.Model, PriceVersion: "price-1", ReservationExpiresAt: expiresAt,
		Reservation: &reservation,
		Execution: &durablestore.RouteExecution{
			Candidate: candidate, Price: pricing.Entry{Version: "price-1"},
			Reservations: reservations, EstimatedUSD: reserved,
		},
	}
	facts, err := persistedFactsForRoute(route, reservation)
	if err != nil {
		t.Fatal(err)
	}
	immutableFacts, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	return route, immutableFacts
}

func durableRouteFixture(t *testing.T, occurredAt, expiresAt time.Time) durablestore.RoutePlan {
	t.Helper()
	reserved := pricing.MustUSD("0.2")
	route := durablestore.RoutePlan{
		OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1", PriceVersion: "price-1", ReservationExpiresAt: expiresAt,
		Execution: &durablestore.RouteExecution{
			Candidate:    routing.Candidate{RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider-1", Model: "model-1"},
			Reservations: []admission.WindowReservation{{PolicyID: "policy-1", WindowID: "window-1", Bucket: occurredAt.Unix(), Amount: 200000, Limit: 1000000, AmountUSD: reserved, LimitUSD: pricing.MustUSD("1"), BucketNanos: int64(time.Second), DurationNanos: int64(time.Hour)}},
			EstimatedUSD: reserved,
		},
	}
	reservation, err := durablestore.PlannedReserveResult(durablestore.ReserveRequest{
		OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: "incarnation-1",
		Reservations: route.Execution.Reservations, ExpiresAt: expiresAt, OccurredAt: occurredAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	route.Reservation = &reservation
	return route
}
