//go:build integration

package runtime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	durablestore "github.com/mfow/llm-temporal-worker/golang/storage/durable"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

// Exercises the production reserve/allocate/sign/Generate-confirm path against
// the actual Redis Function. Only the unrelated PostgreSQL facts sink is a
// capture fixture; no grant lookup or accounting port is replaced.
func TestLiveRedisProductionBatchGrantMaterializationIdentity(t *testing.T) {
	address := os.Getenv("LLMTW_REDIS_ADDR")
	if address == "" {
		t.Skip("set LLMTW_REDIS_ADDR to an isolated Redis to run the durable grant gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("LLMTW_REDIS_TEST_PROVISION") == "1" {
		if err := client.FunctionLoad(ctx, redisstore.AdmissionFunctionSource()).Err(); err != nil && !strings.Contains(err.Error(), "already exists") {
			t.Fatal(err)
		}
	}
	for _, scenario := range []struct {
		mode  string
		count int
	}{{"direct", 1}, {"template", 1}, {"fractional-template", 1}, {"direct", 65}, {"template", 65}, {"direct", 1024}, {"template", 1024}} {
		t.Run(fmt.Sprintf("%s-%d", scenario.mode, scenario.count), func(t *testing.T) {
			mode := scenario.mode
			now := time.Now().UTC().Truncate(time.Second)
			clock := func() time.Time { return now }
			prefix := fmt.Sprintf("live-grant-%s-%d-%d", mode, scenario.count, time.Now().UnixNano())
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				var cursor uint64
				for {
					keys, next, err := client.Scan(cleanupCtx, cursor, prefix+":*", 100).Result()
					if err != nil {
						t.Error(err)
						return
					}
					if len(keys) > 0 {
						if err := client.Del(cleanupCtx, keys...).Err(); err != nil {
							t.Error(err)
							return
						}
					}
					cursor = next
					if cursor == 0 {
						return
					}
				}
			})
			materializer, err := redisstore.NewRedisBudgetMaterializer(redisstore.RedisBudgetMaterializerOptions{
				Client: client, Mode: redisstore.AdmissionModeFunction,
				Keys:         redisstore.KeyOptions{Prefix: prefix, HashTag: "admission", KeySecret: []byte(strings.Repeat("k", 32))},
				GenerationID: "generation-grant", IncarnationID: "incarnation-grant", Clock: clock,
			})
			if err != nil {
				t.Fatal(err)
			}
			entry := testPriceEntry("endpoint-a", "gpt-test", "standard")
			entry.Version = "prices-v1"
			if mode == "fractional-template" {
				entry.Prices.InputPerMillion = pricing.MustDecimalUSD("0.0001")
				entry.Prices.OutputPerMillion = pricing.MustDecimalUSD("0.0001")
			}
			catalog, err := pricing.CompileUSD("prices-v1", []pricing.Entry{entry})
			if err != nil {
				t.Fatal(err)
			}
			manifest := hex.EncodeToString(catalog.Digest[:])
			routes := routing.Catalog{Models: map[string]routing.Model{"logical-model": {Name: "logical-model", Routes: []routing.Route{{
				ID: "route-a", EndpointID: entry.EndpointID, Provider: entry.Provider, Family: entry.Family, Region: entry.Region,
				Model: entry.Model, Classes: []llm.ServiceClass{llm.ServiceClassStandard},
				ProviderTiers: map[llm.ServiceClass]string{llm.ServiceClassStandard: "standard"}, PriceAvailable: true,
			}}}}}
			calls := []string{}
			binding := &productionPhaseBinding{
				cap: V1RuntimeCapabilities{
					Clock: clock, Planner: routing.DeterministicPlanner{}, ReservationLease: 30 * time.Minute, OperationRetention: 2 * time.Hour,
					BudgetGenerationID: "generation-grant", BudgetIncarnationID: "incarnation-grant",
					GrantKeyID: "grant-key", GrantHMACKey: []byte(strings.Repeat("h", 32)),
					ResolveScope: func(context.Context, llm.RequestContext) (string, error) { return "scope-1", nil },
					Snapshot: engine.StaticSnapshot{Value: engine.Snapshot{Routes: routes, Prices: pricing.NewResolver(catalog), RequireBudgetMatch: true,
						BudgetPolicies: []budget.Policy{{ID: "policy", Windows: []budget.Window{
							{ID: "hour", Duration: time.Hour, Bucket: time.Minute, Limit: 100_000_000, LimitUSD: pricing.MustUSD("100")},
							{ID: "day", Duration: 24 * time.Hour, Bucket: time.Minute, Limit: 100_000_000, LimitUSD: pricing.MustUSD("100")},
						}}},
					}},
				},
				composition: durablestore.Composition{Materializer: materializer, Operations: &immutableCaptureStore{calls: &calls}},
			}
			requestContext := llm.RequestContext{Tenant: "tenant", Project: "forecast", Actor: "actor"}
			descriptor := llm.ReserveBatchOperationV1{OperationKey: "generate-1", Model: "logical-model", ServiceClass: llm.ServiceClassStandard, MaxInputTokens: 4096, MaxOutputTokens: 1024}
			operations := make([]llm.ReserveBatchOperationV1, scenario.count)
			for index := range operations {
				operations[index] = descriptor
				operations[index].OperationKey = fmt.Sprintf("generate-%d", index+1)
			}
			reserve := llm.ReserveBatchRequestV1{APIVersion: llm.ReserveBatchAPIVersion, Context: requestContext, CustomerID: "customer-1", RunID: "run-1", BudgetID: "budget-1", BatchKey: mode,
				PricingGenerationID: catalog.Version, PricingManifestSHA256: manifest, RemainingMaxCostMicrounits: 100_000_000, Operations: operations}
			if mode != "direct" {
				reserve.Operations = nil
				reserve.PhaseKey = mode
				reserve.PhaseExpiresAt = now.Add(time.Hour)
				templateCount := scenario.count
				if templateCount == 1 {
					templateCount = 3
				}
				reserve.Templates = []llm.ReserveBatchTemplateV1{{TemplateKey: mode, Count: int32(templateCount), Model: descriptor.Model, ServiceClass: descriptor.ServiceClass, MaxInputTokens: descriptor.MaxInputTokens, MaxOutputTokens: descriptor.MaxOutputTokens}}
			}
			response, err := binding.reserveBatch(ctx, reserve)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
			reservedReplay, err := binding.reserveBatch(ctx, reserve)
			if err != nil || !reflect.DeepEqual(reservedReplay, response) {
				t.Fatalf("lost reserve response replay after clock/bucket advance = %#v, %v; want %#v", reservedReplay, err, response)
			}
			batchID, batchSHA, materializationSHA := response.BatchID, response.BatchSHA256, response.RequestSHA256
			grants := response.Grants
			var allocation llm.AllocateBatchGrantsRequestV1
			var allocated llm.AllocateBatchGrantsResponseV1
			if mode != "direct" {
				if response.Status != llm.ReserveBatchStatusEscrowed {
					t.Fatalf("reserve = %#v", response)
				}
				allocation = llm.AllocateBatchGrantsRequestV1{APIVersion: llm.AllocateBatchGrantsAPIVersion, Context: requestContext, CustomerID: reserve.CustomerID, RunID: reserve.RunID, BudgetID: reserve.BudgetID,
					PhaseKey: reserve.PhaseKey, WaveKey: "wave-1", AllocationSequence: 1, BatchID: batchID, BatchSHA256: batchSHA, EscrowID: response.EscrowID, EscrowSHA256: response.EscrowSHA256,
					GrantExpiresAt: now.Add(30 * time.Minute), Operations: operations}
				allocated, err = binding.allocateBatchGrants(ctx, allocation)
				if err != nil {
					t.Fatal(err)
				}
				if allocated.BatchID != batchID || allocated.BatchSHA256 != batchSHA || allocated.RequestSHA256 == batchID {
					t.Fatalf("allocation lost parent/materialization distinction: %#v", allocated)
				}
				now = now.Add(2 * time.Minute)
				replayed, err := binding.allocateBatchGrants(ctx, allocation)
				if err != nil || !reflect.DeepEqual(replayed, allocated) {
					t.Fatalf("exact allocation replay changed: %#v, %v", replayed, err)
				}
				expectedRemaining := response.ReservedCostMicrounits - allocated.ReservedCostMicrounits
				if allocated.RemainingEscrowCostMicrounits != expectedRemaining {
					t.Fatalf("logical remaining duplicated across enforcement windows: got %d want %d", allocated.RemainingEscrowCostMicrounits, expectedRemaining)
				}
				changed := allocation
				changed.WaveKey = "changed-wave"
				if _, err := binding.allocateBatchGrants(ctx, changed); err == nil {
					t.Fatal("changed allocation reused occupied sequence")
				}
				materializationSHA, grants = allocated.RequestSHA256, allocated.Grants
				wrongParent := allocation
				wrongParent.BatchSHA256 = strings.Repeat("f", 64)
				if _, err := binding.allocateBatchGrants(ctx, wrongParent); err == nil {
					t.Fatal("wrong-parent allocation accepted")
				}
			}
			if len(grants) != scenario.count {
				t.Fatalf("grant count = %d, want %d", len(grants), scenario.count)
			}
			if scenario.count == 1 && mode != "fractional-template" {
				originalKey := binding.cap.GrantHMACKey
				binding.cap.GrantHMACKey = []byte(strings.Repeat("j", 32))
				if mode == "direct" {
					if _, err := binding.reserveBatch(ctx, reserve); err == nil {
						t.Fatal("stored grant re-signed with changed signing secret")
					}
				} else {
					if _, err := binding.allocateBatchGrants(ctx, allocation); err == nil {
						t.Fatal("stored allocation re-signed with changed signing secret")
					}
				}
				binding.cap.GrantHMACKey = originalKey
			}
			if scenario.count == llm.MaxReserveBatchOperations {
				extra := descriptor
				extra.OperationKey = "generate-over-bound"
				tooMany := append(append([]llm.ReserveBatchOperationV1(nil), operations...), extra)
				if mode == "direct" {
					invalid := reserve
					invalid.Operations = tooMany
					if _, err := binding.reserveBatch(ctx, invalid); err == nil {
						t.Fatal("1025-operation direct batch accepted")
					}
				} else {
					invalid := allocation
					invalid.Operations = tooMany
					if _, err := binding.allocateBatchGrants(ctx, invalid); err == nil {
						t.Fatal("1025-operation allocation accepted")
					}
				}
			}
			grant := grants[0]
			if mode == "fractional-template" && grant.MaxCostMicrounits != 1 {
				t.Fatalf("fractional price ceiling = %d want 1 micro-unit", grant.MaxCostMicrounits)
			}
			admitted := llm.CostAdmissionV1{APIVersion: llm.CostAdmissionAPIVersion, BudgetID: reserve.BudgetID, CustomerID: reserve.CustomerID, RunID: reserve.RunID, OperationKey: descriptor.OperationKey,
				GatewayAttemptOrdinal: 1, BatchID: batchID, BatchSHA256: batchSHA, GrantMaterializationRequestSHA256: materializationSHA,
				GrantID: grant.GrantID, GrantSHA256: grant.GrantSHA256, GrantKeyID: grant.GrantKeyID, GrantHMACSHA256: grant.GrantHMACSHA256,
				PricingGenerationID: catalog.Version, PricingManifestSHA256: manifest, RemainingMaxCostMicrounits: grant.MaxCostMicrounits}
			encoded, err := json.Marshal(admitted)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &admitted); err != nil {
				t.Fatal(err)
			}
			generate := llm.GenerateRequestV1{APIVersion: llm.APIVersion, OperationKey: descriptor.OperationKey, Context: requestContext, CostAdmission: &admitted}
			contentDigest, err := decodeSHA256(materializationSHA)
			if err != nil {
				t.Fatal(err)
			}
			stored, original, err := materializer.ConfirmBatchGrant(ctx, contentDigest, durablestore.OperationID(grant.OperationID))
			if err != nil {
				t.Fatal(err)
			}
			if scenario.count > 1 {
				last := grants[len(grants)-1]
				_, lastResult, err := materializer.ConfirmBatchGrant(ctx, contentDigest, durablestore.OperationID(last.OperationID))
				if err != nil || !lastResult.Accepted {
					t.Fatalf("last grant was not atomically accepted: %#v, %v", lastResult, err)
				}
			}
			candidate := routing.Candidate{RouteID: stored.Route.RouteID, EndpointID: entry.EndpointID, Provider: entry.Provider, Family: entry.Family, Region: entry.Region, Model: entry.Model, RequestedClass: llm.ServiceClassStandard, AttemptedClass: llm.ServiceClassStandard, ProviderTier: "standard"}
			providerRequest := llm.Request{APIVersion: llm.APIVersion, Context: requestContext, OperationKey: descriptor.OperationKey, Model: descriptor.Model, ServiceClass: descriptor.ServiceClass}
			newRoute := func() durablestore.RoutePlan {
				// Generate matches configured hour/day order at the current clock;
				// Redis stores day/hour order at the original escrow bucket.
				actualReservations := append([]admission.WindowReservation(nil), stored.Reservations...)
				actualReservations[0], actualReservations[1] = actualReservations[1], actualReservations[0]
				for index := range actualReservations {
					actualReservations[index].Bucket = now.Unix() / 60
				}
				return durablestore.RoutePlan{OperationID: stored.OperationID, GenerationID: stored.GenerationID, RouteID: stored.Route.RouteID, EndpointID: stored.Route.EndpointID, Provider: stored.Route.Provider, Model: stored.Route.ResolvedModel, PriceVersion: stored.Route.PriceVersion,
					Execution: &durablestore.RouteExecution{Request: providerRequest, Candidate: candidate, Price: entry, Reservations: actualReservations, EstimatedUSD: stored.LogicalCostUSD}}
			}
			for attempt := range 2 {
				confirmed, err := binding.prepareGrantedReservationRoute(ctx, generate, newRoute())
				if err != nil {
					t.Fatalf("Generate confirmation %d: %v", attempt, err)
				}
				if !confirmed.ReservationRecovered || confirmed.Reservation == nil || !reflect.DeepEqual(*confirmed.Reservation, original) {
					t.Fatalf("confirmation changed reservation: %#v", confirmed.Reservation)
				}
			}
			for _, wrongDigest := range []string{strings.Repeat("f", 64), response.EscrowSHA256, batchID} {
				if wrongDigest == "" || wrongDigest == materializationSHA {
					continue
				}
				forged := admitted
				forged.GrantMaterializationRequestSHA256 = wrongDigest
				generate.CostAdmission = &forged
				if _, err := binding.prepareGrantedReservationRoute(ctx, generate, newRoute()); err == nil {
					t.Fatal("changed materialization digest authenticated")
				}
			}
			if mode != "direct" && scenario.count == 1 {
				secondRequest := allocation
				secondRequest.AllocationSequence = 2
				secondRequest.WaveKey = "wave-2"
				secondOperation := descriptor
				secondOperation.OperationKey = "generate-second-wave"
				secondRequest.Operations = []llm.ReserveBatchOperationV1{secondOperation}
				second, err := binding.allocateBatchGrants(ctx, secondRequest)
				if err != nil {
					t.Fatal(err)
				}
				if second.RemainingEscrowCostMicrounits != grant.MaxCostMicrounits {
					t.Fatalf("second wave logical balance = %d", second.RemainingEscrowCostMicrounits)
				}
				firstReplay, err := binding.allocateBatchGrants(ctx, allocation)
				if err != nil || !reflect.DeepEqual(firstReplay, allocated) {
					t.Fatalf("later allocation changed original receipt: %#v, %v", firstReplay, err)
				}
				closeRequest := llm.CloseBatchRequestV1{APIVersion: llm.CloseBatchAPIVersion, Context: requestContext,
					CustomerID: reserve.CustomerID, RunID: reserve.RunID, BudgetID: reserve.BudgetID, PhaseKey: reserve.PhaseKey,
					CloseKey: "close-phase", Reason: llm.CloseBatchReasonCompleted, BatchID: response.BatchID, BatchSHA256: response.BatchSHA256,
					EscrowID: response.EscrowID, EscrowSHA256: response.EscrowSHA256}
				closed, err := binding.closeBatch(ctx, closeRequest)
				if err != nil {
					t.Fatal(err)
				}
				if closed.RefundedCostMicrounits != grant.MaxCostMicrounits {
					t.Fatalf("multi-window close refund = %d want %d", closed.RefundedCostMicrounits, grant.MaxCostMicrounits)
				}
				closedReplay, err := binding.closeBatch(ctx, closeRequest)
				if err != nil || closedReplay.RefundedCostMicrounits != closed.RefundedCostMicrounits || closedReplay.Status != llm.CloseBatchStatusAlreadyClosed {
					t.Fatalf("close replay = %#v, %v", closedReplay, err)
				}
				now = now.Add(2 * time.Minute)
				firstReplay, err = binding.allocateBatchGrants(ctx, allocation)
				if err != nil || !reflect.DeepEqual(firstReplay, allocated) {
					t.Fatalf("closed escrow changed allocation receipt: %#v, %v", firstReplay, err)
				}
				reservedReplay, err = binding.reserveBatch(ctx, reserve)
				if err != nil || !reflect.DeepEqual(reservedReplay, response) {
					t.Fatalf("closed escrow changed reserve receipt: %#v, %v", reservedReplay, err)
				}
			}
			now = grant.ReservationExpiresAt.Add(time.Second)
			generate.CostAdmission = &admitted
			if _, err := binding.prepareGrantedReservationRoute(ctx, generate, newRoute()); err == nil {
				t.Fatal("replayed receipt renewed expired grant")
			}
		})
	}
}
