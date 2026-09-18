package postgres

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

type responseCacheFixture struct {
	repository   ResponseCacheRepository
	operations   OperationRepository
	ctx          context.Context
	cleanup      func()
	scope        Scope
	originID     string
	checkpointID uuid.UUID
}

func newResponseCacheFixture(t *testing.T) responseCacheFixture {
	t.Helper()
	operations, ctx, cleanup := operationIntegrationRepository(t)
	t.Cleanup(cleanup)
	scope, err := operations.Scopes.Ensure(ctx, "cache-integration-tenant", "cache-integration-project")
	if err != nil {
		t.Fatal(err)
	}
	originID := "cache-origin-" + uuid.NewString()
	origin, err := operations.Begin(ctx, admission.BeginRequest{ID: originID, OperationKey: originID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
		RequestDigest:   admission.Digest([]byte(originID)),
		ReservationUSD:  pricing.MustUSD("0"),
		ExpiresAt:       time.Now().UTC().Add(time.Hour),
		RequestManifest: []byte(`{"model":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	blobs := BlobRepository{Pool: operations.Pool, Namespace: operations.Namespace, Keys: operations.Keys, NewID: UUIDv7}
	digest := sha256.Sum256([]byte(originID + "-blob"))
	blob, err := blobs.PutLocator(ctx, scope.ID, "cache-fixture", BlobMetadata{StoreID: "cache-fixture-store", Digest: digest, ByteLength: 8, MediaType: "application/json"}, []byte("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	checkpointID := uuid.New()
	publicID := sha256.Sum256([]byte(originID + "-public"))
	lineage := sha256.Sum256([]byte(originID + "-lineage"))
	settings := sha256.Sum256([]byte(originID + "-settings"))
	frontier := sha256.Sum256([]byte(originID + "-frontier"))
	checkpoints, err := operations.Namespace.Render("conversation_checkpoints")
	if err != nil {
		t.Fatal(err)
	}
	_, err = operations.Pool.Exec(ctx, "INSERT INTO "+checkpoints+" (checkpoint_id, scope_id, public_id_hmac, handle_key_id, checkpoint_kind, depth, origin_operation_id, delta_blob_id, response_blob_id, settings_patch_blob_id, canonical_lineage_digest, materialized_settings_digest, tool_frontier_digest, schema_version, compiler_epoch, expires_at) VALUES ($1,$2,$3,$4,'generation',0,$5,$6,$6,$6,$7,$8,$9,1,'cache-fixture',$10)", checkpointID, scope.ID, publicID[:], operations.Keys.Active, operationUUID(origin.Operation.ID), blob.BlobID, lineage[:], settings[:], frontier[:], time.Now().UTC().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return responseCacheFixture{repository: DefaultResponseCacheRepository(operations.Pool, operations.Namespace, operations.Keys), operations: operations, ctx: ctx, cleanup: cleanup, scope: scope, originID: origin.Operation.ID, checkpointID: checkpointID}
}

func fixtureCacheLookup(key CacheKey, operationID string) CacheLookupRequest {
	return CacheLookupRequest{
		Key: key, OperationID: operationID, MaxAge: time.Hour,
		CanonicalRequestJSON:   []byte(`{"model":"fixture"}`),
		SemanticProfileVersion: "profile-v1", CacheEpoch: "epoch-v1",
		Provider: "fixture", EndpointID: "endpoint-fixture", ResolvedModel: "model-fixture",
	}
}

func TestResponseCacheFillLookupAndUseAccounting(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	fill := CacheFillRequest{Key: key, OperationID: fixture.originID, Lease: time.Minute}
	acquired, err := fixture.repository.BeginFill(fixture.ctx, fill)
	if err != nil || acquired.Status != CacheFillAcquired || acquired.LeaseUntil.IsZero() {
		t.Fatalf("begin fill=%#v err=%v", acquired, err)
	}
	published, err := fixture.repository.Publish(fixture.ctx, CachePublishRequest{
		Fill:                   fill,
		CanonicalRequestJSON:   []byte(`{"model":"fixture"}`),
		SemanticProfileVersion: "profile-v1",
		CacheEpoch:             "epoch-v1",
		OriginOperationID:      fixture.originID,
		OriginCheckpointID:     fixture.checkpointID,
		OriginProvider:         "fixture",
		OriginEndpointID:       "endpoint-fixture",
		OriginResolvedModel:    "model-fixture",
		Response:               []byte(`{"output":"cached"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if published.ID == uuid.Nil || published.UseCount != 1 || string(published.Response.Ciphertext) == `{"output":"cached"}` {
		t.Fatalf("published entry=%#v", published)
	}
	consumerID := "cache-consumer-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: consumerID, OperationKey: consumerID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(consumerID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour), RequestManifest: []byte(`{"model":"fixture"}`)}); err != nil {
		t.Fatal(err)
	}
	hit, err := fixture.repository.Lookup(fixture.ctx, fixtureCacheLookup(key, consumerID))
	if err != nil || !hit.Hit || string(hit.Response) != `{"output":"cached"}` {
		t.Fatalf("cache hit=%#v err=%v", hit, err)
	}
	// A Temporal retry reuses the same logical operation and must not inflate
	// use_count or create a second response_cache_uses row.
	retry, err := fixture.repository.Lookup(fixture.ctx, fixtureCacheLookup(key, consumerID))
	if err != nil || !retry.Hit || string(retry.Response) != `{"output":"cached"}` {
		t.Fatalf("cache retry=%#v err=%v", retry, err)
	}
	entries, err := fixture.operations.Namespace.Render("response_cache_entries")
	if err != nil {
		t.Fatal(err)
	}
	var useCount int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT use_count FROM "+entries+" WHERE cache_entry_id=$1", published.ID).Scan(&useCount); err != nil {
		t.Fatal(err)
	}
	if useCount != 2 {
		t.Fatalf("use_count=%d, want origin plus one logical hit", useCount)
	}
	uses, err := fixture.operations.Namespace.Render("response_cache_uses")
	if err != nil {
		t.Fatal(err)
	}
	var useRows int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+uses+" WHERE cache_entry_id=$1", published.ID).Scan(&useRows); err != nil {
		t.Fatal(err)
	}
	if useRows != 2 {
		t.Fatalf("cache use rows=%d, want origin plus one logical hit", useRows)
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET use_count=2147483647 WHERE cache_entry_id=$1", published.ID); err != nil {
		t.Fatal(err)
	}
	saturatingID := "cache-saturating-consumer-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: saturatingID, OperationKey: saturatingID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(saturatingID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if hit, err := fixture.repository.Lookup(fixture.ctx, fixtureCacheLookup(key, saturatingID)); err != nil || !hit.Hit {
		t.Fatalf("saturating cache hit=%#v err=%v", hit, err)
	}
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT use_count FROM "+entries+" WHERE cache_entry_id=$1", published.ID).Scan(&useCount); err != nil {
		t.Fatal(err)
	}
	if useCount != 2147483647 {
		t.Fatalf("saturated use_count=%d, want max int32", useCount)
	}
	wrongRoute := key
	wrongRoute.RouteIdentityHMAC = sha256.Sum256([]byte("different-route"))
	missRequest := fixtureCacheLookup(wrongRoute, "cache-route-miss-"+uuid.NewString())
	miss, err := fixture.repository.Lookup(fixture.ctx, missRequest)
	if err != nil {
		t.Fatal(err)
	}
	if miss.Hit {
		t.Fatal("route-isolated cache entry crossed route identity")
	}
	for name, mutate := range map[string]func(*CacheLookupRequest){
		"canonical request": func(request *CacheLookupRequest) { request.CanonicalRequestJSON = []byte(`{"model":"other"}`) },
		"semantic profile":  func(request *CacheLookupRequest) { request.SemanticProfileVersion = "profile-v2" },
		"cache epoch":       func(request *CacheLookupRequest) { request.CacheEpoch = "epoch-v2" },
		"provider":          func(request *CacheLookupRequest) { request.Provider = "other-provider" },
		"endpoint":          func(request *CacheLookupRequest) { request.EndpointID = "other-endpoint" },
		"resolved model":    func(request *CacheLookupRequest) { request.ResolvedModel = "other-model" },
	} {
		request := fixtureCacheLookup(key, "cache-frozen-policy-miss-"+uuid.NewString())
		mutate(&request)
		result, err := fixture.repository.Lookup(fixture.ctx, request)
		if err != nil {
			t.Fatalf("%s mismatch lookup: %v", name, err)
		}
		if result.Hit {
			t.Fatalf("cache hit crossed frozen %s", name)
		}
	}
	if _, err := fixture.operations.Pool.Exec(fixture.ctx, "UPDATE "+entries+" SET state='tombstoned' WHERE cache_entry_id=$1", published.ID); err != nil {
		t.Fatal(err)
	}
	takeoverID := "cache-tombstoned-takeover-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: takeoverID, OperationKey: takeoverID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(takeoverID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	takeover := CacheFillRequest{Key: key, OperationID: takeoverID, Lease: time.Minute}
	if result, err := fixture.repository.BeginFill(fixture.ctx, takeover); err != nil || result.Status != CacheFillAcquired {
		t.Fatalf("tombstoned entry takeover=%#v err=%v", result, err)
	}
	if err := fixture.repository.FailFill(fixture.ctx, takeover); err != nil {
		t.Fatal(err)
	}
}

func TestResponseCacheConcurrentFillAcquisition(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	// Repeat the race so the conflict-safe INSERT ... ON CONFLICT DO
	// NOTHING reread path is exercised consistently rather than relying on
	// one scheduler interleaving.
	for attempt := 0; attempt < 8; attempt++ {
		key := testCacheKey()
		key.ScopeID = fixture.scope.ID
		key.SemanticFingerprintHMAC = sha256.Sum256([]byte(fmt.Sprintf("cache-concurrent-fill-%d", attempt)))
		ids := []string{"cache-concurrent-fill-a-" + uuid.NewString(), "cache-concurrent-fill-b-" + uuid.NewString()}
		for _, id := range ids {
			if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: id, OperationKey: id, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(id)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
		}
		type fillResult struct {
			result CacheFillResult
			err    error
		}
		start := make(chan struct{})
		results := make(chan fillResult, len(ids))
		for _, id := range ids {
			go func(id string) {
				<-start
				result, err := fixture.repository.BeginFill(fixture.ctx, CacheFillRequest{Key: key, OperationID: id, Lease: time.Minute})
				results <- fillResult{result: result, err: err}
			}(id)
		}
		close(start)
		acquired, busy := 0, 0
		for range ids {
			outcome := <-results
			if outcome.err != nil {
				t.Fatal(outcome.err)
			}
			switch outcome.result.Status {
			case CacheFillAcquired:
				acquired++
			case CacheFillBusy:
				busy++
			default:
				t.Fatalf("unexpected concurrent fill result=%#v", outcome.result)
			}
		}
		if acquired != 1 || busy != 1 {
			t.Fatalf("concurrent fill outcomes acquired=%d busy=%d, want one each", acquired, busy)
		}
	}
}

func TestResponseCacheHundredWayConcurrentFillAcquisition(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	key.SemanticFingerprintHMAC = sha256.Sum256([]byte("cache-hundred-way-fill"))

	const workers = 100
	operationIDs := make([]string, workers)
	for i := range operationIDs {
		operationIDs[i] = fmt.Sprintf("cache-hundred-way-fill-%03d-%s", i, uuid.NewString())
		if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: operationIDs[i], OperationKey: operationIDs[i], Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
			RequestDigest:   admission.Digest([]byte(operationIDs[i])),
			ReservationUSD:  pricing.MustUSD("0"),
			ExpiresAt:       time.Now().UTC().Add(time.Hour),
			RequestManifest: []byte(`{"model":"fixture"}`)}); err != nil {
			t.Fatal(err)
		}
	}

	type fillOutcome struct {
		result CacheFillResult
		err    error
	}
	start := make(chan struct{})
	results := make(chan fillOutcome, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for _, operationID := range operationIDs {
		go func(operationID string) {
			defer group.Done()
			<-start
			result, err := fixture.repository.BeginFill(fixture.ctx, CacheFillRequest{Key: key, OperationID: operationID, Lease: time.Minute})
			results <- fillOutcome{result: result, err: err}
		}(operationID)
	}
	close(start)
	group.Wait()
	close(results)

	acquired, busy := 0, 0
	for outcome := range results {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		switch outcome.result.Status {
		case CacheFillAcquired:
			acquired++
		case CacheFillBusy:
			busy++
		default:
			t.Fatalf("unexpected hundred-way fill result=%#v", outcome.result)
		}
	}
	if acquired != 1 || busy != workers-1 {
		t.Fatalf("hundred-way fill outcomes acquired=%d busy=%d, want one acquired and %d busy", acquired, busy, workers-1)
	}
}

func TestResponseCacheHundredWayMissHasOneFillAndOneUsePerOperation(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	key.SemanticFingerprintHMAC = sha256.Sum256([]byte("cache-hundred-way-miss-" + uuid.NewString()))

	// Every caller has a distinct durable operation, but all callers ask for
	// the same cache identity. This is the production miss race: exactly one
	// caller may perform the provider work while the others wait for the
	// resulting entry rather than starting duplicate fills.
	const callers = 100
	operationIDs := make([]string, callers)
	operationIDs[0] = fixture.originID
	for i := 1; i < callers; i++ {
		operationIDs[i] = fmt.Sprintf("cache-hundred-way-%03d-%s", i, uuid.NewString())
		if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: operationIDs[i], OperationKey: operationIDs[i], Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project",
			RequestDigest:   admission.Digest([]byte(operationIDs[i])),
			ReservationUSD:  pricing.MustUSD("0"),
			ExpiresAt:       time.Now().UTC().Add(time.Hour),
			RequestManifest: []byte(`{"model":"fixture"}`)}); err != nil {
			t.Fatal(err)
		}
	}

	// Start every fill request together. PostgreSQL's conflict-safe insert and
	// row lock must elect exactly one owner while all other operations observe
	// the active lease as busy.
	start := make(chan struct{})
	type fillOutcome struct {
		operationID string
		result      CacheFillResult
		err         error
	}
	results := make(chan fillOutcome, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for _, operationID := range operationIDs {
		go func(operationID string) {
			defer group.Done()
			<-start
			result, err := fixture.repository.BeginFill(fixture.ctx, CacheFillRequest{Key: key, OperationID: operationID, Lease: time.Minute})
			results <- fillOutcome{operationID: operationID, result: result, err: err}
		}(operationID)
	}
	close(start)
	group.Wait()
	close(results)
	acquired, busy := 0, 0
	ownerID := ""
	for outcome := range results {
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		switch outcome.result.Status {
		case CacheFillAcquired:
			acquired++
			ownerID = outcome.operationID
		case CacheFillBusy:
			busy++
		default:
			t.Fatalf("unexpected concurrent fill result=%#v", outcome.result)
		}
	}
	if acquired != 1 || busy != callers-1 {
		t.Fatalf("hundred-way miss fill outcomes acquired=%d busy=%d, want one acquired and %d busy", acquired, busy, callers-1)
	}
	ownerFill := CacheFillRequest{Key: key, OperationID: ownerID, Lease: time.Minute}

	if _, err := fixture.repository.Publish(fixture.ctx, CachePublishRequest{
		Fill:                   ownerFill,
		CanonicalRequestJSON:   []byte(`{"model":"fixture"}`),
		SemanticProfileVersion: "profile-v1",
		CacheEpoch:             "epoch-v1",
		OriginOperationID:      ownerID,
		OriginCheckpointID:     fixture.checkpointID,
		OriginProvider:         "fixture",
		OriginEndpointID:       "endpoint-fixture",
		OriginResolvedModel:    "model-fixture",
		Response:               []byte(`{"output":"cached"}`),
	}); err != nil {
		t.Fatal(err)
	}

	// All non-owner callers now replay the exact response. Their uses are
	// recorded independently, while the owner use inserted by Publish remains
	// the only use for the fill owner.
	start = make(chan struct{})
	errs := make(chan error, callers-1)
	group = sync.WaitGroup{}
	group.Add(callers - 1)
	for _, operationID := range operationIDs {
		if operationID == ownerID {
			continue
		}
		go func(operationID string) {
			defer group.Done()
			<-start
			hit, err := fixture.repository.Lookup(fixture.ctx, fixtureCacheLookup(key, operationID))
			if err == nil && (!hit.Hit || string(hit.Response) != `{"output":"cached"}`) {
				err = fmt.Errorf("cache lookup hit=%#v", hit)
			}
			errs <- err
		}(operationID)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	entries, err := fixture.operations.Namespace.Render("response_cache_entries")
	if err != nil {
		t.Fatal(err)
	}
	var useCount int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT use_count FROM "+entries+" WHERE semantic_fingerprint_hmac=$1", key.SemanticFingerprintHMAC[:]).Scan(&useCount); err != nil {
		t.Fatal(err)
	}
	if useCount != callers {
		t.Fatalf("hundred-way cache use_count=%d, want %d", useCount, callers)
	}
	uses, err := fixture.operations.Namespace.Render("response_cache_uses")
	if err != nil {
		t.Fatal(err)
	}
	var useRows int
	if err := fixture.operations.Pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+uses+" WHERE cache_entry_id=(SELECT cache_entry_id FROM "+entries+" WHERE semantic_fingerprint_hmac=$1)", key.SemanticFingerprintHMAC[:]).Scan(&useRows); err != nil {
		t.Fatal(err)
	}
	if useRows != callers {
		t.Fatalf("hundred-way cache use rows=%d, want %d", useRows, callers)
	}
}

func TestResponseCacheFillLeaseBusyAndTakeover(t *testing.T) {
	fixture := newResponseCacheFixture(t)
	key := testCacheKey()
	key.ScopeID = fixture.scope.ID
	first := CacheFillRequest{Key: key, OperationID: fixture.originID, Lease: time.Minute}
	if result, err := fixture.repository.BeginFill(fixture.ctx, first); err != nil || result.Status != CacheFillAcquired {
		t.Fatalf("first fill=%#v err=%v", result, err)
	}
	secondID := "cache-fill-contender-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: secondID, OperationKey: secondID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(secondID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	second := CacheFillRequest{Key: key, OperationID: secondID, Lease: time.Minute}
	if result, err := fixture.repository.BeginFill(fixture.ctx, second); err != nil || result.Status != CacheFillBusy {
		t.Fatalf("busy fill=%#v err=%v", result, err)
	}
	// A different route identity is an independent fill, even when the
	// semantic fingerprint and variant are identical.
	thirdID := "cache-fill-other-route-" + uuid.NewString()
	if _, err := fixture.operations.Begin(fixture.ctx, admission.BeginRequest{ID: thirdID, OperationKey: thirdID, Actor: "postgres-test", ScopeKey: "cache-integration-tenant/cache-integration-project", RequestDigest: admission.Digest([]byte(thirdID)), ReservationUSD: pricing.MustUSD("0"), ExpiresAt: time.Now().UTC().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	otherRoute := key
	otherRoute.RouteIdentityHMAC = sha256.Sum256([]byte("other-route"))
	otherRouteFill := CacheFillRequest{Key: otherRoute, OperationID: thirdID, Lease: time.Minute}
	if result, err := fixture.repository.BeginFill(fixture.ctx, otherRouteFill); err != nil || result.Status != CacheFillAcquired {
		t.Fatalf("other route fill=%#v err=%v", result, err)
	}
	if err := fixture.repository.FailFill(fixture.ctx, otherRouteFill); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repository.FailFill(fixture.ctx, first); err != nil {
		t.Fatal(err)
	}
	if result, err := fixture.repository.BeginFill(fixture.ctx, second); err != nil || result.Status != CacheFillAcquired {
		t.Fatalf("takeover fill=%#v err=%v", result, err)
	}
}
