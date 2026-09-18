package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testCacheKey() CacheKey {
	suffix := uuid.NewString()
	return CacheKey{
		ScopeID:                 uuid.New(),
		FingerprintVersion:      1,
		SemanticFingerprintHMAC: sha256.Sum256([]byte("semantic:" + suffix)),
		RouteIdentityHMAC:       sha256.Sum256([]byte("route:" + suffix)),
		Variant:                 0,
	}
}

func TestCacheKeyRequiresAllIndexedIdentityParts(t *testing.T) {
	valid := testCacheKey()
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		edit func(*CacheKey)
	}{
		{name: "scope", edit: func(key *CacheKey) { key.ScopeID = uuid.Nil }},
		{name: "fingerprint version", edit: func(key *CacheKey) { key.FingerprintVersion = 0 }},
		{name: "semantic fingerprint", edit: func(key *CacheKey) { key.SemanticFingerprintHMAC = [keyDigestBytes]byte{} }},
		{name: "route identity", edit: func(key *CacheKey) { key.RouteIdentityHMAC = [keyDigestBytes]byte{} }},
		{name: "negative variant", edit: func(key *CacheKey) { key.Variant = -1 }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			key := valid
			test.edit(&key)
			if err := key.validate(); err == nil {
				t.Fatal("invalid cache key unexpectedly accepted")
			}
		})
	}
}

func TestNormalizeCacheManifestIsObjectAndCanonical(t *testing.T) {
	got, err := normalizeCacheManifest([]byte(`{"b":2,"a":1}`))
	if err != nil || string(got) != `{"a":1,"b":2}` {
		t.Fatalf("manifest=%s err=%v", got, err)
	}
	large, err := normalizeCacheManifest([]byte(`{"seed":9007199254740993}`))
	if err != nil || string(large) != `{"seed":9007199254740993}` {
		t.Fatalf("large numeric manifest=%s err=%v", large, err)
	}
	for _, raw := range []string{"[1]", `{"a":`, "null"} {
		if _, err := normalizeCacheManifest([]byte(raw)); err == nil {
			t.Fatalf("manifest %q unexpectedly accepted", raw)
		}
	}
	empty, err := normalizeCacheManifest(nil)
	if err != nil || string(empty) != `{}` {
		t.Fatalf("empty manifest=%s err=%v", empty, err)
	}
}

func TestResponseCacheValidationRejectsUnboundedConfiguration(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "llm_worker", "")
	if err != nil {
		t.Fatal(err)
	}
	keyring := Keyring{Active: "cache-v1", Keys: map[string][]byte{"cache-v1": []byte("01234567890123456789012345678901")}}
	repository := DefaultResponseCacheRepository(nil, namespace, keyring)
	if err := repository.validate(); err == nil {
		t.Fatal("nil pool unexpectedly accepted")
	}
	repository.Pool = &pgxpool.Pool{}
	repository.MaxInlineBytes = maxEnvelope + 1
	if err := repository.validate(); err == nil {
		t.Fatal("oversized inline limit unexpectedly accepted")
	}
	repository.MaxInlineBytes = 1
	repository.MaxLookupAge = 0
	if err := repository.validate(); err == nil {
		t.Fatal("zero cache age unexpectedly accepted")
	}
}

func TestCacheLookupRejectsInvalidOptInBeforeDatabaseAccess(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "llm_worker", "")
	if err != nil {
		t.Fatal(err)
	}
	keyring := Keyring{Active: "cache-v1", Keys: map[string][]byte{"cache-v1": []byte("01234567890123456789012345678901")}}
	repository := DefaultResponseCacheRepository(nil, namespace, keyring)
	request := CacheLookupRequest{
		Key: testCacheKey(), OperationID: "operation", MaxAge: time.Hour,
		CanonicalRequestJSON:   []byte(`{"model":"fixture"}`),
		SemanticProfileVersion: "profile-v1", CacheEpoch: "epoch-v1",
		Provider: "fixture", EndpointID: "endpoint", ResolvedModel: "model",
	}
	if _, err := repository.Lookup(context.Background(), request); err == nil {
		t.Fatal("lookup unexpectedly reached a nil database")
	}
	request.MaxAge = 0
	result, err := repository.Lookup(context.Background(), request)
	if err == nil {
		t.Fatal("zero max age unexpectedly accepted")
	}
	if errors.Is(err, ErrCacheEntryNotFound) {
		t.Fatal("cache miss should not be represented as a database error")
	}
	_ = result
}

func TestCacheLookupRequiresFrozenRequestAndRouteIdentity(t *testing.T) {
	namespace, err := NewNamespace("llm_worker", "llm_worker", "")
	if err != nil {
		t.Fatal(err)
	}
	keyring := Keyring{Active: "cache-v1", Keys: map[string][]byte{"cache-v1": []byte("01234567890123456789012345678901")}}
	repository := DefaultResponseCacheRepository(nil, namespace, keyring)
	valid := CacheLookupRequest{
		Key: testCacheKey(), OperationID: "operation", MaxAge: time.Hour,
		CanonicalRequestJSON:   []byte(`{"model":"fixture"}`),
		SemanticProfileVersion: "profile-v1", CacheEpoch: "epoch-v1",
		Provider: "fixture", EndpointID: "endpoint", ResolvedModel: "model",
	}
	for name, mutate := range map[string]func(*CacheLookupRequest){
		"canonical request": func(request *CacheLookupRequest) { request.CanonicalRequestJSON = nil },
		"semantic profile":  func(request *CacheLookupRequest) { request.SemanticProfileVersion = "" },
		"cache epoch":       func(request *CacheLookupRequest) { request.CacheEpoch = "" },
		"provider":          func(request *CacheLookupRequest) { request.Provider = "" },
		"endpoint":          func(request *CacheLookupRequest) { request.EndpointID = "" },
		"resolved model":    func(request *CacheLookupRequest) { request.ResolvedModel = "" },
	} {
		request := valid
		mutate(&request)
		if _, err := repository.Lookup(context.Background(), request); err == nil {
			t.Fatalf("lookup accepted missing %s", name)
		}
	}
}
