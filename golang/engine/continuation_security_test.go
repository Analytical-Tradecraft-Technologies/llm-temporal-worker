package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

type trackingContinuationStore struct {
	value        state.Continuation
	getCalls     int
	getForTenant int
}

func (store *trackingContinuationStore) Get(context.Context, state.Handle) (state.Continuation, error) {
	store.getCalls++
	return store.value.Clone(), nil
}

func (store *trackingContinuationStore) GetForTenant(_ context.Context, tenant string, _ state.Handle) (state.Continuation, error) {
	store.getForTenant++
	if tenant == "" || tenant != store.value.Tenant {
		return state.Continuation{}, state.ErrInvalidHandle
	}
	return store.value.Clone(), nil
}

func (store *trackingContinuationStore) PutChild(context.Context, state.PutChildRequest) (state.Handle, error) {
	return "", errors.New("not implemented")
}

func TestLoadContinuationRejectsTenantlessRequest(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	store := &trackingContinuationStore{value: validContinuation(t, now, "tenant-1")}
	engineValue := &Engine{dependencies: Dependencies{Continuations: store}}
	request := baseRequest("tenantless-continuation")
	request.Context.Tenant = ""
	request.Continuation = &llm.Continuation{Handle: "continuation-handle"}

	_, _, _, err := engineValue.loadContinuation(context.Background(), request, now)
	if err == nil {
		t.Fatal("tenantless continuation request was accepted")
	}
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeInvalidArgument {
		t.Fatalf("loadContinuation error = %v, want invalid argument provider error", err)
	}
	if store.getCalls != 0 || store.getForTenant != 0 {
		t.Fatalf("tenantless request loaded continuation with Get=%d GetForTenant=%d", store.getCalls, store.getForTenant)
	}
}

func TestLoadContinuationUsesTenantScopedLookup(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	store := &trackingContinuationStore{value: validContinuation(t, now, "tenant-1")}
	engineValue := &Engine{dependencies: Dependencies{Continuations: store}}
	request := baseRequest("tenant-scoped-continuation")
	request.Continuation = &llm.Continuation{Handle: "continuation-handle"}

	_, _, parent, err := engineValue.loadContinuation(context.Background(), request, now)
	if err != nil {
		t.Fatalf("loadContinuation returned error: %v", err)
	}
	if parent == nil || parent.Tenant != "tenant-1" {
		t.Fatalf("loaded parent = %#v, want tenant-1 continuation", parent)
	}
	if store.getCalls != 0 || store.getForTenant != 1 {
		t.Fatalf("loadContinuation used Get=%d GetForTenant=%d, want only tenant-scoped lookup", store.getCalls, store.getForTenant)
	}
}

func validContinuation(t *testing.T, now time.Time, tenant string) state.Continuation {
	t.Helper()
	transcript := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	_, digest, err := state.CanonicalTranscript(transcript)
	if err != nil {
		t.Fatal(err)
	}
	return state.Continuation{
		ID:                 "continuation-handle",
		Tenant:             tenant,
		Transcript:         transcript,
		TranscriptDigest:   digest,
		TranscriptComplete: true,
		ProviderState: []state.OpaqueStateRef{{
			Provider: "openai", EndpointID: "endpoint-1", Family: string(provider.FamilyOpenAIResponses), ModelLineage: "lineage-1", Media: "application/vnd.openai.response-id", Data: []byte("resp_123"), Required: true,
		}},
		Pinning:   state.Pinning{Provider: "openai", EndpointID: "endpoint-1", Family: string(provider.FamilyOpenAIResponses), ModelLineage: "lineage-1"},
		CreatedAt: now.Add(-time.Minute),
		ExpiresAt: now.Add(time.Hour),
	}
}
