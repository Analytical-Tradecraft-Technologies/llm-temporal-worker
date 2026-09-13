package cache

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func testInput() Input {
	temperature := 0.7
	return Input{
		Operation: OperationGenerate,
		Config:    ConfigDigest("config-a"),
		Route: RouteIdentity{
			Provider: "openai", Endpoint: "https://api.openai.test", Account: "acct-a", Region: "us-east-1",
			Model: "gpt", Revision: "gpt-2026-01", Compiler: "openai-responses/v1",
		},
		CapabilityLowering: "cap/v1", Epoch: "epoch-1", Conversation: "sha256:conversation",
		Policy: Policy{MaxAge: 5 * time.Minute, Variant: 7},
		Request: llm.Request{
			OperationKey: "operation-a", Model: "logical-model", ServiceClass: llm.ServiceClassStandard,
			Context:  llm.RequestContext{Tenant: "tenant-a", Project: "project-a", Actor: "actor-a", Tags: map[string]string{"trace": "one"}},
			Sampling: &llm.SamplingSpec{Temperature: &temperature},
			Input:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		},
	}
}

func TestComputeExcludesOnlyPerCallIdentityAndObservability(t *testing.T) {
	key := []byte("deployment-secret")
	base := testInput()
	first, err := Compute(key, base)
	if err != nil {
		t.Fatal(err)
	}
	base.Request.OperationKey = "operation-b"
	base.Request.Context.Actor = "different-actor"
	base.Request.Context.Tags = map[string]string{"trace": "two", "new": "tag"}
	second, err := Compute(key, base)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("per-call identity or observability changed semantic fingerprint: %s != %s", first.Hex(), second.Hex())
	}
}

func TestComputeDomainSeparatesGenerateAndCompact(t *testing.T) {
	key := []byte("deployment-secret")
	generate, err := Compute(key, testInput())
	if err != nil {
		t.Fatal(err)
	}
	compact := testInput()
	compact.Operation = OperationCompact
	compact.Policy.Variant = 0
	compactResult, err := Compute(key, compact)
	if err != nil {
		t.Fatal(err)
	}
	if generate == compactResult {
		t.Fatal("generate and compact fingerprints must be domain-separated")
	}
}

func TestCanonicalManifestIsStableAndContainsNoOperationKey(t *testing.T) {
	canonical, err := testInput().Canonical()
	if err != nil {
		t.Fatal(err)
	}
	again, err := testInput().Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, again) {
		t.Fatalf("canonical manifest changed across identical calls")
	}
	var decoded map[string]any
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatal(err)
	}
	request := decoded["request"].(map[string]any)
	if _, ok := request["operation_key"]; ok {
		t.Fatal("operation key must not be present in semantic manifest")
	}
	if _, ok := request["service_class"]; !ok {
		t.Fatal("frozen service class must be present in semantic manifest")
	}
}

func TestCanonicalizationIgnoresMapInsertionOrder(t *testing.T) {
	first := testInput()
	first.Request.Extensions = map[string]json.RawMessage{
		"z": json.RawMessage(`{"b":2,"a":1}`),
		"a": json.RawMessage(`true`),
	}
	second := testInput()
	second.Request.Extensions = map[string]json.RawMessage{
		"a": json.RawMessage(`true`),
		"z": json.RawMessage(`{"a":1,"b":2}`),
	}
	firstFingerprint, err := Compute([]byte("secret"), first)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := Compute([]byte("secret"), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != secondFingerprint {
		t.Fatal("JSON object spelling or map insertion order changed fingerprint")
	}
}

func TestCanonicalizationPreservesLargeIntegerSeeds(t *testing.T) {
	first := testInput()
	seedA := int64(9007199254740992)
	first.Request.Sampling.Seed = &seedA
	second := testInput()
	seedB := int64(9007199254740993)
	second.Request.Sampling.Seed = &seedB
	firstFingerprint, err := Compute([]byte("secret"), first)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := Compute([]byte("secret"), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint == secondFingerprint {
		t.Fatal("distinct int64 sampling seeds must not share a fingerprint")
	}
}

func TestCompactRejectsPositiveVariant(t *testing.T) {
	input := testInput()
	input.Operation = OperationCompact
	input.Policy.Variant = 1
	if _, err := Compute([]byte("secret"), input); err == nil {
		t.Fatal("compact fingerprints must reject positive variants")
	}
}

func TestComputeRejectsMissingKeyAndRouteIdentity(t *testing.T) {
	if _, err := Compute(nil, testInput()); err == nil {
		t.Fatal("empty HMAC key must fail closed")
	}
	input := testInput()
	input.Route.Revision = ""
	if _, err := Compute([]byte("secret"), input); err == nil {
		t.Fatal("missing resolved model revision must fail closed")
	}
}
