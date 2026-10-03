package cache

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func testInput() Input {
	return Input{
		Operation: OperationGenerate,
		Config:    ConfigDigest("config-a"),
		Route: RouteIdentity{
			Provider: "openai", Endpoint: "https://api.openai.test", Account: "acct-a", Region: "us-east-1",
			Model: "gpt", Revision: "gpt-2026-01", Compiler: "openai-responses/v1",
		},
		CapabilityLowering: "cap/v1", Epoch: "epoch-1", Conversation: "sha256:conversation",
		Request: llm.Request{
			OperationKey: "operation-a", Model: "logical-model", ServiceClass: llm.ServiceClassStandard,
			Context: llm.RequestContext{Tenant: "tenant-a", Project: "project-a", Actor: "actor-a", Tags: map[string]string{"trace": "one"}},
			Input:   []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
		},
	}
}

func TestComputeExcludesPerCallControls(t *testing.T) {
	key := []byte("deployment-secret")
	base := testInput()
	first, err := Compute(key, base)
	if err != nil {
		t.Fatal(err)
	}
	base.Request.OperationKey = "operation-b"
	base.Request.ServiceClass = llm.ServiceClassPriority
	base.Request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassEconomy}
	base.Request.Context.Actor = "different-actor"
	base.Request.Context.Tags = map[string]string{"trace": "two", "new": "tag"}
	second, err := Compute(key, base)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("per-call controls changed semantic fingerprint: %s != %s", first.Hex(), second.Hex())
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
	compact.Variant = 0
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
	if _, ok := request["service_class"]; ok {
		t.Fatal("service class must not be present in semantic manifest")
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
	first.Request.Sampling = &llm.SamplingSpec{Seed: &seedA}
	second := testInput()
	seedB := int64(9007199254740993)
	second.Request.Sampling = &llm.SamplingSpec{Seed: &seedB}
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

func TestSampleIndexSeparatesBothOperationsWithoutChangingProviderInput(t *testing.T) {
	for _, operation := range []OperationKind{OperationGenerate, OperationCompact} {
		for _, temperature := range []*float64{nil, new(float64)} {
			input := testInput()
			input.Operation = operation
			input.Request.Sampling = &llm.SamplingSpec{Temperature: temperature}
			before, _ := json.Marshal(input.Request)
			seen := map[Fingerprint]bool{}
			for _, index := range []int32{0, 1, 2147483647} {
				input.Variant = index
				fingerprint, err := Compute([]byte("secret"), input)
				if err != nil || seen[fingerprint] {
					t.Fatalf("%s index %d: duplicate or error %v", operation, index, err)
				}
				seen[fingerprint] = true
			}
			after, _ := json.Marshal(input.Request)
			if !bytes.Equal(before, after) {
				t.Fatal("sample index changed provider input")
			}
			input.Variant = -1
			if _, err := Compute([]byte("secret"), input); err == nil {
				t.Fatal("negative index accepted")
			}
		}
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
