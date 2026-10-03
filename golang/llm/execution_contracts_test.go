package llm_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

const requestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5"

func TestExecutionContractsRoundTripEveryState(t *testing.T) {
	var generate llm.GenerateResponseV1
	var compact llm.CompactResponseV1
	if err := json.Unmarshal(readV1Fixture(t, "generate-response.json"), &generate); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(readV1Fixture(t, "compact-response.json"), &compact); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"generate", "compact"} {
		for _, state := range []llm.ExecutionStateV1{llm.ExecutionBudgetRequired, llm.ExecutionAcquired, llm.ExecutionBudgetWait, llm.ExecutionCacheWait, llm.ExecutionPending, llm.ExecutionProviderCompleted, llm.ExecutionCompleted, llm.ExecutionFailed, llm.ExecutionOutcomeUnknown} {
			t.Run(kind+"/"+string(state), func(t *testing.T) {
				value := llm.ExecutionResultV1{RequestID: requestID, Kind: kind, State: state}
				if state == llm.ExecutionBudgetWait || state == llm.ExecutionCacheWait || state == llm.ExecutionPending {
					value.RetryAfterSeconds = 3
				}
				if state == llm.ExecutionFailed {
					value.FailureCode = "provider_error"
					value.Retryable = true
				}
				if state == llm.ExecutionCompleted {
					if kind == "generate" {
						value.Generate = &generate
					} else {
						value.Compact = &compact
					}
				}
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var decoded llm.ExecutionResultV1
				if err := json.Unmarshal(data, &decoded); err != nil {
					t.Fatal(err)
				}
				second, err := json.Marshal(decoded)
				if err != nil || string(second) != string(data) {
					t.Fatalf("unstable wire round trip: %v", err)
				}
			})
		}
	}
}

func TestExecutionResultRejectsInvalidUnionsAndClosedFields(t *testing.T) {
	base := `{"request_id":"` + requestID + `","kind":"generate","state":"pending","retry_after_seconds":3}`
	cases := []string{
		`null`, `[]`, `{}`, strings.Replace(base, "pending", "bogus", 1),
		strings.Replace(base, "pending", "completed", 1), strings.Replace(base, "pending", "failed", 1),
		strings.Replace(base, `3}`, `0}`, 1), strings.Replace(base, `3}`, `86401}`, 1), strings.Replace(base, `3}`, `1.5}`, 1),
		strings.Replace(base, `3}`, `null}`, 1), strings.Replace(base, `3}`, `3,"provider_job_id":"secret"}`, 1),
		strings.Replace(base, `3}`, `3,"generate":null}`, 1), strings.Replace(base, `3}`, `3,"generate":{}}`, 1),
		strings.Replace(base, `3}`, `3,"request_id":"`+requestID+`"}`, 1),
		strings.Replace(base, `3}`, `3,"failure_code":"provider_error"}`, 1),
		strings.Replace(base, `3}`, `3,"retryable":true}`, 1),
		strings.Replace(base, requestID, "provider-job-123", 1), strings.Replace(base, requestID, "llmtw_req_00000000-0000-0000-0000-000000000000", 1),
		strings.Replace(base, requestID, strings.ToUpper(requestID), 1),
	}
	for _, data := range cases {
		t.Run(data, func(t *testing.T) {
			original := llm.ExecutionResultV1{RequestID: requestID, Kind: "compact", State: llm.ExecutionAcquired}
			value := original
			if err := json.Unmarshal([]byte(data), &value); err == nil {
				t.Fatalf("accepted %s", data)
			}
			if !reflect.DeepEqual(value, original) {
				t.Fatal("failed decode mutated destination")
			}
		})
	}
	invalid := []llm.ExecutionResultV1{
		{RequestID: requestID, Kind: "generate", State: llm.ExecutionCompleted},
		{RequestID: requestID, Kind: "generate", State: llm.ExecutionPending, RetryAfterSeconds: 3, Compact: &llm.CompactResponseV1{}},
		{RequestID: requestID, Kind: "generate", State: llm.ExecutionFailed, FailureCode: "raw-secret-message"},
		{RequestID: requestID, Kind: "generate", State: llm.ExecutionAcquired, RetryAfterSeconds: 1},
	}
	for _, value := range invalid {
		if _, err := json.Marshal(value); err == nil {
			t.Fatalf("accepted %#v", value)
		}
	}
}

func TestExecutionPrepareAndReferenceContracts(t *testing.T) {
	var generate llm.GenerateRequestV1
	var compact llm.CompactRequestV1
	if err := json.Unmarshal(readV1Fixture(t, "generate-root.json"), &generate); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(readV1Fixture(t, "compact-request.json"), &compact); err != nil {
		t.Fatal(err)
	}
	for _, request := range []llm.PrepareExecutionV1{{Generate: &generate}, {Compact: &compact}} {
		data, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		var decoded llm.PrepareExecutionV1
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []llm.PrepareExecutionV1{{}, {Generate: &generate, Compact: &compact}} {
		if _, err := json.Marshal(request); err == nil {
			t.Fatal("invalid selector accepted")
		}
	}
	for _, data := range []string{`{}`, `null`, `{"generate":null}`, `{"generate":{},"extra":true}`, `{"compact":{},"compact":{}}`} {
		var value llm.PrepareExecutionV1
		if err := json.Unmarshal([]byte(data), &value); err == nil {
			t.Fatal("invalid prepare accepted")
		}
	}
	reference := llm.ExecutionReferenceV1{RequestID: requestID, Context: generate.Context}
	data, err := json.Marshal(reference)
	if err != nil {
		t.Fatal(err)
	}
	var decoded llm.ExecutionReferenceV1
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(reference, decoded) {
		t.Fatalf("reference: %v", err)
	}
	for _, bad := range []string{`{}`, `null`, `{"request_id":"` + requestID + `","context":null}`, `{"request_id":"` + requestID + `","context":{"tenant":"t","project":"p","actor":"a","secret":true}}`, strings.Replace(string(data), `"request_id"`, `"provider_id"`, 1)} {
		if err := json.Unmarshal([]byte(bad), &decoded); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
