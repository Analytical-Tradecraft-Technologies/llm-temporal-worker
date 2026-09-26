package llm

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPendingPollContracts(t *testing.T) {
	h := PendingOperationV1{OperationID: "operation", Kind: "generate", Provider: "openai", EndpointID: "openai-prod", ProviderOperationID: "resp_1"}
	req := PollRequestV1{Context: RequestContext{Tenant: "t", Project: "p", Actor: "a"}, Pending: h}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PollRequestV1
	if err = json.Unmarshal(b, &decoded); err != nil || !reflect.DeepEqual(decoded, req) {
		t.Fatalf("request: %s %v", b, err)
	}
	for _, kind := range []string{"generate", "compact"} {
		h.Kind = kind
		var value any
		if kind == "generate" {
			value = GenerateResponseV1{OperationKey: "key", OperationID: "operation", Pending: &h}
		} else {
			value = CompactResponseV1{OperationKey: "key", OperationID: "operation", Pending: &h}
		}
		b, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if kind == "generate" {
			var r GenerateResponseV1
			err = json.Unmarshal(b, &r)
			if r.Pending == nil {
				t.Fatal("lost handle")
			}
		} else {
			var r CompactResponseV1
			err = json.Unmarshal(b, &r)
			if r.Pending == nil {
				t.Fatal("lost compact handle")
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	h.Kind = "generate"
	for _, bad := range []PollResponseV1{{Status: "pending", Pending: h, Failure: &PollFailureV1{Code: "provider_failed", CostUnknown: true}}, {Status: "failed", Pending: h, Failure: &PollFailureV1{Code: "sensitive provider message", CostUnknown: true}}, {Status: "completed", Pending: h}} {
		if _, err = json.Marshal(bad); err == nil {
			t.Fatal("accepted invalid response")
		}
	}
	b, err = json.Marshal(PollResponseV1{Status: "failed", Pending: h, Failure: &PollFailureV1{Code: "provider_failed", CostUnknown: true}})
	if err != nil {
		t.Fatal(err)
	}
	var failure PollResponseV1
	if err = json.Unmarshal(b, &failure); err != nil {
		t.Fatal(err)
	}
	var p PollRequestV1
	b, _ = json.Marshal(req)
	b = []byte(strings.Replace(string(b), `"provider":"openai"`, `"provider":"openai","url":"https://evil"`, 1))
	if json.Unmarshal(b, &p) == nil {
		t.Fatal("accepted extra routing field")
	}
	if _, err = json.Marshal(GenerateResponseV1{OperationKey: "key", OperationID: "operation", Pending: &h, Status: ResponseStatusCompleted}); err == nil {
		t.Fatal("accepted mixed result")
	}
}
