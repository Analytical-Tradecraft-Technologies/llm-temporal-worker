package provider

import (
	"encoding/json"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// DispatchParams returns the SDK parameters of a call that an adapter for
// family and endpointID compiled. Compile stores them as a P value; a non-nil
// *P is accepted too. A call from another adapter, or with parameters of any
// other type, is refused as not dispatched.
func DispatchParams[P any](call Call, family Family, endpointID string) (P, error) {
	var params P
	if call.Family != family || call.EndpointID != endpointID {
		return params, NewDispatchError("call does not belong to this adapter", DispatchNotDispatched)
	}
	switch value := call.SDKParams.(type) {
	case P:
		return value, nil
	case *P:
		if value != nil {
			return *value, nil
		}
	}
	return params, NewDispatchError("call SDK parameters have unexpected type", DispatchNotDispatched)
}

// OutputSchema returns a copy of the caller's json_schema output schema, for
// Call.OutputSchema, or nil when the request asks for no JSON schema output.
func OutputSchema(request llm.Request) json.RawMessage {
	if request.Output == nil || request.Output.Format.Kind != llm.OutputKindJSONSchema {
		return nil
	}
	return append(json.RawMessage(nil), request.Output.Format.Schema...)
}
