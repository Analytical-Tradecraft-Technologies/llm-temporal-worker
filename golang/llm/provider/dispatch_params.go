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
	if call.Family != family || call.EndpointID != endpointID {
		var params P
		return params, NewDispatchError("call does not belong to this adapter", DispatchNotDispatched)
	}
	params, ok := SDKParamsAs[P](call)
	if !ok {
		return params, NewDispatchError("call SDK parameters have unexpected type", DispatchNotDispatched)
	}
	return params, nil
}

// SDKParamsAs returns the call's SDK parameters when they are a P or a
// non-nil *P.
func SDKParamsAs[P any](call Call) (P, bool) {
	switch value := call.SDKParams.(type) {
	case P:
		return value, true
	case *P:
		if value != nil {
			return *value, true
		}
	}
	var zero P
	return zero, false
}

// OutputSchema returns a copy of the caller's json_schema output schema, for
// Call.OutputSchema, or nil when the request asks for no JSON schema output.
func OutputSchema(request llm.Request) json.RawMessage {
	if request.Output == nil || request.Output.Format.Kind != llm.OutputKindJSONSchema {
		return nil
	}
	return append(json.RawMessage(nil), request.Output.Format.Schema...)
}
