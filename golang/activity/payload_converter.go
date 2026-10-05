package activity

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// BoundedDataConverter wraps Temporal's default converter with the same
// application payload limit used by Activity validation. The check happens on
// raw payload bytes before the SDK decodes a registered Activity argument,
// which is the only point that can protect the normal Temporal dispatch path.
// It intentionally applies to every payload owned by this worker, including
// Activity results and workflow arguments, so no registration path can bypass
// the boundary.
func BoundedDataConverter(limits PayloadLimits) converter.DataConverter {
	return &boundedDataConverter{delegate: converter.GetDefaultDataConverter(), limits: limits}
}

type boundedDataConverter struct {
	delegate converter.DataConverter
	limits   PayloadLimits
}

func (converter *boundedDataConverter) ToPayload(value interface{}) (*commonpb.Payload, error) {
	payload, err := converter.delegate.ToPayload(value)
	if err != nil {
		return nil, err
	}
	if err := converter.check(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (converter *boundedDataConverter) FromPayload(payload *commonpb.Payload, valuePtr interface{}) error {
	if deferredDecode(valuePtr) {
		return converter.delegate.FromPayload(payload, valuePtr)
	}
	if err := converter.check(payload); err != nil {
		return err
	}
	return converter.delegate.FromPayload(payload, valuePtr)
}

func (converter *boundedDataConverter) ToPayloads(values ...interface{}) (*commonpb.Payloads, error) {
	payloads, err := converter.delegate.ToPayloads(values...)
	if err != nil {
		return nil, err
	}
	if err := converter.checkAll(payloads); err != nil {
		return nil, err
	}
	return payloads, nil
}

func (converter *boundedDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	if payloads != nil {
		for index, payload := range payloads.Payloads {
			if index < len(valuePtrs) && deferredDecode(valuePtrs[index]) {
				continue
			}
			if err := converter.check(payload); err != nil {
				return err
			}
		}
	}
	return converter.delegate.FromPayloads(payloads, valuePtrs...)
}

// deferredDecode reports a raw-payload target. The v1 Activity handlers and
// the registered workflows take converter.RawValue so they can apply the same
// inline limit and strict decode in their own code, where the failure becomes
// a typed, non-retryable llm_invalid_argument. Rejecting here instead would surface an untyped,
// retryable SDK wrapper error before the handler could classify it.
func deferredDecode(valuePtr interface{}) bool {
	_, ok := valuePtr.(*converter.RawValue)
	return ok
}

// DecodeBoundedPayload applies the inline limit and the strict contract decode
// to a raw payload received by a handler registered with converter.RawValue.
// It reports only success: decoder text can echo caller values, so callers map
// a false result to their own stable, typed failure.
func DecodeBoundedPayload[T any](limits PayloadLimits, input converter.RawValue) (T, bool) {
	var value T
	payload := input.Payload()
	if payload == nil || len(payload.GetData()) > limits.inlineBytes() {
		return value, false
	}
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
		var zero T
		return zero, false
	}
	return value, true
}

func (converter *boundedDataConverter) ToString(payload *commonpb.Payload) string {
	return converter.delegate.ToString(payload)
}

func (converter *boundedDataConverter) ToStrings(payloads *commonpb.Payloads) []string {
	return converter.delegate.ToStrings(payloads)
}

func (converter *boundedDataConverter) checkAll(payloads *commonpb.Payloads) error {
	if payloads == nil {
		return nil
	}
	for _, payload := range payloads.Payloads {
		if err := converter.check(payload); err != nil {
			return err
		}
	}
	return nil
}

func (converter *boundedDataConverter) check(payload *commonpb.Payload) error {
	if payload == nil {
		return nil
	}
	max := converter.limits.inlineBytes()
	if len(payload.Data) > max {
		return fmt.Errorf("Temporal payload is %d bytes; limit is %d", len(payload.Data), max)
	}
	return nil
}
