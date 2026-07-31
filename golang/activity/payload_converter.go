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
	if err := converter.checkAll(payloads); err != nil {
		return err
	}
	return converter.delegate.FromPayloads(payloads, valuePtrs...)
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
