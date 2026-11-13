package activity

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// BoundedDataConverter wraps Temporal's default converter with the same
// application payload limit used by Activity validation. The check happens on
// raw payload bytes before the SDK decodes a registered Activity argument,
// which is the only point that can protect the normal Temporal dispatch path.
// It intentionally applies to every payload owned by this worker, including
// Activity results and workflow arguments, so no registration path can bypass
// the boundary.
//
// Inside a workflow run that pinned its start-time limit (see
// WithPinnedPayloadLimits), the converter applies the pinned limit instead,
// so lowering the configured limit does not change what replay decodes or
// encodes (#1511).
func BoundedDataConverter(limits PayloadLimits) converter.DataConverter {
	return WrappedBoundedDataConverter(limits, nil)
}

// WrappedBoundedDataConverter is BoundedDataConverter with wrap, for example a
// payload codec, applied around the bounded converter. Pinning rebuilds the
// whole stack around a converter bounded by the pinned limit; the SDK's
// CodecDataConverter does not pass the workflow context on to the converter
// it wraps, so the pinning has to happen outside it.
func WrappedBoundedDataConverter(limits PayloadLimits, wrap func(converter.DataConverter) converter.DataConverter) converter.DataConverter {
	if wrap == nil {
		wrap = func(dataConverter converter.DataConverter) converter.DataConverter { return dataConverter }
	}
	return &pinnableDataConverter{DataConverter: wrap(newBoundedDataConverter(limits)), limits: limits, wrap: wrap}
}

func newBoundedDataConverter(limits PayloadLimits) *boundedDataConverter {
	return &boundedDataConverter{delegate: converter.GetDefaultDataConverter(), limits: limits}
}

// pinnableDataConverter is the worker's outermost converter. The SDK binds it
// to the workflow context for every payload a workflow encodes or decodes
// (Activity and child arguments and results, side effects, continue-as-new
// arguments), which is where a pinned start-time limit is read.
type pinnableDataConverter struct {
	converter.DataConverter
	limits PayloadLimits
	wrap   func(converter.DataConverter) converter.DataConverter
}

var _ converter.ContextAware = (*pinnableDataConverter)(nil)

func (dataConverter *pinnableDataConverter) WithWorkflowContext(ctx converter.WorkflowContext) converter.DataConverter {
	pinned, ok := pinnedPayloadLimits(ctx)
	if !ok || pinned.inlineBytes() == dataConverter.limits.inlineBytes() {
		return dataConverter
	}
	return dataConverter.wrap(newBoundedDataConverter(pinned))
}

// WithContext keeps the configured limit for Activities and clients: a new
// Activity execution always runs under the limit the worker has now.
func (dataConverter *pinnableDataConverter) WithContext(context.Context) converter.DataConverter {
	return dataConverter
}

type pinnedPayloadLimitsKey struct{}

// WithPinnedPayloadLimits returns a workflow context whose payloads the
// worker's converter bounds by limits instead of the configured limit. A
// workflow pins the limit it recorded when its run started, so a history
// replays with the payload checks it was recorded under even after the
// worker restarts with a smaller server.inline_payload_bytes.
func WithPinnedPayloadLimits(ctx workflow.Context, limits PayloadLimits) workflow.Context {
	return workflow.WithValue(ctx, pinnedPayloadLimitsKey{}, PayloadLimits{MaxInlineBytes: limits.inlineBytes()})
}

// WorkflowPayloadLimits is the limit pinned on ctx, or fallback when the run
// pinned none.
func WorkflowPayloadLimits(ctx workflow.Context, fallback PayloadLimits) PayloadLimits {
	if pinned, ok := pinnedPayloadLimits(ctx); ok {
		return pinned
	}
	return fallback
}

func pinnedPayloadLimits(ctx converter.WorkflowContext) (PayloadLimits, bool) {
	if ctx == nil {
		return PayloadLimits{}, false
	}
	pinned, ok := ctx.Value(pinnedPayloadLimitsKey{}).(PayloadLimits)
	return pinned, ok
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
	if preEncoded(value) {
		return payload, nil
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
	for index, payload := range payloads.GetPayloads() {
		if index < len(values) && preEncoded(values[index]) {
			continue
		}
		if err := converter.check(payload); err != nil {
			return nil, err
		}
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

// preEncoded reports a value that is already a payload, which the default
// converter passes through unchanged. Only EncodeBoundedPayload builds one, and
// it has already applied the limit that governs that payload; the worker's
// converter must not apply its configured limit again (see
// EncodeBoundedPayload).
func preEncoded(value interface{}) bool {
	_, ok := value.(converter.RawValue)
	return ok
}

// EncodeBoundedPayload encodes value the way the worker's converter does and
// applies limits to it, returning the payload as a converter.RawValue that the
// worker's converter passes through unchanged (a codec, if configured, still
// encodes it).
//
// A workflow returns its result through it. The SDK binds the converter for a
// workflow's return value to the workflow's root context before workflow code
// runs, so that converter cannot see a limit the run pinned (see
// WithPinnedPayloadLimits); encoding the result in workflow code under the
// pinned limit keeps completion the same on replay after the configured limit
// is lowered (#1511). The error for an oversize value is the one the worker's
// converter returns.
func EncodeBoundedPayload(limits PayloadLimits, value any) (converter.RawValue, error) {
	payload, err := newBoundedDataConverter(limits).ToPayload(value)
	if err != nil {
		return converter.RawValue{}, err
	}
	return converter.NewRawValue(payload), nil
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

// FitsInlinePayload reports whether value, encoded the way this worker encodes
// its own Temporal payloads, stays within the inline limit. The worker's JSON
// encoding can be larger than the caller's, since Go escapes <, > and & while
// other encoders need not.
func FitsInlinePayload(limits PayloadLimits, value any) bool {
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	return err == nil && len(payload.GetData()) <= limits.inlineBytes()
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
