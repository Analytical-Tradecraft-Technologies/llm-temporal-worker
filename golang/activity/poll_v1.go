package activity

import (
	"context"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

const PollActivityName = "llm.poll.v1"

type PollV1Runtime interface {
	PollV1(context.Context, llm.PollRequestV1) (llm.PollResponseV1, error)
}

func MarshalPollV1(request llm.PollRequestV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(request, limits)
}
func UnmarshalPollV1(data []byte, limits PayloadLimits) (llm.PollRequestV1, error) {
	var p llm.PollRequestV1
	err := unmarshalBounded(data, &p, limits)
	return p, err
}
func MarshalPollResponseV1(response llm.PollResponseV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(response, limits)
}
func UnmarshalPollResponseV1(data []byte, limits PayloadLimits) (llm.PollResponseV1, error) {
	var p llm.PollResponseV1
	err := unmarshalBounded(data, &p, limits)
	return p, err
}
func (a *Activities) PollV1(ctx context.Context, request llm.PollRequestV1) (*llm.PollResponseV1, error) {
	if err := validateV1Request(ctx, MarshalPollV1, request, a); err != nil {
		return nil, err
	}
	if a == nil {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	runtime, ok := a.V1Runtime.(PollV1Runtime)
	if !ok {
		return nil, ToTemporalError(UnconfiguredV1Runtime{}.unavailable(provider.PhaseStateLoad))
	}
	var response llm.PollResponseV1
	err := a.runV1(ctx, func(ctx context.Context) error {
		var err error
		response, err = runtime.PollV1(ctx, request)
		if err != nil {
			return err
		}
		_, err = MarshalPollResponseV1(response, a.payloadLimits())
		if err != nil {
			return v1OutputError("Poll", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}
