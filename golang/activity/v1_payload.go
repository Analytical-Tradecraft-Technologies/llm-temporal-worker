package activity

import (
	"encoding/json"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The v1 contract codecs are separate from the pre-release engine envelope.
// Keeping them explicit prevents a caller from accidentally putting a full
// transcript or a provider response into a Temporal payload.
func MarshalGenerateV1(request llm.GenerateRequestV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(request, limits)
}

func UnmarshalGenerateV1(data []byte, limits PayloadLimits) (llm.GenerateRequestV1, error) {
	var request llm.GenerateRequestV1
	if err := unmarshalBounded(data, &request, limits); err != nil {
		return llm.GenerateRequestV1{}, err
	}
	return request, nil
}

func MarshalGenerateResponseV1(response llm.GenerateResponseV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(response, limits)
}

func UnmarshalGenerateResponseV1(data []byte, limits PayloadLimits) (llm.GenerateResponseV1, error) {
	var response llm.GenerateResponseV1
	if err := unmarshalBounded(data, &response, limits); err != nil {
		return llm.GenerateResponseV1{}, err
	}
	return response, nil
}

func MarshalCompactV1(request llm.CompactRequestV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(request, limits)
}
func UnmarshalCompactV1(data []byte, limits PayloadLimits) (llm.CompactRequestV1, error) {
	var value llm.CompactRequestV1
	if err := unmarshalBounded(data, &value, limits); err != nil {
		return llm.CompactRequestV1{}, err
	}
	return value, nil
}
func MarshalCompactResponseV1(response llm.CompactResponseV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(response, limits)
}
func UnmarshalCompactResponseV1(data []byte, limits PayloadLimits) (llm.CompactResponseV1, error) {
	var value llm.CompactResponseV1
	if err := unmarshalBounded(data, &value, limits); err != nil {
		return llm.CompactResponseV1{}, err
	}
	return value, nil
}
func MarshalQueryV1(request llm.QueryRequestV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(request, limits)
}
func UnmarshalQueryV1(data []byte, limits PayloadLimits) (llm.QueryRequestV1, error) {
	var value llm.QueryRequestV1
	if err := unmarshalBounded(data, &value, limits); err != nil {
		return llm.QueryRequestV1{}, err
	}
	return value, nil
}
func MarshalQueryResponseV1(response llm.QueryResponseV1, limits PayloadLimits) ([]byte, error) {
	return marshalBounded(response, limits)
}
func UnmarshalQueryResponseV1(data []byte, limits PayloadLimits) (llm.QueryResponseV1, error) {
	var value llm.QueryResponseV1
	if err := unmarshalBounded(data, &value, limits); err != nil {
		return llm.QueryResponseV1{}, err
	}
	return value, nil
}

func marshalBounded(value any, limits PayloadLimits) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	max := limits.inlineBytes()
	if len(data) > max {
		return nil, fmt.Errorf("v1 payload is %d bytes; limit is %d", len(data), max)
	}
	return data, nil
}

func unmarshalBounded(data []byte, value any, limits PayloadLimits) error {
	if len(data) > limits.inlineBytes() {
		return fmt.Errorf("v1 payload is %d bytes; limit is %d", len(data), limits.inlineBytes())
	}
	if err := json.Unmarshal(data, value); err != nil {
		return err
	}
	return nil
}
