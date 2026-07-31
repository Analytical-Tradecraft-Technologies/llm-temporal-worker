package activity

import (
	"encoding/json"
	"fmt"
	"time"
)

// PayloadLimits are application-level limits below Temporal's service limits.
// They are enforced before the request reaches an Activity implementation.
type PayloadLimits struct {
	MaxInlineBytes int
}

func (limits PayloadLimits) inlineBytes() int {
	if limits.MaxInlineBytes <= 0 {
		return DefaultInlineBytes
	}
	return limits.MaxInlineBytes
}

func MarshalRequest(request GenerateRequest, limits PayloadLimits) ([]byte, error) {
	if _, err := request.Validate(limits.inlineBytes()); err != nil {
		return nil, err
	}
	return json.Marshal(request)
}

func UnmarshalRequest(data []byte, limits PayloadLimits) (GenerateRequest, error) {
	if err := rejectOversizedPayload(data, limits); err != nil {
		return GenerateRequest{}, err
	}
	var request GenerateRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return GenerateRequest{}, err
	}
	if _, err := request.Validate(limits.inlineBytes()); err != nil {
		return GenerateRequest{}, err
	}
	return request, nil
}

func MarshalResponse(response GenerateResponse, limits PayloadLimits) ([]byte, error) {
	if err := response.Validate(limits.inlineBytes()); err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func UnmarshalResponse(data []byte, limits PayloadLimits) (GenerateResponse, error) {
	if err := rejectOversizedPayload(data, limits); err != nil {
		return GenerateResponse{}, err
	}
	var response GenerateResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return GenerateResponse{}, err
	}
	if err := response.Validate(limits.inlineBytes()); err != nil {
		return GenerateResponse{}, err
	}
	return response, nil
}

// rejectOversizedPayload runs before JSON decoding so an untrusted Temporal
// payload cannot allocate or traverse an arbitrarily large object before the
// application-level limit is enforced. Marshal paths and v1 codecs use the
// same limit, keeping every Activity boundary fail-closed and bounded.
func rejectOversizedPayload(data []byte, limits PayloadLimits) error {
	max := limits.inlineBytes()
	if len(data) > max {
		return fmt.Errorf("payload is %d bytes; limit is %d", len(data), max)
	}
	return nil
}

func ValidateBlobRef(ref BlobRef, nowUnixNano int64) error {
	if err := ref.Validate(time.Unix(0, nowUnixNano)); err != nil {
		return fmt.Errorf("invalid blob reference: %w", err)
	}
	return nil
}
