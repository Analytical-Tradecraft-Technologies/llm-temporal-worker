package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const (
	operationSchema      = "admission/v1"
	continuationSchema   = "continuation/v1"
	maxRedisInteger      = int64(1<<53 - 1)
	maxAttemptFieldBytes = 256
	maxRedisReservations = 253
)

// operationWire deliberately uses hex digests and decimal monetary strings.
// Redis Lua numbers are IEEE-754 doubles, so strings keep the wire format
// exact even when callers are close to the configured integer ceiling.
type operationWire struct {
	Schema       string                   `json:"schema"`
	ID           string                   `json:"id"`
	ScopeKey     string                   `json:"scope_key"`
	Digest       string                   `json:"request_digest"`
	State        admission.OperationState `json:"state"`
	Reserved     string                   `json:"reserved_micro_usd"`
	Incurred     string                   `json:"incurred_micro_usd"`
	Final        string                   `json:"final_micro_usd"`
	Reservations []reservationWire        `json:"reservations"`
	Config       string                   `json:"config_version"`
	Price        string                   `json:"price_version"`
	Attempt      attemptWire              `json:"attempt"`
	Result       *blobRefWire             `json:"result_ref,omitempty"`
	Token        string                   `json:"dispatch_token"`
	Lease        time.Time                `json:"lease_until"`
	Created      string                   `json:"created_at"`
	Updated      string                   `json:"updated_at"`
	Expires      time.Time                `json:"expires_at"`
}

// attemptWire is deliberately explicit. The public admission structs do not
// carry JSON tags, while the Redis Function reads these lower-case names
// directly from cjson tables.
type attemptWire struct {
	RouteID           string                      `json:"route_id"`
	EndpointID        string                      `json:"endpoint_id"`
	Provider          string                      `json:"provider"`
	ResolvedModel     string                      `json:"resolved_model"`
	ProviderRequestID string                      `json:"provider_request_id"`
	ServiceClass      string                      `json:"service_class"`
	Dispatch          admission.DispatchCertainty `json:"dispatch"`
	AttemptNumber     int                         `json:"attempt_number"`
}

type outcomeWire struct {
	Certainty         admission.DispatchCertainty `json:"certainty"`
	Incurred          string                      `json:"incurred"`
	ProviderRequestID string                      `json:"provider_request_id"`
	Attempt           attemptWire                 `json:"attempt"`
}

type reservationWire struct {
	Policy     string `json:"policy_id"`
	Window     string `json:"window_id"`
	Bucket     int64  `json:"bucket"`
	Amount     int64  `json:"amount"`
	Limit      int64  `json:"limit"`
	BucketNS   int64  `json:"bucket_nanos"`
	DurationNS int64  `json:"duration_nanos"`
}

type blobRefWire struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	Media  string `json:"media"`
}

type denialWire struct {
	RetryAfter int64  `json:"retry_after_nanos"`
	Policy     string `json:"policy_id"`
	Window     string `json:"window_id"`
	Limit      int64  `json:"limit"`
	Active     int64  `json:"active"`
	Requested  int64  `json:"requested"`
}

func encodeOperation(operation admission.Operation) ([]byte, error) {
	wire := operationWire{
		Schema: operationSchema, ID: operation.ID, ScopeKey: operation.ScopeKey,
		Digest: hex.EncodeToString(operation.RequestDigest[:]), State: operation.State,
		Reserved:     strconv.FormatInt(int64(operation.ReservedMicroUSD), 10),
		Incurred:     strconv.FormatInt(int64(operation.IncurredMicroUSD), 10),
		Final:        strconv.FormatInt(int64(operation.FinalMicroUSD), 10),
		Reservations: make([]reservationWire, len(operation.Reservations)),
		Config:       operation.ConfigVersion, Price: operation.PriceVersion,
		Attempt: encodeAttemptWire(operation.Attempt), Token: operation.DispatchToken,
		Lease: operation.LeaseUntil, Created: formatRedisTime(operation.CreatedAt),
		Updated: formatRedisTime(operation.UpdatedAt), Expires: operation.ExpiresAt,
	}
	for index, reservation := range operation.Reservations {
		wire.Reservations[index] = reservationWire{
			Policy: reservation.PolicyID, Window: reservation.WindowID,
			Bucket: reservation.Bucket, Amount: int64(reservation.Amount),
			Limit: int64(reservation.Limit), BucketNS: reservation.BucketNanos,
			DurationNS: reservation.DurationNanos,
		}
	}
	if operation.ResultRef != nil {
		wire.Result = &blobRefWire{Digest: hex.EncodeToString(operation.ResultRef.Digest[:]), Size: operation.ResultRef.Size, Media: operation.ResultRef.Media}
	}
	return json.Marshal(wire)
}

func decodeOperation(data []byte) (admission.Operation, error) {
	if len(data) == 0 || len(data) > 2<<20 {
		return admission.Operation{}, fmt.Errorf("invalid operation record size")
	}
	var wire operationWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return admission.Operation{}, fmt.Errorf("decode operation record: %w", err)
	}
	if wire.Schema != operationSchema || wire.ID == "" || wire.ScopeKey == "" || wire.State == "" || wire.Token == "" {
		return admission.Operation{}, fmt.Errorf("invalid operation record")
	}
	digest, err := decodeDigest(wire.Digest)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation request digest")
	}
	reserved, err := parseMicro(wire.Reserved)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation reservation")
	}
	incurred, err := parseMicro(wire.Incurred)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation incurred cost")
	}
	final, err := parseMicro(wire.Final)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation final cost")
	}
	operation := admission.Operation{
		ID: wire.ID, ScopeKey: wire.ScopeKey, RequestDigest: digest, State: wire.State,
		ReservedMicroUSD: reserved, IncurredMicroUSD: incurred, FinalMicroUSD: final,
		ConfigVersion: wire.Config, PriceVersion: wire.Price, Attempt: decodeAttemptWire(wire.Attempt),
		DispatchToken: wire.Token, LeaseUntil: wire.Lease, ExpiresAt: wire.Expires,
	}
	operation.CreatedAt, err = parseRedisTime(wire.Created)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation created time")
	}
	operation.UpdatedAt, err = parseRedisTime(wire.Updated)
	if err != nil {
		return admission.Operation{}, fmt.Errorf("invalid operation updated time")
	}
	operation.Reservations = make([]admission.WindowReservation, len(wire.Reservations))
	for index, reservation := range wire.Reservations {
		if reservation.Policy == "" || reservation.Window == "" || reservation.Amount < 0 || reservation.Limit < 0 || reservation.BucketNS <= 0 || reservation.DurationNS <= 0 {
			return admission.Operation{}, fmt.Errorf("invalid operation reservation %d", index)
		}
		operation.Reservations[index] = admission.WindowReservation{PolicyID: reservation.Policy, WindowID: reservation.Window, Bucket: reservation.Bucket, Amount: pricing.MicroUSD(reservation.Amount), Limit: pricing.MicroUSD(reservation.Limit), BucketNanos: reservation.BucketNS, DurationNanos: reservation.DurationNS}
	}
	if wire.Result != nil {
		result, err := decodeBlobRef(*wire.Result)
		if err != nil {
			return admission.Operation{}, err
		}
		operation.ResultRef = &result
	}
	return operation, nil
}

func decodeBlobRef(wire blobRefWire) (state.BlobRef, error) {
	digest, err := decodeDigest(wire.Digest)
	if err != nil || wire.Size < 0 || wire.Media == "" {
		return state.BlobRef{}, fmt.Errorf("invalid result reference")
	}
	return state.BlobRef{Digest: digest, Size: wire.Size, Media: wire.Media}, nil
}

func encodeAttemptWire(attempt admission.AttemptFacts) attemptWire {
	return attemptWire{
		RouteID:           attempt.RouteID,
		EndpointID:        attempt.EndpointID,
		Provider:          attempt.Provider,
		ResolvedModel:     attempt.ResolvedModel,
		ProviderRequestID: attempt.ProviderRequestID,
		ServiceClass:      attempt.ServiceClass,
		Dispatch:          attempt.Dispatch,
		AttemptNumber:     attempt.AttemptNumber,
	}
}

func decodeAttemptWire(wire attemptWire) admission.AttemptFacts {
	return admission.AttemptFacts{
		RouteID:           wire.RouteID,
		EndpointID:        wire.EndpointID,
		Provider:          wire.Provider,
		ResolvedModel:     wire.ResolvedModel,
		ProviderRequestID: wire.ProviderRequestID,
		ServiceClass:      wire.ServiceClass,
		Dispatch:          wire.Dispatch,
		AttemptNumber:     wire.AttemptNumber,
	}
}

func encodeAttempt(attempt admission.AttemptFacts) ([]byte, error) {
	if err := validateAttempt(attempt); err != nil {
		return nil, err
	}
	return json.Marshal(encodeAttemptWire(attempt))
}

func validateAttempt(attempt admission.AttemptFacts) error {
	for name, value := range map[string]string{
		"route_id":            attempt.RouteID,
		"endpoint_id":         attempt.EndpointID,
		"provider":            attempt.Provider,
		"resolved_model":      attempt.ResolvedModel,
		"provider_request_id": attempt.ProviderRequestID,
		"service_class":       attempt.ServiceClass,
	} {
		if len(value) > maxAttemptFieldBytes {
			return fmt.Errorf("attempt %s exceeds Redis field limit", name)
		}
	}
	if attempt.AttemptNumber < 0 || int64(attempt.AttemptNumber) > 1_000_000 {
		return fmt.Errorf("invalid attempt number")
	}
	return nil
}

func decodeAttempt(data []byte) (admission.AttemptFacts, error) {
	var wire attemptWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return admission.AttemptFacts{}, fmt.Errorf("decode attempt: %w", err)
	}
	if wire.AttemptNumber < 0 {
		return admission.AttemptFacts{}, fmt.Errorf("invalid attempt number")
	}
	return decodeAttemptWire(wire), nil
}

func encodeOutcome(outcome admission.AttemptOutcome) ([]byte, error) {
	if err := validateAttempt(outcome.Attempt); err != nil {
		return nil, err
	}
	return json.Marshal(outcomeWire{
		Certainty:         outcome.Certainty,
		Incurred:          strconv.FormatInt(int64(outcome.Incurred), 10),
		ProviderRequestID: outcome.ProviderRequestID,
		Attempt:           encodeAttemptWire(outcome.Attempt),
	})
}

func decodeOutcome(data []byte) (admission.AttemptOutcome, error) {
	var wire outcomeWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return admission.AttemptOutcome{}, fmt.Errorf("decode attempt outcome: %w", err)
	}
	incurred, err := parseMicro(wire.Incurred)
	if err != nil {
		return admission.AttemptOutcome{}, err
	}
	return admission.AttemptOutcome{
		Certainty:         wire.Certainty,
		Incurred:          incurred,
		ProviderRequestID: wire.ProviderRequestID,
		Attempt:           decodeAttemptWire(wire.Attempt),
	}, nil
}

func encodeReservations(reservations []admission.WindowReservation) ([]byte, error) {
	encoded := make([]reservationWire, len(reservations))
	for index, reservation := range reservations {
		encoded[index] = reservationWire{Policy: reservation.PolicyID, Window: reservation.WindowID, Bucket: reservation.Bucket, Amount: int64(reservation.Amount), Limit: int64(reservation.Limit), BucketNS: reservation.BucketNanos, DurationNS: reservation.DurationNanos}
	}
	return json.Marshal(encoded)
}

func decodeDenial(data []byte) (*admission.Denial, error) {
	var wire denialWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return nil, fmt.Errorf("decode admission denial: %w", err)
	}
	if wire.Policy == "" || wire.Window == "" || wire.Limit < 0 || wire.Active < 0 || wire.Requested < 0 {
		return nil, fmt.Errorf("invalid admission denial")
	}
	return &admission.Denial{RetryAfter: time.Duration(wire.RetryAfter), PolicyID: wire.Policy, WindowID: wire.Window, Limit: pricing.MicroUSD(wire.Limit), Active: pricing.MicroUSD(wire.Active), Requested: pricing.MicroUSD(wire.Requested)}, nil
}

type continuationWire struct {
	Schema string             `json:"schema"`
	Value  state.Continuation `json:"value"`
}

type continuationStateAlias state.Continuation

type continuationDecodeWire struct {
	Schema string `json:"schema"`
	Value  struct {
		continuationStateAlias
		Transcript json.RawMessage `json:"Transcript"`
	} `json:"value"`
}

func encodeContinuation(value state.Continuation) ([]byte, error) {
	// Continuation transcripts are represented as arrays on the v1 wire. A
	// zero-value continuation may carry a nil slice, which encoding/json would
	// otherwise emit as JSON null and the strict item decoder correctly rejects.
	// Normalize the internal nil-as-empty representation at this persistence
	// boundary without mutating the caller's continuation.
	encoded := value.Clone()
	if encoded.Transcript == nil {
		encoded.Transcript = []llm.Item{}
	}
	return json.Marshal(continuationWire{Schema: continuationSchema, Value: encoded})
}

func decodeContinuation(data []byte) (state.Continuation, error) {
	if len(data) == 0 || len(data) > 4<<20 {
		return state.Continuation{}, fmt.Errorf("invalid continuation record size")
	}
	var wire continuationDecodeWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return state.Continuation{}, fmt.Errorf("decode continuation record: %w", err)
	}
	if wire.Schema != continuationSchema {
		return state.Continuation{}, fmt.Errorf("unsupported continuation schema")
	}
	transcript, err := llm.DecodeItems(wire.Value.Transcript)
	if err != nil {
		return state.Continuation{}, fmt.Errorf("decode continuation transcript: %w", err)
	}
	value := state.Continuation(wire.Value.continuationStateAlias)
	value.Transcript = transcript
	return value, nil
}

func decodeDigest(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return digest, fmt.Errorf("invalid digest")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func parseMicro(value string) (pricing.MicroUSD, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 || parsed > maxRedisInteger {
		return 0, fmt.Errorf("invalid microUSD")
	}
	return pricing.MicroUSD(parsed), nil
}

const (
	// redisZeroTime is the legacy marker for an unset timestamp.
	redisZeroTime = "0:0"
	// redisUnixEpochTime keeps the Unix epoch distinct from redisZeroTime.
	redisUnixEpochTime = "+0:0"
)

func formatRedisTime(value time.Time) string {
	if value.IsZero() {
		return redisZeroTime
	}
	seconds := value.Unix()
	micros := int64(value.Nanosecond() / 1000)
	if seconds == 0 && micros == 0 {
		return redisUnixEpochTime
	}
	return strconv.FormatInt(seconds, 10) + ":" + strconv.FormatInt(micros, 10)
}

func parseRedisTime(value string) (time.Time, error) {
	if value == redisZeroTime {
		return time.Time{}, nil
	}
	if value == redisUnixEpochTime {
		return time.Unix(0, 0).UTC(), nil
	}
	var seconds, micros int64
	if _, err := fmt.Sscanf(value, "%d:%d", &seconds, &micros); err != nil || micros < 0 || micros >= 1_000_000 {
		return time.Time{}, fmt.Errorf("invalid redis timestamp")
	}
	return time.Unix(seconds, micros*1000).UTC(), nil
}

func validateRedisInteger(value int64) error {
	if value < 0 || value > maxRedisInteger {
		return fmt.Errorf("integer exceeds Redis safe range")
	}
	return nil
}

func validateMicro(value pricing.MicroUSD) error { return validateRedisInteger(int64(value)) }

func validJSONSize(value []byte, max int) error {
	if max <= 0 || len(value) > max {
		return fmt.Errorf("record exceeds configured size")
	}
	if len(value) > math.MaxInt32 {
		return fmt.Errorf("record size is invalid")
	}
	return nil
}
