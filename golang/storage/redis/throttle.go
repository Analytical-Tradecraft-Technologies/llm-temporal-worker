package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// ThrottleKind identifies an operational (non-monetary) Redis limit. These
// limits deliberately remain separate from the monetary admission ledger.
type ThrottleKind string

const (
	ThrottleRequests    ThrottleKind = "requests"
	ThrottleTokens      ThrottleKind = "tokens"
	ThrottleConcurrency ThrottleKind = "concurrency"
)

var (
	ErrThrottleDenied   = errors.New("Redis throttle limit denied")
	ErrThrottleConflict = errors.New("Redis throttle reservation conflict")
	ErrThrottleNotFound = errors.New("Redis throttle reservation not found")
)

// ThrottleLimit describes one atomically acquired operational limit. Scope is
// HMACed before it becomes a Redis key and is never stored in the reservation.
type ThrottleLimit struct {
	Kind   ThrottleKind
	Scope  string
	Amount int64
	Limit  int64
	Window time.Duration
}

type ThrottleReservationLimit struct {
	Kind      ThrottleKind
	KeyDigest string
	Amount    int64
}

// ThrottleReservation is an opaque lease returned by Acquire and Lookup. It
// contains only key digests, so a Redis record cannot expose tenant scopes.
type ThrottleReservation struct {
	ID     string
	Digest string
	Limits []ThrottleReservationLimit
}

type ThrottleAcquireResult struct {
	Existing    bool
	Reservation ThrottleReservation
}

type ThrottleOptions struct {
	Client          redisclient.Scripter
	Reader          StringReader
	Invoker         FunctionInvoker
	Mode            AdmissionMode
	FunctionVersion string
	Keys            KeyOptions
	MaxRecordBytes  int
}

type ThrottleStore struct {
	space          keySpace
	reader         StringReader
	invoke         FunctionInvoker
	function       string
	maxRecordBytes int
}

func NewThrottleStore(options ThrottleOptions) (*ThrottleStore, error) {
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	if options.MaxRecordBytes <= 0 {
		options.MaxRecordBytes = 64 << 10
	}
	function := options.FunctionVersion
	if function == "" {
		function = ThrottleFunctionVersion
	}
	invoke := options.Invoker
	if invoke == nil {
		client, ok := options.Client.(redisclient.Scripter)
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis throttle client is required")
		}
		if options.Mode != AdmissionModeFunction && options.Mode != AdmissionModeLua {
			return nil, fmt.Errorf("Redis throttle mode must be function or lua")
		}
		invoke = throttleRedisInvoker{client: client, mode: options.Mode, version: function}
	}
	reader := options.Reader
	if reader == nil {
		client, ok := options.Client.(interface {
			Get(context.Context, string) *redisclient.StringCmd
		})
		if !ok || client == nil {
			return nil, fmt.Errorf("Redis throttle reader is required")
		}
		reader = redisReader{client: client}
	}
	return &ThrottleStore{space: space, reader: reader, invoke: invoke, function: function, maxRecordBytes: options.MaxRecordBytes}, nil
}

func (store *ThrottleStore) Acquire(ctx context.Context, id string, limits []ThrottleLimit) (ThrottleAcquireResult, error) {
	if err := ctx.Err(); err != nil {
		return ThrottleAcquireResult{}, err
	}
	if id == "" || len(limits) == 0 || len(limits) > 64 {
		return ThrottleAcquireResult{}, fmt.Errorf("invalid Redis throttle reservation")
	}
	wire := throttleWire{Schema: "throttle/v1", ID: id, Digest: throttleDigest(limits), Limits: make([]throttleWireLimit, len(limits))}
	keys := make([]string, 1, len(limits)+1)
	keys[0] = store.space.throttleReservationKey(id)
	seen := make(map[string]struct{}, len(limits))
	ttl := int64(1)
	for index, limit := range limits {
		if err := validateThrottleLimit(limit); err != nil {
			return ThrottleAcquireResult{}, err
		}
		digest := store.space.throttleDigest(string(limit.Kind), limit.Scope)
		if _, exists := seen[digest]; exists {
			return ThrottleAcquireResult{}, fmt.Errorf("duplicate Redis throttle limit")
		}
		seen[digest] = struct{}{}
		keys = append(keys, store.space.throttleKey(string(limit.Kind), limit.Scope))
		wire.Limits[index] = throttleWireLimit{Kind: limit.Kind, KeyDigest: digest, Amount: limit.Amount, Limit: limit.Limit}
		seconds := int64((limit.Window + time.Second - 1) / time.Second)
		if seconds > ttl {
			ttl = seconds
		}
	}
	payload, err := json.Marshal(wire)
	if err != nil || len(payload) > store.maxRecordBytes {
		return ThrottleAcquireResult{}, fmt.Errorf("encode Redis throttle reservation")
	}
	result, err := store.invoke.Run(ctx, store.function, keys, "acquire", string(payload), strconv.FormatInt(ttl, 10))
	if err != nil {
		return ThrottleAcquireResult{}, resolveMutationError(ctx, err)
	}
	status, data := parseThrottleResult(result)
	switch status {
	case "created", "existing":
		reservation, err := decodeThrottleReservation([]byte(data))
		if err != nil {
			return ThrottleAcquireResult{}, ErrUnavailable
		}
		return ThrottleAcquireResult{Existing: status == "existing", Reservation: reservation}, nil
	case "denied":
		return ThrottleAcquireResult{}, ErrThrottleDenied
	case "conflict":
		return ThrottleAcquireResult{}, ErrThrottleConflict
	case "not_found":
		return ThrottleAcquireResult{}, ErrThrottleNotFound
	default:
		return ThrottleAcquireResult{}, mapStatus(status)
	}
}

// Lookup resolves an ambiguous acquire without mutating Redis.
func (store *ThrottleStore) Lookup(ctx context.Context, id string) (ThrottleReservation, error) {
	if err := ctx.Err(); err != nil {
		return ThrottleReservation{}, err
	}
	data, err := store.reader.Get(ctx, store.space.throttleReservationKey(id))
	if errors.Is(err, redisclient.Nil) {
		return ThrottleReservation{}, ErrThrottleNotFound
	}
	if err != nil {
		return ThrottleReservation{}, resolveMutationError(ctx, err)
	}
	if len(data) > store.maxRecordBytes {
		return ThrottleReservation{}, ErrUnavailable
	}
	return decodeThrottleReservation([]byte(data))
}

// Release is idempotent. A missing reservation means a previous release
// already committed or its TTL elapsed; callers can treat that as success.
func (store *ThrottleStore) Release(ctx context.Context, reservation ThrottleReservation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reservation.ID == "" || len(reservation.Limits) == 0 {
		return fmt.Errorf("invalid Redis throttle reservation")
	}
	keys := make([]string, 1, len(reservation.Limits)+1)
	keys[0] = store.space.throttleReservationKey(reservation.ID)
	args := []string{"release", reservation.Digest}
	for _, limit := range reservation.Limits {
		if limit.KeyDigest == "" || limit.Amount <= 0 || limit.Amount > maxRedisInteger {
			return fmt.Errorf("invalid Redis throttle reservation limit")
		}
		keys = append(keys, store.space.throttleKeyDigest(string(limit.Kind), limit.KeyDigest))
		args = append(args, strconv.FormatInt(limit.Amount, 10))
	}
	result, err := store.invoke.Run(ctx, store.function, keys, args...)
	if err != nil {
		return resolveMutationError(ctx, err)
	}
	status, _ := parseThrottleResult(result)
	if status == "not_found" || status == "released" {
		return nil
	}
	return mapStatus(status)
}

type throttleWire struct {
	Schema string              `json:"schema"`
	ID     string              `json:"id"`
	Digest string              `json:"digest"`
	Limits []throttleWireLimit `json:"limits"`
}

type throttleWireLimit struct {
	Kind      ThrottleKind `json:"kind"`
	KeyDigest string       `json:"key_digest"`
	Amount    int64        `json:"amount"`
	Limit     int64        `json:"limit"`
}

func throttleDigest(limits []ThrottleLimit) string {
	h := sha256.New()
	for _, limit := range limits {
		_, _ = h.Write([]byte(string(limit.Kind)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(limit.Scope))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(limit.Amount, 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(limit.Limit, 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(limit.Window.String()))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func validateThrottleLimit(limit ThrottleLimit) error {
	if limit.Kind != ThrottleRequests && limit.Kind != ThrottleTokens && limit.Kind != ThrottleConcurrency {
		return fmt.Errorf("invalid Redis throttle kind")
	}
	if limit.Scope == "" || limit.Amount <= 0 || limit.Limit <= 0 || limit.Amount > limit.Limit || limit.Limit > maxRedisInteger || limit.Window <= 0 {
		return fmt.Errorf("invalid Redis throttle limit")
	}
	return nil
}

func parseThrottleResult(result []any) (string, string) {
	if len(result) == 0 {
		return "", ""
	}
	status, _ := result[0].(string)
	data := ""
	if len(result) > 1 {
		data, _ = result[1].(string)
	}
	return status, data
}

func decodeThrottleReservation(data []byte) (ThrottleReservation, error) {
	var wire throttleWire
	if len(data) == 0 || json.Unmarshal(data, &wire) != nil || wire.Schema != "throttle/v1" || wire.ID == "" || wire.Digest == "" || len(wire.Limits) == 0 {
		return ThrottleReservation{}, ErrUnavailable
	}
	reservation := ThrottleReservation{ID: wire.ID, Digest: wire.Digest, Limits: make([]ThrottleReservationLimit, len(wire.Limits))}
	for index, limit := range wire.Limits {
		if limit.Kind == "" || limit.KeyDigest == "" || limit.Amount <= 0 || limit.Amount > maxRedisInteger {
			return ThrottleReservation{}, ErrUnavailable
		}
		reservation.Limits[index] = ThrottleReservationLimit{Kind: limit.Kind, KeyDigest: limit.KeyDigest, Amount: limit.Amount}
	}
	return reservation, nil
}
