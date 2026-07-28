package redis

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

const (
	defaultHashTag = "admission"
)

var keyPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// KeyOptions controls the names used by the shared Redis stores. The secret
// is used only to derive opaque key components; raw operation and tenant
// identifiers never become Redis key names.
type KeyOptions struct {
	Prefix    string
	HashTag   string
	KeySecret []byte
}

// NewKeyOptions constructs one immutable, validated namespace for all
// worker-owned Redis stores. Callers should pass the effective configuration
// prefix rather than relying on a store-local default.
func NewKeyOptions(prefix, hashTag string, keySecret []byte) (KeyOptions, error) {
	options := KeyOptions{Prefix: prefix, HashTag: hashTag, KeySecret: keySecret}
	if _, err := newKeySpace(options); err != nil {
		return KeyOptions{}, err
	}
	return options, nil
}

type keySpace struct {
	prefix string
	tag    string
	secret []byte
}

func newKeySpace(options KeyOptions) (keySpace, error) {
	prefix := options.Prefix
	if !keyPrefixPattern.MatchString(prefix) {
		return keySpace{}, fmt.Errorf("invalid Redis key prefix")
	}
	tag := options.HashTag
	if tag == "" {
		tag = defaultHashTag
	}
	if !safeKeyPart(tag) {
		return keySpace{}, fmt.Errorf("invalid Redis hash tag")
	}
	if len(options.KeySecret) < 32 {
		return keySpace{}, fmt.Errorf("Redis key secret must be at least 32 bytes")
	}
	return keySpace{prefix: prefix, tag: tag, secret: append([]byte(nil), options.KeySecret...)}, nil
}

func safeKeyPart(value string) bool {
	if value == "" || len(value) > 64 || strings.ContainsAny(value, "{} \t\r\n") {
		return false
	}
	return true
}

func (space keySpace) admissionPrefix() string {
	return space.prefix + ":{" + space.tag + "}:"
}

func (space keySpace) admissionKey(kind string, values ...string) string {
	return space.admissionPrefix() + kind + ":" + space.digest(kind, values...)
}

func (space keySpace) budgetKey(policy, window string) string {
	return space.admissionPrefix() + "budget:" + space.digest("budget", policy, window)
}

// durableBudgetKey and durableBudgetExpiryKey are kept separate from the
// legacy admission budget hash. The durable materializer stores nano-USD
// values and must never be read as legacy micro-USD state.
func (space keySpace) durableBudgetKey(policy, window string) string {
	return space.admissionPrefix() + "durable-budget:" + space.digest("durable-budget", policy, window)
}

func (space keySpace) durableBudgetExpiryKey(policy, window string) string {
	return space.admissionPrefix() + "durable-budget-expiry:" + space.digest("durable-budget-expiry", policy, window)
}

func (space keySpace) durableBudgetOperationKey(generation, operation string) string {
	return space.admissionPrefix() + "durable-budget-operation:" + space.digest("durable-budget-operation", generation, operation)
}

func (space keySpace) throttleDigest(kind, scope string) string {
	return space.digest("throttle", kind, scope)
}

func (space keySpace) throttleKey(kind, scope string) string {
	return space.throttleKeyDigest(kind, space.throttleDigest(kind, scope))
}

func (space keySpace) throttleKeyDigest(kind, digest string) string {
	return space.admissionPrefix() + "throttle:" + kind + ":" + digest
}

func (space keySpace) throttleReservationKey(id string) string {
	return space.admissionKey("throttle-reservation", id)
}

func (space keySpace) digest(purpose string, values ...string) string {
	mac := hmac.New(sha256.New, space.secret)
	write := func(value string) {
		_, _ = mac.Write([]byte{byte(len(value) >> 24), byte(len(value) >> 16), byte(len(value) >> 8), byte(len(value))})
		_, _ = mac.Write([]byte(value))
	}
	write(purpose)
	for _, value := range values {
		write(value)
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func (space keySpace) scopeKey(scope string) string {
	return space.admissionKey("scope", scope)
}

func (space keySpace) operationIndexKey(id string) string {
	return space.admissionKey("operation-index", id)
}

func (space keySpace) operationKey(scope, id string) string {
	return space.admissionKey("operation", scope, id)
}

func (space keySpace) continuationIndexKey(handle string) string {
	// Continuation writes can atomically update the handle index, immutable
	// record, and (for child writes) the admission operation index. Keep all
	// three keys in the configured Redis Cluster hash slot.
	return space.admissionPrefix() + "continuation:index:" + space.digest("continuation-index", handle)
}

func (space keySpace) continuationKey(tenant, handle string) string {
	return space.admissionPrefix() + "continuation:" + space.digest("tenant", tenant) + ":" + space.digest("continuation", handle)
}

func (space keySpace) continuationOperationKey(tenant, parent, operation string) string {
	return space.admissionKey("continuation-operation", tenant, parent, operation)
}

// HashSlotKey is useful to readiness checks and tests: every admission key
// must retain the same literal hash tag so one Function can update all of its
// budget buckets atomically in Redis Cluster.
func (space keySpace) HashSlotKey(kind string) string {
	return space.admissionPrefix() + kind
}
