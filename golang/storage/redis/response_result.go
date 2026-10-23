package redis

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	redisclient "github.com/redis/go-redis/v9"
)

const responseResultSchema = "response-result/v1"

// DefaultResponseResultTTL keeps a terminal result long enough for a polling
// client to collect it. After expiry a poll reads the durable record instead.
const DefaultResponseResultTTL = time.Hour

// maxResponseResultBytes bounds one sealed value. A larger result is not
// copied; its polls keep reading the durable record.
const maxResponseResultBytes = 1 << 20

// ResponseResult is a terminal Generate outcome: exactly one of Response or
// Failure is set. Failure is a permanent (non-retryable) execution failure.
type ResponseResult struct {
	CreatedAt time.Time
	Response  *llm.GenerateResponseV1
	Failure   *llm.ExecutionResultV1
}

type ResponseResultOptions struct {
	Client redisclient.UniversalClient
	Keys   KeyOptions
	// TTL is how long a copied result stays readable. Zero selects
	// DefaultResponseResultTTL.
	TTL time.Duration
}

// ResponseResultStore is an expiring Redis copy of terminal Generate results,
// so a Responses API poll for a recently finished request needs neither the
// durable repository nor Temporal. It is never authoritative: a missing,
// expired or unreadable entry sends the poll to the durable record. Values are
// authenticated-encrypted and bound to their HMAC-derived key, so Redis holds
// no readable output and a value cannot be replayed under another scope.
type ResponseResultStore struct {
	client redisclient.UniversalClient
	space  keySpace
	ttl    time.Duration
	aead   cipher.AEAD
}

func NewResponseResultStore(options ResponseResultOptions) (*ResponseResultStore, error) {
	if options.Client == nil {
		return nil, errors.New("Redis response result client is required")
	}
	if options.TTL < 0 {
		return nil, errors.New("Redis response result TTL must not be negative")
	}
	if options.TTL == 0 {
		options.TTL = DefaultResponseResultTTL
	}
	space, err := newKeySpace(options.Keys)
	if err != nil {
		return nil, err
	}
	// Domain-separate value encryption from key-name HMACs.
	key, _ := hex.DecodeString(space.digest("response-result-encryption"))
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &ResponseResultStore{client: options.Client, space: space, ttl: options.TTL, aead: aead}, nil
}

func (store *ResponseResultStore) key(tenant, project, operationKey string) string {
	return store.space.responseResultKey(tenant, project, operationKey)
}

type responseResultWire struct {
	Schema    string                  `json:"schema"`
	CreatedAt time.Time               `json:"created_at"`
	Response  *llm.GenerateResponseV1 `json:"response,omitempty"`
	Failure   *llm.ExecutionResultV1  `json:"failure,omitempty"`
}

func validResponseResult(operationKey string, result ResponseResult) bool {
	if (result.Response == nil) == (result.Failure == nil) {
		return false
	}
	if result.Response != nil {
		return result.Response.OperationKey == operationKey
	}
	return result.Failure.Validate() == nil && result.Failure.State == llm.ExecutionFailed && !result.Failure.Retryable
}

// PutResponseResult copies a terminal result that the durable repository has
// already committed. An oversized result is skipped without error.
func (store *ResponseResultStore) PutResponseResult(ctx context.Context, tenant, project, operationKey string, result ResponseResult) error {
	if !validResponseResult(operationKey, result) {
		return errors.New("Redis response result is not a terminal result for this operation")
	}
	plain, err := json.Marshal(responseResultWire{Schema: responseResultSchema, CreatedAt: result.CreatedAt.UTC(), Response: result.Response, Failure: result.Failure})
	if err != nil {
		return err
	}
	if len(plain) > maxResponseResultBytes {
		return nil
	}
	key := store.key(tenant, project, operationKey)
	sealed, err := store.seal(key, plain)
	if err != nil {
		return err
	}
	return store.client.Set(ctx, key, sealed, store.ttl).Err()
}

// GetResponseResult reports found=false for a missing or expired entry. A
// value that fails authentication or validation is an error, never a result.
func (store *ResponseResultStore) GetResponseResult(ctx context.Context, tenant, project, operationKey string) (ResponseResult, bool, error) {
	key := store.key(tenant, project, operationKey)
	raw, err := store.client.Get(ctx, key).Bytes()
	if errors.Is(err, redisclient.Nil) {
		return ResponseResult{}, false, nil
	}
	if err != nil {
		return ResponseResult{}, false, err
	}
	result, err := store.open(key, operationKey, raw)
	if err != nil {
		return ResponseResult{}, false, err
	}
	return result, true, nil
}

func (store *ResponseResultStore) seal(key string, plain []byte) ([]byte, error) {
	nonce := make([]byte, store.aead.NonceSize(), store.aead.NonceSize()+len(plain)+store.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return store.aead.Seal(nonce, nonce, plain, []byte(key)), nil
}

func (store *ResponseResultStore) open(key, operationKey string, raw []byte) (ResponseResult, error) {
	invalid := errors.New("Redis response result is invalid")
	size := store.aead.NonceSize()
	if len(raw) < size+store.aead.Overhead() || len(raw) > maxResponseResultBytes+size+store.aead.Overhead() {
		return ResponseResult{}, invalid
	}
	plain, err := store.aead.Open(nil, raw[:size], raw[size:], []byte(key))
	if err != nil {
		return ResponseResult{}, invalid
	}
	var wire responseResultWire
	if json.Unmarshal(plain, &wire) != nil || wire.Schema != responseResultSchema {
		return ResponseResult{}, invalid
	}
	result := ResponseResult{CreatedAt: wire.CreatedAt, Response: wire.Response, Failure: wire.Failure}
	if !validResponseResult(operationKey, result) {
		return ResponseResult{}, invalid
	}
	return result, nil
}
