package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// ProviderStateRetireAfter is how long a configuration digest's provider
// status and inventory hashes survive after the last live worker reported it
// loaded. Workers report every readiness probe interval, so only a digest that
// no running worker has loaded for this long is dropped (#1543).
const ProviderStateRetireAfter = 7 * 24 * time.Hour

// maxRetiredConfigsPerPass bounds one retirement pass; a backlog drains over
// later passes.
const maxRetiredConfigsPerPass = 64

// providerRetireScript drops one retired digest's hashes only if its loaded
// marker is unchanged, so a worker that loads the digest again concurrently
// keeps its state.
var providerRetireScript = redisclient.NewScript(`
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[2], KEYS[3])
redis.call('HDEL', KEYS[1], ARGV[1])
return 1
`)

func (store *ProviderStateStore) loadedConfigsKey() string {
	return store.space.admissionKey("provider-config-loaded")
}

// MarkConfigLoaded records that this worker has digest loaded now. Callers
// repeat it while the configuration stays active.
func (store *ProviderStateStore) MarkConfigLoaded(ctx context.Context, digest [32]byte) error {
	if store == nil {
		return errors.New("provider state store is nil")
	}
	at := strconv.FormatInt(store.clock().UnixMilli(), 10)
	if err := store.client.HSet(ctx, store.loadedConfigsKey(), hex.EncodeToString(digest[:]), at).Err(); err != nil {
		return fmt.Errorf("record loaded provider configuration: %w", err)
	}
	return nil
}

// DropRetiredConfigs deletes the provider status and inventory hashes of
// every configuration digest whose last loaded report is older than
// retireAfter, except current, and returns how many digests it dropped.
// Digests that no worker of this version ever reported are never touched.
func (store *ProviderStateStore) DropRetiredConfigs(ctx context.Context, current [32]byte, retireAfter time.Duration) (int, error) {
	if store == nil {
		return 0, errors.New("provider state store is nil")
	}
	if retireAfter <= 0 {
		return 0, errors.New("provider state retirement horizon must be positive")
	}
	registry := store.loadedConfigsKey()
	loaded, err := store.client.HGetAll(ctx, registry).Result()
	if err != nil {
		return 0, fmt.Errorf("read loaded provider configurations: %w", err)
	}
	cutoff := store.clock().Add(-retireAfter).UnixMilli()
	currentField := hex.EncodeToString(current[:])
	dropped := 0
	for field, value := range loaded {
		if dropped >= maxRetiredConfigsPerPass {
			break
		}
		at, parseErr := strconv.ParseInt(value, 10, 64)
		raw, decodeErr := hex.DecodeString(field)
		if field == currentField || parseErr != nil || decodeErr != nil || len(raw) != 32 || at >= cutoff {
			continue
		}
		var digest [32]byte
		copy(digest[:], raw)
		applied, err := providerRetireScript.Run(ctx, store.client, []string{registry, store.key("status", digest), store.key("inventory", digest)}, field, value).Int()
		if err != nil {
			return dropped, fmt.Errorf("drop retired provider configuration: %w", err)
		}
		dropped += applied
	}
	return dropped, nil
}
