package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// maxModelCatalogBytes bounds one stored model catalog body. It matches the
// model-sync Document limit.
const maxModelCatalogBytes = 8 << 20

// modelCatalogPublishScript replaces the catalog hash only with a strictly
// later fetch, so two workers that fetched concurrently cannot regress it.
// KEYS[1] catalog hash; ARGV digest, fetched_at (unix ms), body.
var modelCatalogPublishScript = redisclient.NewScript(`
local current = redis.call('HGET', KEYS[1], 'fetched_at')
if current and tonumber(current) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('HSET', KEYS[1], 'digest', ARGV[1], 'fetched_at', ARGV[2], 'body', ARGV[3])
return 1
`)

// modelCatalogReleaseScript deletes the refresh lease only for its owner.
var modelCatalogReleaseScript = redisclient.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// ModelCatalogStore shares the model-sync catalog between workers. The
// catalog hash has no TTL: it is replaced, never expired, so a long OpenRouter
// outage keeps the last good catalog. The refresh lease expires on its own.
type ModelCatalogStore struct {
	client redisclient.UniversalClient
	space  keySpace
}

func NewModelCatalogStore(client redisclient.UniversalClient, keys KeyOptions) (*ModelCatalogStore, error) {
	if client == nil {
		return nil, errors.New("Redis model catalog client is required")
	}
	space, err := newKeySpace(keys)
	if err != nil {
		return nil, err
	}
	return &ModelCatalogStore{client: client, space: space}, nil
}

func (store *ModelCatalogStore) catalogKey() string {
	return store.space.admissionKey("model-catalog", "openrouter")
}

func (store *ModelCatalogStore) leaseKey() string {
	return store.space.admissionKey("model-catalog-lease", "openrouter")
}

func (store *ModelCatalogStore) LatestDigest(ctx context.Context) (string, error) {
	digest, err := store.client.HGet(ctx, store.catalogKey(), "digest").Result()
	if errors.Is(err, redisclient.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: read model catalog digest: %v", ErrUnavailable, err)
	}
	return digest, nil
}

func (store *ModelCatalogStore) Latest(ctx context.Context) (string, time.Time, []byte, bool, error) {
	values, err := store.client.HMGet(ctx, store.catalogKey(), "digest", "fetched_at", "body").Result()
	if err != nil {
		return "", time.Time{}, nil, false, fmt.Errorf("%w: read model catalog: %v", ErrUnavailable, err)
	}
	if len(values) != 3 || values[0] == nil {
		return "", time.Time{}, nil, false, nil
	}
	digest, digestOK := values[0].(string)
	fetched, fetchedOK := values[1].(string)
	body, bodyOK := values[2].(string)
	if !digestOK || !fetchedOK || !bodyOK || len(body) > maxModelCatalogBytes {
		return "", time.Time{}, nil, false, fmt.Errorf("%w: model catalog record is corrupt", ErrUnavailable)
	}
	millis, err := strconv.ParseInt(fetched, 10, 64)
	if err != nil {
		return "", time.Time{}, nil, false, fmt.Errorf("%w: model catalog fetched_at is corrupt", ErrUnavailable)
	}
	return digest, time.UnixMilli(millis).UTC(), []byte(body), true, nil
}

func (store *ModelCatalogStore) Publish(ctx context.Context, digest string, fetchedAt time.Time, body []byte) (bool, error) {
	if digest == "" || fetchedAt.IsZero() || len(body) == 0 || len(body) > maxModelCatalogBytes {
		return false, fmt.Errorf("model catalog record is invalid")
	}
	result, err := modelCatalogPublishScript.Run(ctx, store.client, []string{store.catalogKey()}, digest, strconv.FormatInt(fetchedAt.UnixMilli(), 10), string(body)).Int()
	if err != nil {
		return false, fmt.Errorf("%w: publish model catalog: %v", ErrUnavailable, err)
	}
	return result == 1, nil
}

func (store *ModelCatalogStore) AcquireRefresh(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	if token == "" || ttl <= 0 {
		return false, fmt.Errorf("model catalog lease token and ttl are required")
	}
	acquired, err := store.client.SetNX(ctx, store.leaseKey(), token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("%w: acquire model catalog lease: %v", ErrUnavailable, err)
	}
	return acquired, nil
}

func (store *ModelCatalogStore) ReleaseRefresh(ctx context.Context, token string) error {
	if err := modelCatalogReleaseScript.Run(ctx, store.client, []string{store.leaseKey()}, token).Err(); err != nil && !errors.Is(err, redisclient.Nil) {
		return fmt.Errorf("%w: release model catalog lease: %v", ErrUnavailable, err)
	}
	return nil
}
