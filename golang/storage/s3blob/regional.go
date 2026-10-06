package s3blob

import (
	"context"
	"fmt"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/regional"
)

// RegionalStore preserves content-addressed references across buckets. Writes
// require one available bucket; cross-region durability relies on replication.
type RegionalStore struct {
	stores []*Store
	router regional.Router
}

func NewRegional(stores []*Store, timeout time.Duration) (*RegionalStore, error) {
	if len(stores) < 2 || len(stores) > 3 || timeout <= 0 {
		return nil, fmt.Errorf("regional S3 storage requires two or three stores and a positive attempt timeout")
	}
	for _, store := range stores {
		if store == nil {
			return nil, fmt.Errorf("regional S3 storage requires two or three stores and a positive attempt timeout")
		}
	}
	return &RegionalStore{stores: stores, router: regional.Router{Count: len(stores), Timeout: timeout}}, nil
}
func (s *RegionalStore) Put(ctx context.Context, request blob.PutRequest) (blob.Ref, error) {
	// This port uses deterministic content-addressed keys, checks existing object
	// metadata, and returns the same reference after an identical retry. Repeating
	// a Put cannot overwrite an object or duplicate a paid provider operation.
	return regional.Run(ctx, &s.router, regional.Unavailable, func(ctx context.Context, i int) (blob.Ref, error) { return s.stores[i].Put(ctx, request) })
}
func (s *RegionalStore) Get(ctx context.Context, tenant string, ref blob.Ref) ([]byte, error) {
	return regional.Run(ctx, &s.router, func(err error) bool { return regional.Unavailable(err) || regional.Missing(err) }, func(ctx context.Context, i int) ([]byte, error) { return s.stores[i].Get(ctx, tenant, ref) })
}
func (s *RegionalStore) ProbeBucket(ctx context.Context) error {
	_, err := regional.Run(ctx, &s.router, regional.Unavailable, func(ctx context.Context, i int) (struct{}, error) { return struct{}{}, s.stores[i].ProbeBucket(ctx) })
	return err
}
