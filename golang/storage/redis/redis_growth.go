package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// BudgetGrowth is a cheap O(1) sample of the Redis state that grows with
// traffic: durable operation records (reuse tombstones) and the coordination
// Stream. It is exported as llmtw_budget_operation_records and
// llmtw_budget_stream_length (#1543).
type BudgetGrowth struct {
	// OperationRecords counts indexed durable operation records whose expiry
	// has not passed, including persistent ones.
	OperationRecords int64
	// PersistentOperationRecords counts the records with no expiry: those
	// still claimed or ambiguous, and every record when no
	// operation_key_reuse_horizon is configured.
	PersistentOperationRecords int64
	// StreamLength is XLEN of the budget coordination Stream (0 when absent).
	StreamLength int64
}

// BudgetGrowthClient is the Redis surface BudgetGrowthSampler reads.
type BudgetGrowthClient interface {
	ZCount(context.Context, string, string, string) *redisclient.IntCmd
	XLen(context.Context, string) *redisclient.IntCmd
}

// BudgetGrowthSampler reads BudgetGrowth for one namespace.
type BudgetGrowthSampler struct {
	client BudgetGrowthClient
	space  keySpace
	clock  func() time.Time
}

func NewBudgetGrowthSampler(client BudgetGrowthClient, keys KeyOptions, clock func() time.Time) (*BudgetGrowthSampler, error) {
	if client == nil {
		return nil, errors.New("Redis budget growth sampler client is required")
	}
	space, err := newKeySpace(keys)
	if err != nil {
		return nil, err
	}
	if clock == nil {
		clock = time.Now
	}
	return &BudgetGrowthSampler{client: client, space: space, clock: clock}, nil
}

// Sample counts index entries scored after now, so entries whose key has
// expired but which no later mutation has pruned yet are not counted.
func (sampler *BudgetGrowthSampler) Sample(ctx context.Context) (BudgetGrowth, error) {
	if sampler == nil {
		return BudgetGrowth{}, errors.New("Redis budget growth sampler is nil")
	}
	index := sampler.space.admissionPrefix() + BudgetOperationsSuffix
	live, err := sampler.client.ZCount(ctx, index, fmt.Sprintf("(%d", sampler.clock().UnixMilli()), "+inf").Result()
	if err != nil {
		return BudgetGrowth{}, fmt.Errorf("count Redis operation records: %w", err)
	}
	persistent, err := sampler.client.ZCount(ctx, index, "+inf", "+inf").Result()
	if err != nil {
		return BudgetGrowth{}, fmt.Errorf("count persistent Redis operation records: %w", err)
	}
	length, err := sampler.client.XLen(ctx, sampler.space.admissionPrefix()+BudgetEventsSuffix).Result()
	if err != nil && !errors.Is(err, redisclient.Nil) {
		return BudgetGrowth{}, fmt.Errorf("read Redis budget Stream length: %w", err)
	}
	return BudgetGrowth{OperationRecords: live, PersistentOperationRecords: persistent, StreamLength: length}, nil
}
