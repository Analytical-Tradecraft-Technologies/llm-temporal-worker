package redis

import (
	"context"
	"errors"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	redisclient "github.com/redis/go-redis/v9"
)

var ErrBudgetAuthorityUnavailable = errors.New("Redis budget authority is unavailable; initialization or recovery is required")

const budgetAuthoritySuffix = "budget:authority"

func (keys BudgetKeySpace) AuthorityKey() string {
	return keys.space.admissionPrefix() + budgetAuthoritySuffix
}

func (keys BudgetKeySpace) InitializationIdentity() budget.InitializationIdentity {
	return budget.InitializationIdentity{Namespace: keys.space.admissionPrefix(), KeyFingerprint: keys.space.digest("budget-initialization-v1")}
}

// RedisBudgetInitializer provisions an unused accounting namespace. It never
// clears a key, changes balances, replaces Functions, or repairs lost authority.
// All workers must be stopped during first initialization, including older
// versions that do not require an authority marker. The preparing marker keeps
// current workers fenced while the bounded namespace census is in progress.
type RedisBudgetInitializer struct {
	client  redisclient.UniversalClient
	keys    BudgetKeySpace
	invoke  FunctionInvoker
	version string
	mode    AdmissionMode
}

func NewRedisBudgetInitializer(client redisclient.UniversalClient, keys KeyOptions, mode AdmissionMode, version string) (*RedisBudgetInitializer, error) {
	if client == nil || (mode != AdmissionModeFunction && mode != AdmissionModeLua) {
		return nil, ErrBudgetAuthorityUnavailable
	}
	space, err := NewBudgetKeySpace(keys)
	if err != nil {
		return nil, err
	}
	if version == "" {
		version = AdmissionFunctionVersion
	}
	return &RedisBudgetInitializer{client: client, keys: space, invoke: redisInvoker{client: client, mode: mode, version: version}, version: version, mode: mode}, nil
}

func (initializer *RedisBudgetInitializer) Identity() budget.InitializationIdentity {
	return initializer.keys.InitializationIdentity()
}

func (initializer *RedisBudgetInitializer) run(ctx context.Context, action string, value budget.Initialization, extra ...string) (string, error) {
	if ctx == nil || value.Validate() != nil || value.Identity != initializer.Identity() {
		return "", ErrBudgetAuthorityUnavailable
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	args := append([]string{action, value.Marker()}, extra...)
	result, err := initializer.invoke.Run(ctx, initializer.version, []string{initializer.keys.AuthorityKey(), initializer.keys.EventsKey()}, args...)
	if err != nil {
		return "", err
	}
	status, _, err := durableFunctionRecord(result)
	if err != nil || (status != "preparing" && status != "ready") {
		return "", ErrBudgetAuthorityUnavailable
	}
	return status, nil
}

// Check is read-only. Readiness cannot initialize, reset, or extend authority.
func (initializer *RedisBudgetInitializer) Check(ctx context.Context, value budget.Initialization) error {
	status, err := initializer.run(ctx, "durable_authority_check", value)
	if err != nil {
		return err
	}
	if status != "ready" {
		return ErrBudgetAuthorityUnavailable
	}
	return nil
}

// Initialize is safe to retry after an unknown outcome. A Ready cloud receipt
// permits only a read, so an absent Redis marker can never grant fresh budgets.
func (initializer *RedisBudgetInitializer) Initialize(ctx context.Context, preparation budget.InitializationPreparation) error {
	value := preparation.Receipt
	if value.Ready {
		return initializer.Check(ctx, value)
	}
	var status string
	var err error
	if preparation.Created {
		// The one-time permission cannot be retransmitted after a lost reply:
		// another initializer may have completed setup and lost Redis meanwhile.
		// Disable both node retries and cluster redirect/transport retry loops.
		client, clientErr := singleAttemptBudgetClient(initializer.client)
		if clientErr != nil {
			return clientErr
		}
		defer client.Close()
		once := *initializer
		once.invoke = redisInvoker{client: client, mode: initializer.mode, version: initializer.version}
		status, err = once.run(ctx, "durable_authority_prepare", value, "create")
	} else {
		status, err = initializer.run(ctx, "durable_authority_prepare", value, "resume")
	}
	if err != nil || status == "ready" {
		return err
	}
	// SCAN is deliberately outside Lua so a large shared database cannot block
	// Redis's event loop. The caller supplies a deadline. Scan every primary in
	// cluster mode; a node-local SCAN would not prove the namespace is unused.
	scan := func(ctx context.Context, client *redisclient.Client) error {
		return scanBudgetNamespace(ctx, client, initializer.keys)
	}
	if cluster, ok := initializer.client.(*redisclient.ClusterClient); ok {
		err = cluster.ForEachMaster(ctx, scan)
	} else {
		err = scanBudgetNamespace(ctx, initializer.client, initializer.keys)
	}
	if err != nil {
		return err
	}
	event, err := (BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventInitialize, GenerationID: "redis-budget-v1", OccurredAt: value.CreatedAt}).Marshal()
	if err != nil {
		return err
	}
	status, err = initializer.run(ctx, "durable_authority_commit", value, string(event))
	if err != nil {
		return err
	}
	if status != "ready" {
		return ErrBudgetAuthorityUnavailable
	}
	return nil
}

func singleAttemptBudgetClient(client redisclient.UniversalClient) (redisclient.UniversalClient, error) {
	switch source := client.(type) {
	case *redisclient.Client:
		options := *source.Options()
		options.MaxRetries = -1
		return redisclient.NewClient(&options), nil
	case *redisclient.ClusterClient:
		options := *source.Options()
		options.MaxRetries, options.MaxRedirects = -1, -1
		return redisclient.NewClusterClient(&options), nil
	default:
		return nil, ErrBudgetAuthorityUnavailable
	}
}

type budgetNamespaceScanner interface {
	Scan(context.Context, uint64, string, int64) *redisclient.ScanCmd
}

func scanBudgetNamespace(ctx context.Context, client budgetNamespaceScanner, keys BudgetKeySpace) error {
	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Match locally rather than interpolating a hash tag into a Redis glob.
		page, next, err := client.Scan(ctx, cursor, "*", 256).Result()
		if err != nil {
			return err
		}
		for _, key := range page {
			if strings.HasPrefix(key, keys.space.admissionPrefix()) && key != keys.AuthorityKey() {
				return ErrBudgetAuthorityUnavailable
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}
