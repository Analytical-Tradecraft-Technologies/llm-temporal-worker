package runtime

import (
	"context"
	"encoding/json"
	"errors"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/app"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/responsesapi"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

// operationLookup is the read-only part of the cloud request repository the
// Responses API needs. It never creates or repairs a record.
type operationLookup interface {
	LookupOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error)
}

// ResponseResultIndex is the expiring Redis copy of terminal Generate results.
// The cloud execution runtime fills it after the durable write, and Responses
// API polls read it before the durable record. It is never authoritative.
type ResponseResultIndex interface {
	PutResponseResult(ctx context.Context, tenant, project, operationKey string, result redisstore.ResponseResult) error
	GetResponseResult(ctx context.Context, tenant, project, operationKey string) (redisstore.ResponseResult, bool, error)
}

// snapshotResponseResults reads Generate results from the Redis result index
// and the cloud request repository of the current snapshot, so a reload never
// leaves polls on a closed storage client.
type snapshotResponseResults struct {
	application *app.App
}

var _ responsesapi.ResultStore = (*snapshotResponseResults)(nil)

func (results *snapshotResponseResults) LookupGenerate(ctx context.Context, tenant, project, operationKey string) (responsesapi.StoredResult, error) {
	if results == nil || results.application == nil {
		return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
	}
	lease, err := results.application.Acquire()
	if err != nil {
		return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
	}
	defer lease.Release()
	index, lookup := snapshotResultSources(lease.Snapshot().Clients)
	return lookupIndexedGenerateResult(ctx, index, lookup, tenant, project, operationKey)
}

func snapshotResultSources(set app.ClientSet) (ResponseResultIndex, operationLookup) {
	clients, ok := set.(*snapshotClients)
	if !ok || clients == nil {
		return nil, nil
	}
	source, ok := clients.clients.(V1RuntimeCapabilitiesSource)
	if !ok || isNilCapability(source) {
		return nil, nil
	}
	capabilities := source.V1RuntimeCapabilities()
	var index ResponseResultIndex
	if !isNilCapability(capabilities.ResponseResults) {
		index = capabilities.ResponseResults
	}
	lookup, ok := capabilities.Requests.(operationLookup)
	if !ok || isNilCapability(lookup) {
		return index, nil
	}
	return index, lookup
}

// lookupIndexedGenerateResult answers a recently finished operation from
// Redis alone. A miss or an unreadable entry reads the durable record, and a
// terminal durable answer is copied back so later polls skip durable storage.
// Non-terminal states are never copied: a running record may belong to a
// workflow that ends without writing one, which only Temporal can report.
func lookupIndexedGenerateResult(ctx context.Context, index ResponseResultIndex, lookup operationLookup, tenant, project, operationKey string) (responsesapi.StoredResult, error) {
	if index != nil {
		if cached, found, err := index.GetResponseResult(ctx, tenant, project, operationKey); err == nil && found {
			return storedFromIndex(cached), nil
		}
	}
	if lookup == nil {
		return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
	}
	result, err := lookupGenerateResult(ctx, lookup, tenant, project, operationKey)
	if err != nil || index == nil {
		return result, err
	}
	if cached, ok := indexFromStored(result); ok {
		// A failed copy only costs the next poll another durable read.
		_ = index.PutResponseResult(ctx, tenant, project, operationKey, cached)
	}
	return result, nil
}

func storedFromIndex(cached redisstore.ResponseResult) responsesapi.StoredResult {
	if cached.Response != nil {
		return responsesapi.StoredResult{State: responsesapi.StoredCompleted, CreatedAt: cached.CreatedAt, Response: *cached.Response}
	}
	return responsesapi.StoredResult{State: responsesapi.StoredFailed, CreatedAt: cached.CreatedAt, Failure: *cached.Failure}
}

func indexFromStored(result responsesapi.StoredResult) (redisstore.ResponseResult, bool) {
	switch result.State {
	case responsesapi.StoredCompleted:
		return redisstore.ResponseResult{CreatedAt: result.CreatedAt, Response: &result.Response}, true
	case responsesapi.StoredFailed:
		return redisstore.ResponseResult{CreatedAt: result.CreatedAt, Failure: &result.Failure}, true
	}
	return redisstore.ResponseResult{}, false
}

// lookupGenerateResult decodes the progress the cloud execution runtime
// writes: the response on completion and the execution failure on a failed
// operation. Other states are reported as running.
func lookupGenerateResult(ctx context.Context, lookup operationLookup, tenant, project, operationKey string) (responsesapi.StoredResult, error) {
	record, err := lookup.LookupOperation(ctx, cloudstate.Operation{
		Scope: cloudstate.Scope{Tenant: tenant, Project: project}, Kind: "generate", Key: operationKey,
	})
	if errors.Is(err, contracts.ErrNotFound) && !errors.Is(err, contracts.ErrOutcomeUnknown) {
		return responsesapi.StoredResult{}, responsesapi.ErrResultNotFound
	}
	if err != nil {
		return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
	}
	result := responsesapi.StoredResult{State: responsesapi.StoredRunning, CreatedAt: record.Request.CreatedAt}
	switch record.Status {
	case cloudstate.StatusCompleted:
		var progress cloudRuntimeResult
		if json.Unmarshal(record.Progress, &progress) != nil || progress.Version != 1 {
			return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
		}
		if json.Unmarshal(progress.Response, &result.Response) != nil || result.Response.OperationKey != operationKey {
			return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
		}
		result.State = responsesapi.StoredCompleted
	case cloudstate.StatusFailed:
		var progress struct {
			Failure llm.ExecutionResultV1 `json:"execution_failure"`
		}
		if json.Unmarshal(record.Progress, &progress) != nil || progress.Failure.State != llm.ExecutionFailed {
			return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
		}
		result.State, result.Failure = responsesapi.StoredFailed, progress.Failure
	}
	return result, nil
}
