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
)

// operationLookup is the read-only part of the cloud request repository the
// Responses API needs. It never creates or repairs a record.
type operationLookup interface {
	LookupOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error)
}

// snapshotResponseResults reads Generate results from the cloud request
// repository of the current snapshot, so a reload never leaves polls on a
// closed storage client.
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
	lookup := snapshotOperationLookup(lease.Snapshot().Clients)
	if lookup == nil {
		return responsesapi.StoredResult{}, responsesapi.ErrResultStoreUnavailable
	}
	return lookupGenerateResult(ctx, lookup, tenant, project, operationKey)
}

func snapshotOperationLookup(set app.ClientSet) operationLookup {
	clients, ok := set.(*snapshotClients)
	if !ok || clients == nil {
		return nil
	}
	source, ok := clients.clients.(V1RuntimeCapabilitiesSource)
	if !ok || isNilCapability(source) {
		return nil
	}
	lookup, ok := source.V1RuntimeCapabilities().Requests.(operationLookup)
	if !ok || isNilCapability(lookup) {
		return nil
	}
	return lookup
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
