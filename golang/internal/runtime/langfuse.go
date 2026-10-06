package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/langfuse"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"log/slog"
	"sort"
	"strings"
	"time"
)

type langfuseRepository interface {
	SaveLangfuseCapture(context.Context, cloudstate.Scope, cloudstate.RequestID, string, json.RawMessage) error
	LoadLangfuseCaptures(context.Context, cloudstate.Scope, cloudstate.RequestID) (map[string]json.RawMessage, error)
	ClaimLangfuseExport(context.Context, cloudstate.Scope, cloudstate.RequestID, time.Time) (string, error)
	AcknowledgeLangfuseExport(context.Context, cloudstate.Scope, cloudstate.RequestID, string) error
	ReleaseLangfuseExport(context.Context, cloudstate.Scope, cloudstate.RequestID, string) error
	LangfuseID(cloudstate.Scope, string, string) string
	LangfusePaidAttempts(context.Context, cloudstate.Scope, cloudstate.RequestID) (map[cloudstate.RequestID]cloudstate.SavedProviderExecution, cloudstate.RequestID, error)
}
type operationCapture struct {
	CheckpointScope string             `json:"checkpoint_scope"`
	Context         llm.RequestContext `json:"context"`
	Input           json.RawMessage    `json:"input"`
	Session         string             `json:"session"`
	Metadata        map[string]any     `json:"metadata"`
}
type generationCapture struct {
	Disabled     bool                  `json:"disabled,omitempty"`
	Input        json.RawMessage       `json:"input"`
	Provider     string                `json:"provider"`
	LogicalModel string                `json:"logical_model"`
	Plan         cloudstate.BudgetPlan `json:"plan"`
}

// Content is semantic, never SDK wire parameters (which may hold credentials).
// Opaque provider state is excluded at both item and part levels.
func langfuseContent(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var tree any
	if json.Unmarshal(data, &tree) != nil {
		return nil
	}
	if root, ok := tree.(map[string]any); ok {
		for _, k := range []string{"continuation", "provider_states", "operation_key", "extensions", "diagnostics"} {
			delete(root, k)
		}
	}

	// Only interpret kind markers at semantic item/part boundaries. User tool
	// arguments/results can legitimately contain the same key names.
	var scrubItems func(any) any
	scrubItems = func(v any) any {
		items, ok := v.([]any)
		if !ok {
			return v
		}
		out := []any{}
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				if m["kind"] == "provider_state" {
					continue
				}
				if content, ok := m["content"].([]any); ok {
					m["content"] = scrubItems(content)
				}
			}
			out = append(out, item)
		}
		return out
	}
	if root, ok := tree.(map[string]any); ok {
		for _, key := range []string{"instructions", "input", "output"} {
			if v, found := root[key]; found {
				root[key] = scrubItems(v)
			}
		}
	} else {
		tree = scrubItems(tree)
	}

	data, _ = json.Marshal(tree)
	return data
}
func (r *CloudExecutionRuntime) captureLangfuseOperation(ctx context.Context, p PreparedCloudRequest) {
	if r.capabilities.Langfuse == nil || p.Preparation.Version == 0 {
		return
	}
	store, ok := r.store.(langfuseRepository)
	if !ok {
		return
	}
	var request *llm.Request
	var lineage []state.Handle
	var caller llm.RequestContext
	if p.Generate != nil {
		input, err := PrepareGenerateInput(ctx, *p.Generate, p.GenerateReplay)
		if err != nil {
			return
		}
		request = &input.Request
		lineage = p.GenerateReplay.State.Lineage
		caller = p.Generate.Context
	} else {
		input, err := PrepareCompactInput(ctx, *p.Compact, p.CompactReplay)
		if err != nil {
			return
		}
		request = input.Request
		lineage = p.CompactReplay.State.Lineage
		caller = p.Compact.Context
	}
	scope := p.Record.Request.Scope
	root := string(p.Record.Request.ID)
	metadata := map[string]any{}
	key := ""
	if p.Generate != nil {
		key = p.Generate.OperationKey
	} else {
		key = p.Compact.OperationKey
	}
	metadata["operation_key_hash"] = store.LangfuseID(scope, "operation-key", key)
	if p.Compact != nil {
		metadata["automatic_compaction"] = strings.HasPrefix(key, "llmtw_compact_")
	}

	if len(lineage) > 0 {
		// These are durable checkpoint IDs from an already authorized snapshot,
		// not the signed handles accepted at the public API boundary.
		root = cloudstate.RequestIDPrefix + string(lineage[0])
		parent := string(lineage[len(lineage)-1])
		metadata["parent_trace_id"] = store.LangfuseID(scope, "trace", cloudstate.RequestIDPrefix+parent)[:32]
		metadata["source_checkpoint_id"] = store.LangfuseID(scope, "checkpoint", parent)
	}
	input := langfuseContent(request)
	if request == nil {
		input = langfuseContent(map[string]any{"policy": p.Compact.Policy, "no_work": true})
		metadata["no_work"] = true
	}
	value := operationCapture{CheckpointScope: p.Preparation.CheckpointScope, Context: caller, Input: input, Session: store.LangfuseID(scope, "session", root), Metadata: metadata}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := store.SaveLangfuseCapture(bounded, scope, p.Record.Request.ID, "operation", data); err != nil {
		slog.WarnContext(ctx, "Langfuse operation capture failed", "request_id", p.Record.Request.ID)
	}
}
func (e *CloudProviderExecution) captureLangfuse(ctx context.Context, call *CloudBudgetCall, saved cloudstate.SavedProviderExecution) {
	if e.capabilities.Langfuse == nil {
		return
	}
	endpoint, found := e.capabilities.LangfuseEndpoints[saved.Plan.Route.EndpointID]
	if !found {
		return
	}
	store, ok := e.store.(langfuseRepository)
	if !ok {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	record, err := e.store.Read(bounded, call.scope, call.id)
	if err != nil {
		return
	}
	var progress map[string]json.RawMessage
	var attempt cloudstate.RequestAttempt
	if json.Unmarshal(record.Progress, &progress) != nil || json.Unmarshal(progress["attempt_parent"], &attempt) != nil || attempt.Validate() != nil {
		return
	}
	providerName := saved.Plan.Route.Provider
	if _, router := endpoint.Extensions["openrouter"]; router {
		providerName = "openrouter"
	}
	value := generationCapture{Provider: providerName, LogicalModel: call.provider.LogicalModel, Plan: saved.Plan, Disabled: !endpoint.LangfuseEnabled()}
	if !value.Disabled {
		value.Input = langfuseContent(call.provider.Semantic)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	if err := store.SaveLangfuseCapture(bounded, call.scope, attempt.RootID, string(call.id), data); err != nil {
		slog.WarnContext(ctx, "Langfuse generation capture failed", "request_id", attempt.RootID)
	}
}
func (r *CloudExecutionRuntime) ExportLangfuseV1(ctx context.Context, ref llm.ExecutionReferenceV1) error {
	if r.capabilities.Langfuse == nil {
		return nil
	}
	p, err := r.preparation.Load(ctx, ref)
	if err != nil {
		return langfuse.ErrExport
	}
	store, ok := r.store.(langfuseRepository)
	if !ok {
		return nil
	}
	scope, id := p.Record.Request.Scope, p.Record.Request.ID
	if p.Record.Status != cloudstate.StatusCompleted && p.Record.Status != cloudstate.StatusFailed {
		return nil
	}
	captures, err := store.LoadLangfuseCaptures(ctx, scope, id)
	if err != nil {
		return langfuse.ErrExport
	}
	var captured operationCapture
	if json.Unmarshal(captures["operation"], &captured) != nil {
		return langfuse.ErrExport
	}
	o := langfuse.Operation{TraceID: store.LangfuseID(scope, "trace", string(id))[:32], SpanID: store.LangfuseID(scope, "span", string(id))[:16], SessionID: captured.Session, Kind: p.Record.Request.Kind, StartedAt: p.Record.Request.CreatedAt, EndedAt: p.Record.UpdatedAt, Context: captured.Context, Input: captured.Input, Metadata: captured.Metadata}
	if o.Metadata == nil {
		o.Metadata = map[string]any{}
	}
	eligible := captured.Metadata["no_work"] == true
	// Detect lost eligible paid captures before acknowledging a partial export.
	paid, lastAttempt, err := store.LangfusePaidAttempts(ctx, scope, id)
	if err != nil {
		return langfuse.ErrExport
	}
	for attemptID, attempt := range paid {
		endpoint, found := r.capabilities.LangfuseEndpoints[attempt.Plan.Route.EndpointID]
		if found && endpoint.LangfuseEnabled() && captures[string(attemptID)] == nil {
			return langfuse.ErrExport
		}
	}
	eligibleCompletion := false
	eligibleCache := false
	keys := make([]string, 0, len(captures))
	for key := range captures {
		if key != "operation" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		var capture generationCapture
		if json.Unmarshal(captures[key], &capture) != nil {
			return langfuse.ErrExport
		}
		if capture.Disabled {
			continue
		}
		saved, err := r.store.LoadProviderExecution(ctx, scope, cloudstate.RequestID(key))
		unused := errors.Is(err, cloudstate.ErrProviderExecutionMissing) || errors.Is(err, cloudstate.ErrBudgetPlanMissing)
		if unused {
			saved.Plan = capture.Plan
			err = nil
		}
		if err != nil {
			return langfuse.ErrExport
		}
		endpoint, found := r.capabilities.LangfuseEndpoints[saved.Plan.Route.EndpointID]
		if !found || !endpoint.LangfuseEnabled() {
			continue
		}
		eligible = true
		if unused {
			if cloudstate.RequestID(key) == lastAttempt {
				eligibleCache = true
			}
			o.Metadata["provider"] = capture.Provider
			o.Metadata["endpoint_id"] = saved.Plan.Route.EndpointID
			continue
		}
		x := saved.Execution
		if x.Stage == cloudstate.ExecutionSucceeded && cloudstate.RequestID(key) == lastAttempt {
			eligibleCompletion = true
		}
		plan := saved.Plan
		end := x.CompletedAt
		if end.IsZero() {
			end = x.UpdatedAt
		}
		metadata := map[string]any{"route_id": plan.Route.RouteID, "requested_model": capture.LogicalModel, "region": endpoint.Region, "requested_service_class": plan.RequestedClass, "attempted_service_class": plan.AttemptedClass, "pricing": plan.Quote.Entry, "catalog_version": plan.Quote.CatalogVersion, "outcome": x.Stage}

		g := langfuse.Generation{ID: store.LangfuseID(scope, "generation", key)[:16], Provider: capture.Provider, Endpoint: plan.Route.EndpointID, Family: plan.Family, Model: plan.Route.Model, StartedAt: x.StartedAt, EndedAt: end, Input: capture.Input, Metadata: metadata}
		if x.Failure != nil {
			metadata["failure_code"] = x.Failure.Code
			metadata["dispatch"] = x.Failure.Dispatch
		}
		if x.Response != nil {
			v := x.Response
			g.Output = langfuseContent(map[string]any{"output": v.Output, "status": v.Status})
			g.Usage = map[string]any{"input": v.Usage.InputTokens, "output": v.Usage.OutputTokens, "reasoning": v.Usage.ReasoningTokens, "cache_read": v.Usage.CacheReadTokens, "cache_write": v.Usage.CacheWriteTokens, "total": v.Usage.InputTokens + v.Usage.OutputTokens}
			metadata["usage"] = map[string]any{"input_tokens": v.Usage.InputTokens, "output_tokens": v.Usage.OutputTokens, "reasoning_tokens": v.Usage.ReasoningTokens, "cache_read_tokens": v.Usage.CacheReadTokens, "cache_write_tokens": v.Usage.CacheWriteTokens}
			metadata["cost"] = v.Cost
			metadata["service"] = v.Service
			if v.Service.Actual != nil && *v.Service.Actual != plan.AttemptedClass {
				metadata["attempted_pricing"] = plan.Quote.Entry
				if entry, ok := plan.ClassEntries[*v.Service.Actual]; ok {
					metadata["pricing"] = entry
				} else {
					delete(metadata, "pricing")
				}
			}
			if v.Cost.ActualCostUSD != nil {
				g.Cost = map[string]any{"total": json.Number(v.Cost.ActualCostUSD.String())}
			}
			o.Output = g.Output
		}
		o.Generations = append(o.Generations, g)
	}
	if p.Record.Status == cloudstate.StatusCompleted {
		result, found, replayErr := r.replay(ctx, p)
		if found && replayErr == nil {
			if result.Generate != nil && (eligibleCompletion || (result.Generate.Cache.Disposition == "hit" && eligibleCache)) {
				o.Output = langfuseContent(result.Generate.Output)
				o.Metadata["cache"] = result.Generate.Cache
				o.Metadata["cost"] = result.Generate.Cost
				o.Metadata["result_checkpoint_id"] = store.LangfuseID(scope, "checkpoint", strings.TrimPrefix(string(id), cloudstate.RequestIDPrefix))
			}
			if result.Compact != nil {
				if result.Compact.Cache.Disposition == "hit" && eligibleCache {
					cpID := state.CheckpointID(strings.TrimPrefix(string(id), cloudstate.RequestIDPrefix))
					cp, loadErr := r.capabilities.Checkpoints.Repository.Get(ctx, captured.CheckpointScope, cpID)
					if loadErr != nil || cp.ScopeID != captured.CheckpointScope || cp.ID != cpID || cp.Kind != state.CheckpointCompaction || string(cp.OriginOperationID) != string(id) {
						return langfuse.ErrExport
					}
					data, loadErr := r.capabilities.Checkpoints.Blobs.Read(ctx, captured.CheckpointScope, cp.ResponseBlob)
					if loadErr != nil || sha256.Sum256(data) != cp.ResponseBlob.Digest || int64(len(data)) != cp.ResponseBlob.ByteLength {
						return langfuse.ErrExport
					}
					output, loadErr := r.publication.codec.DecodeResponse(data)
					if loadErr != nil {
						return langfuse.ErrExport
					}
					o.Output = langfuseContent(output)
				}
				o.Metadata["cache"] = result.Compact.Cache
				if eligibleCompletion || captured.Metadata["no_work"] == true || (result.Compact.Cache.Disposition == "hit" && eligibleCache) {
					o.Metadata["cost"] = result.Compact.Cost
				}
				o.Metadata["result_checkpoint_id"] = store.LangfuseID(scope, "checkpoint", strings.TrimPrefix(string(id), cloudstate.RequestIDPrefix))
				o.Metadata["compaction_provenance"] = result.Compact.Provenance
			}
		}
	}
	// A disabled provider must not leak its root input through an empty trace.
	if !eligible {
		return nil
	}
	owner, err := store.ClaimLangfuseExport(ctx, scope, id, time.Now())
	if err != nil {
		return langfuse.ErrExport
	}
	if owner == "" {
		return nil
	}
	if err := r.capabilities.Langfuse.Export(ctx, o); err != nil {
		_ = store.ReleaseLangfuseExport(ctx, scope, id, owner)
		return err
	}
	if err := store.AcknowledgeLangfuseExport(ctx, scope, id, owner); err != nil {
		return langfuse.ErrExport
	}
	return nil
}
