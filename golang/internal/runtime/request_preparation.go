package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// PreparedGenerateInput is the shared semantic input for cache planning,
// routing, estimation and provider compilation. Settings retains the exact v1
// decimal for checkpoint publication; Request is the provider-facing projection.
// SampleIndex must remain part of the cache identity, outside llm.Request.
type PreparedGenerateInput struct {
	Request     llm.Request
	Settings    state.ModelState
	SampleIndex int64
}

// PrepareGenerateInput requires an already authorized, materialized replay.
// It performs no I/O or admission work and never interprets a checkpoint handle
// as a provider continuation ID. Completed/reconciling operations bypass it.
func PrepareGenerateInput(ctx context.Context, request llm.GenerateRequestV1, replay durable.GenerateReplay) (PreparedGenerateInput, error) {
	if err := validatePreparationContext(ctx, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return PreparedGenerateInput{}, err
	}
	if _, err := request.MarshalJSON(); err != nil {
		return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
	}
	parent := ""
	if request.Parent != nil {
		parent = string(*request.Parent)
	}
	if err := validatePreparationReplay(request.Context, parent, replay.State); err != nil {
		return PreparedGenerateInput{}, err
	}
	base := replay.State.Settings
	if request.Parent == nil {
		base = state.RootModelState("")
	}
	settings, err := state.ApplySettingsPatchV1(base, request.SettingsPatch)
	if err != nil {
		return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
	}
	// Reject a policy that later compaction planning could not decode before
	// any provider work, so one bad request cannot publish a checkpoint that
	// makes every descendant fail as state_corrupt.
	if request.SettingsPatch.CompactionPolicy.Set != nil {
		if _, err := decodeCompactionPolicy(settings.CompactionPolicy); err != nil {
			return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
		}
	}
	items := append([]llm.Item(nil), replay.State.Items...)
	items = append(items, request.Append...)
	pending, err := state.ValidateTranscript(items)
	if err != nil || len(pending) != 0 {
		return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
	}
	semantic, err := prepareSemanticRequest(request.Context, request.OperationKey, settings, items)
	if err != nil {
		return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
	}
	// The provider fetches every media URL in this request, including ones
	// replayed from the parent. Stored content decodes without the URL policy,
	// so a checkpoint written under an earlier policy is checked here and
	// fails deterministically instead of dispatching a now-blocked URL.
	if err := llm.ValidateMediaURLs(semantic.Instructions, semantic.Input); err != nil {
		return PreparedGenerateInput{}, preparationError(provider.CodeInvalidArgument)
	}
	if err := llm.RejectBlobMedia(semantic.Instructions, semantic.Input); err != nil {
		return PreparedGenerateInput{}, preparationError(provider.CodeUnsupportedCapability)
	}
	if err := ctx.Err(); err != nil {
		return PreparedGenerateInput{}, err
	}
	result := PreparedGenerateInput{Request: semantic, Settings: settings}
	if request.Cache != nil {
		result.SampleIndex = int64(request.Cache.Variant)
	}
	return result, nil
}

// PreparedCompactInput describes the bounded summarizer and the verbatim
// suffix needed for a later compaction checkpoint. A nil Request means there
// is no safe prefix to compact: callers must not reserve budget or dispatch.
// Settings remains the application's configuration, not the stripped summary
// configuration. Policy versions participate in the compaction cache identity.
type PreparedCompactInput struct {
	Request   *llm.Request
	Settings  state.ModelState
	Policy    compaction.Policy
	Selection compaction.PrefixSelection
}

// PrepareCompactInput applies the inherited policy and the compact request's
// target/style overrides, selects whole tool exchanges within the target, and
// uses the existing versioned plain-text summarizer contract. It grants no
// provider authority.
func PrepareCompactInput(ctx context.Context, request llm.CompactRequestV1, replay durable.CompactReplay) (PreparedCompactInput, error) {
	if err := validatePreparationContext(ctx, replay.Completed != nil || replay.ReconciliationPending != nil); err != nil {
		return PreparedCompactInput{}, err
	}
	if _, err := request.MarshalJSON(); err != nil {
		return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
	}
	if err := validatePreparationReplay(request.Context, string(request.Parent), replay.State); err != nil {
		return PreparedCompactInput{}, err
	}
	policy, err := decodeCompactionPolicy(replay.State.Settings.CompactionPolicy)
	if err != nil {
		return PreparedCompactInput{}, preparationError(provider.CodeStateCorrupt)
	}
	if len(request.Policy) != 0 {
		var override struct {
			TargetTokens *int                     `json:"target_tokens"`
			SummaryStyle *compaction.SummaryStyle `json:"summary_style"`
		}
		if err := json.Unmarshal(request.Policy, &override); err != nil {
			return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
		}
		if override.TargetTokens != nil {
			policy.TargetTokens = *override.TargetTokens
		}
		if override.SummaryStyle != nil {
			policy.SummaryStyle = *override.SummaryStyle
		}
		if err := policy.Validate(); err != nil {
			return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
		}
	}
	settings := replay.State.Settings.Clone()
	source, err := prepareSemanticRequest(request.Context, request.OperationKey, settings, replay.State.Items)
	if err != nil {
		return PreparedCompactInput{}, preparationError(provider.CodeStateCorrupt)
	}
	// The summarizer is a provider call over the stored transcript, so the
	// same replayed-URL check applies as for Generate.
	if err := llm.ValidateMediaURLs(source.Instructions, source.Input); err != nil {
		return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
	}
	var selection compaction.PrefixSelection
	if len(source.Input) != 0 {
		selection, err = compaction.SelectRequestPrefix(source, policy)
		if err != nil {
			return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
		}
	}
	result := PreparedCompactInput{Settings: settings, Policy: policy, Selection: selection}
	if len(selection.Prefix) != 0 {
		summary, err := compaction.PrepareRequest(source, request.OperationKey, selection.Prefix, policy)
		if err != nil {
			return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
		}
		// Normalize again to detach the summarizer from retained application
		// settings and the prefix returned for checkpoint/cache planning.
		normalized, err := llm.NormalizeRequest(summary)
		if err != nil {
			return PreparedCompactInput{}, preparationError(provider.CodeInvalidArgument)
		}
		result.Request = &normalized
	}
	if err := ctx.Err(); err != nil {
		return PreparedCompactInput{}, err
	}
	return result, nil
}

func validatePreparationContext(ctx context.Context, resolved bool) error {
	if ctx == nil || resolved {
		return preparationError(provider.CodeConfiguration)
	}
	return ctx.Err()
}

func validatePreparationReplay(caller llm.RequestContext, parent string, replay state.MaterializedState) error {
	if parent == "" {
		// CheckpointReplay returns the zero state for a root. Stale inherited
		// settings must never turn a root into an implicit continuation.
		if !reflect.ValueOf(replay).IsZero() {
			return preparationError(provider.CodeStateCorrupt)
		}
		return nil
	}
	if replay.Handle != state.Handle(parent) || replay.Tenant != caller.Tenant || replay.Project != caller.Project || replay.Depth < 0 {
		return preparationError(provider.CodeStateCorrupt)
	}
	pending, err := state.ValidateTranscript(replay.Items)
	if err != nil || !equalCheckpointFrontier(pending, replay.PendingToolCalls) || replay.Settings.Validate() != nil {
		return preparationError(provider.CodeStateCorrupt)
	}
	return nil
}

func prepareSemanticRequest(caller llm.RequestContext, operationKey string, settings state.ModelState, items []llm.Item) (llm.Request, error) {
	request := llm.Request{
		APIVersion: llm.APIVersion, OperationKey: operationKey, Context: caller,
		Model: settings.Model, ServiceClass: settings.ServiceClass,
		ServiceClassFallbacks: settings.ServiceClassFallbacks, Portability: settings.Portability,
		Instructions: settings.Instructions, Input: items, Tools: settings.Tools,
		ToolPolicy: settings.ToolPolicy, Output: settings.Output, Extensions: settings.Extensions,
	}
	if settings.TemperatureDecimal != nil {
		value, err := settings.TemperatureDecimal.Float64()
		if err != nil {
			return llm.Request{}, err
		}
		request.Sampling = &llm.SamplingSpec{Temperature: &value}
	} else if settings.Temperature != nil {
		request.Sampling = &llm.SamplingSpec{Temperature: settings.Temperature}
	}
	if settings.ReasoningEffort != "" || settings.ReasoningSummary != "" {
		request.Reasoning = &llm.ReasoningSpec{Effort: settings.ReasoningEffort, Summary: settings.ReasoningSummary}
	}
	return llm.NormalizeRequest(request)
}

func preparationError(code provider.Code) error {
	return provider.NewError(code, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "request preparation failed")
}

// decodeCompactionPolicy decodes a stored or requested policy over explicit
// defaults, rejecting unknown fields and invalid values. An empty value is the
// default policy.
func decodeCompactionPolicy(raw json.RawMessage) (compaction.Policy, error) {
	policy := compaction.DefaultPolicy()
	if len(raw) == 0 {
		return policy, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if !json.Valid(trimmed) || len(trimmed) == 0 || trimmed[0] != '{' {
		return compaction.Policy{}, errors.New("compaction policy must be a JSON object")
	}
	// The Policy decoder intentionally accepts partial policies, but does not
	// itself reject unknown fields.
	type policyFields compaction.Policy
	decoded := policyFields(policy)
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return compaction.Policy{}, err
	}
	policy = compaction.Policy(decoded)
	if err := policy.Validate(); err != nil {
		return compaction.Policy{}, err
	}
	return policy, nil
}
