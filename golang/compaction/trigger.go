package compaction

import "fmt"

// TriggerReason identifies the first durable limit that requires compaction.
// The order is intentional: callers can record one stable reason even when
// several limits are crossed in the same checkpoint.
type TriggerReason string

const (
	TriggerReasonNone          TriggerReason = "none"
	TriggerReasonContextTokens TriggerReason = "provider_context_tokens"
	TriggerReasonTokens        TriggerReason = "policy_trigger_tokens"
	TriggerReasonItems         TriggerReason = "provider_items"
	TriggerReasonLineage       TriggerReason = "provider_lineage"
	TriggerReasonBytes         TriggerReason = "materialization_bytes"
	TriggerReasonContinuation  TriggerReason = "continuation_expired"
)

// TriggerInput contains counters and provider capabilities projected for the
// next Generate checkpoint. Counters must describe the request before adding
// another model turn; zero provider limits disable that corresponding check.
// RetainedTokens is the part of ProjectedTokens that compaction cannot remove:
// instructions, tool schemas, the retained recent window and the new turn.
// Continuation expiry is supplied as a boolean because the engine owns the
// durable clock/deadline interpretation.
type TriggerInput struct {
	ProjectedTokens            int
	RetainedTokens             int
	ProjectedBytes             int64
	ProjectedItems             int
	ProjectedLineageDepth      int
	ProviderContextLimitTokens int
	ProviderItemLimit          int
	ProviderLineageLimit       int
	ReservedReasoningTokens    int
	ContinuationExpired        bool
}

// TriggerDecision is the deterministic result of evaluating one checkpoint.
// TargetTokens is populated only when ShouldCompact is true and is the
// hysteresis target the prefix selector should aim to leave behind.
type TriggerDecision struct {
	ShouldCompact bool
	Reason        TriggerReason
	TargetTokens  int
}

// EvaluateTrigger applies provider limits before policy limits. The provider
// context check reserves both the generic output budget and any adapter-owned
// reasoning budget. Policy trigger_tokens is the fallback when no provider
// context limit is available; it is skipped when the tokens compaction cannot
// remove already reach it, because a summary would leave the request over the
// trigger and every following turn would buy another summarizer call. The
// lower target_tokens value prevents a compaction loop: a freshly materialized
// checkpoint below that target does not trigger again until a limit is crossed.
func (policy Policy) EvaluateTrigger(input TriggerInput) (TriggerDecision, error) {
	if err := policy.Validate(); err != nil {
		return TriggerDecision{}, err
	}
	if input.ProjectedTokens < 0 {
		return TriggerDecision{}, fmt.Errorf("projected tokens must be non-negative")
	}
	if input.RetainedTokens < 0 {
		return TriggerDecision{}, fmt.Errorf("retained tokens must be non-negative")
	}
	if input.ProjectedBytes < 0 {
		return TriggerDecision{}, fmt.Errorf("projected bytes must be non-negative")
	}
	if input.ProjectedItems < 0 {
		return TriggerDecision{}, fmt.Errorf("projected items must be non-negative")
	}
	if input.ProjectedLineageDepth < 0 {
		return TriggerDecision{}, fmt.Errorf("projected lineage depth must be non-negative")
	}
	if input.ProviderContextLimitTokens < 0 {
		return TriggerDecision{}, fmt.Errorf("provider context limit must be non-negative")
	}
	if input.ProviderItemLimit < 0 {
		return TriggerDecision{}, fmt.Errorf("provider item limit must be non-negative")
	}
	if input.ProviderLineageLimit < 0 {
		return TriggerDecision{}, fmt.Errorf("provider lineage limit must be non-negative")
	}
	if input.ReservedReasoningTokens < 0 {
		return TriggerDecision{}, fmt.Errorf("reserved reasoning tokens must be non-negative")
	}

	if input.ProviderContextLimitTokens > 0 {
		effectiveLimit := input.ProviderContextLimitTokens - policy.OutputReserveTokens - input.ReservedReasoningTokens
		if effectiveLimit < 1 || input.ProjectedTokens >= effectiveLimit {
			return triggered(TriggerReasonContextTokens, policy), nil
		}
	}
	if input.ProjectedTokens >= policy.TriggerTokens && input.RetainedTokens < policy.TriggerTokens {
		return triggered(TriggerReasonTokens, policy), nil
	}
	if input.ProviderItemLimit > 0 && input.ProjectedItems >= input.ProviderItemLimit {
		return triggered(TriggerReasonItems, policy), nil
	}
	if input.ProviderLineageLimit > 0 && input.ProjectedLineageDepth >= input.ProviderLineageLimit {
		return triggered(TriggerReasonLineage, policy), nil
	}
	if policy.MaterializationThresholdBytes > 0 && input.ProjectedBytes >= policy.MaterializationThresholdBytes {
		return triggered(TriggerReasonBytes, policy), nil
	}
	if input.ContinuationExpired {
		return triggered(TriggerReasonContinuation, policy), nil
	}
	return TriggerDecision{Reason: TriggerReasonNone}, nil
}

func triggered(reason TriggerReason, policy Policy) TriggerDecision {
	return TriggerDecision{ShouldCompact: true, Reason: reason, TargetTokens: policy.TargetTokens}
}
