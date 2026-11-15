package compaction

import "testing"

// TestEvaluateTriggerIsMonotonicInProjectedCounters protects the decision
// boundary from accidentally treating a larger projected conversation as
// safer. Once a limit has been crossed, adding tokens, items, lineage, bytes,
// or reserved reasoning capacity must not clear the compaction decision.
func TestEvaluateTriggerIsMonotonicInProjectedCounters(t *testing.T) {
	policy := DefaultPolicy()
	base := TriggerInput{
		ProjectedTokens:            policy.TriggerTokens - 1,
		ProjectedBytes:             policy.MaterializationThresholdBytes - 1,
		ProjectedItems:             1,
		ProjectedLineageDepth:      1,
		ProviderContextLimitTokens: 0,
		ProviderItemLimit:          0,
		ProviderLineageLimit:       0,
		ReservedReasoningTokens:    0,
		ContinuationExpired:        false,
	}

	cases := []struct {
		name string
		edit func(*TriggerInput)
	}{
		{name: "tokens", edit: func(input *TriggerInput) { input.ProjectedTokens++ }},
		{name: "bytes", edit: func(input *TriggerInput) { input.ProjectedBytes++ }},
		{name: "items", edit: func(input *TriggerInput) {
			input.ProviderItemLimit = input.ProjectedItems + 1
			input.ProjectedItems++
		}},
		{name: "lineage", edit: func(input *TriggerInput) {
			input.ProviderLineageLimit = input.ProjectedLineageDepth + 1
			input.ProjectedLineageDepth++
		}},
		{name: "reasoning reserve", edit: func(input *TriggerInput) {
			input.ProviderContextLimitTokens = policy.OutputReserveTokens + 1
			input.ReservedReasoningTokens++
		}},
		{name: "continuation expiry", edit: func(input *TriggerInput) { input.ContinuationExpired = true }},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			input := base
			test.edit(&input)
			decision := mustEvaluateTrigger(t, policy, input)
			if !decision.ShouldCompact {
				t.Fatalf("projected limit crossed without compaction: %#v", input)
			}
		})
	}
}

func TestEvaluateTriggerDisablesZeroProviderLimits(t *testing.T) {
	policy := DefaultPolicy()
	input := TriggerInput{
		ProjectedTokens:       policy.TriggerTokens - 1,
		ProjectedItems:        1_000_000,
		ProjectedLineageDepth: 1_000_000,
		ProjectedBytes:        policy.MaterializationThresholdBytes - 1,
	}
	decision := mustEvaluateTrigger(t, policy, input)
	if decision.ShouldCompact {
		t.Fatalf("zero provider limits unexpectedly triggered compaction: %#v", decision)
	}

	input.ProjectedTokens = policy.TriggerTokens
	decision = mustEvaluateTrigger(t, policy, input)
	if !decision.ShouldCompact || decision.Reason != TriggerReasonTokens {
		t.Fatalf("policy threshold decision = %#v, want token trigger", decision)
	}
}

func TestEvaluateTriggerReasonPrecedenceCoversEveryBoundary(t *testing.T) {
	policy := DefaultPolicy()
	cases := []struct {
		name   string
		input  TriggerInput
		reason TriggerReason
	}{
		{
			name:   "provider context",
			input:  TriggerInput{ProjectedTokens: 9_000, ProviderContextLimitTokens: policy.OutputReserveTokens + 9_000},
			reason: TriggerReasonContextTokens,
		},
		{
			name:   "policy tokens",
			input:  TriggerInput{ProjectedTokens: policy.TriggerTokens},
			reason: TriggerReasonTokens,
		},
		{
			name:   "provider items",
			input:  TriggerInput{ProjectedItems: 3, ProviderItemLimit: 3},
			reason: TriggerReasonItems,
		},
		{
			name:   "provider lineage",
			input:  TriggerInput{ProjectedLineageDepth: 2, ProviderLineageLimit: 2},
			reason: TriggerReasonLineage,
		},
		{
			name:   "materialized bytes",
			input:  TriggerInput{ProjectedBytes: policy.MaterializationThresholdBytes},
			reason: TriggerReasonBytes,
		},
		{
			name:   "expired continuation",
			input:  TriggerInput{ContinuationExpired: true},
			reason: TriggerReasonContinuation,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			decision := mustEvaluateTrigger(t, policy, test.input)
			if !decision.ShouldCompact || decision.Reason != test.reason {
				t.Fatalf("decision = %#v, want compact reason %q", decision, test.reason)
			}
			if decision.TargetTokens != policy.TargetTokens {
				t.Fatalf("target_tokens = %d, want %d", decision.TargetTokens, policy.TargetTokens)
			}
		})
	}
}
