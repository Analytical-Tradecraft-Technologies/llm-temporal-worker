package runtime

import (
	"encoding/hex"

	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// Durable v1 continuation pinning.
//
// Provider state (encrypted reasoning, thinking signatures, hosted-tool
// records) travels inline in the canonical transcript. Publication records,
// per transcript item, the route that produced it: provider, endpoint, the
// endpoint's configuration-version-independent account identity, API family
// and resolved provider model. Planning derives state.Constraints from
// the newest recorded pin and applies state.CheckPinning to every candidate,
// exactly as the legacy engine does, with the state marked optional because
// the canonical transcript stays complete. A candidate on another lineage is
// rejected with continuation_pinned in strict mode; in best-effort mode it
// compiles without the provider state pinned elsewhere and the response
// carries a provider_state_dropped diagnostic.
//
// Items without recorded provenance (checkpoints written before provenance
// existed, or state supplied by the caller in Append) are never constraints
// and are never stripped: they keep the adapters' family-only check. A request
// with no recorded provenance therefore plans exactly as before.
//
// An account is compared only when both sides prove it. Provenance recorded
// without an account, and a route or saved plan without one, each get their
// own sentinel that matches nothing: such state stays off every route (strict
// mode reports continuation_pinned, best-effort drops it). That fails closed;
// treating a missing account as a wildcard could replay account-bound state
// to another account after a reload reused the endpoint ID.

const (
	diagnosticContinuationPinned    = "continuation_pinned"
	diagnosticProviderStateDropped  = "provider_state_dropped"
	continuationPinnedSafeDetailKey = "continuation"

	// Never valid hex digests, and different from each other, so an unproven
	// account can match neither a real account nor the other side's gap.
	unrecordedPinAccount = "unrecorded-account"
	unknownRouteAccount  = "unknown-route-account"
)

// recordedPinning is the pin of one recorded provenance entry.
func recordedPinning(value state.ProviderStateProvenance) state.Pinning {
	pin := value.Pinning()
	if pin.AccountRegion == "" {
		pin.AccountRegion = unrecordedPinAccount
	}
	return pin
}

func accountKey(digest [32]byte) string {
	if digest == ([32]byte{}) {
		return unknownRouteAccount
	}
	return hex.EncodeToString(digest[:])
}

// providerStatePins maps an index of the semantic request's Input to the pin
// of the route that produced the provider state in that item.
type providerStatePins struct {
	byIndex map[int]state.Pinning
	latest  state.Pinning
}

func (pins providerStatePins) present() bool { return len(pins.byIndex) > 0 }

// newProviderStatePins validates materialized provenance against the parent
// transcript. Every entry must name an item that carries provider state; any
// other shape means the checkpoint and its blobs disagree.
func newProviderStatePins(items []llm.Item, provenance []state.ProviderStateProvenance) (providerStatePins, bool) {
	if len(provenance) == 0 {
		return providerStatePins{}, true
	}
	if state.ValidateProviderStateProvenance(provenance) != nil {
		return providerStatePins{}, false
	}
	pins := providerStatePins{byIndex: make(map[int]state.Pinning, len(provenance))}
	for _, value := range provenance {
		if value.Ordinal >= len(items) || !itemHasProviderState(items[value.Ordinal]) {
			return providerStatePins{}, false
		}
		pins.byIndex[value.Ordinal] = recordedPinning(value)
		// Ordinals are strictly increasing, so the last entry is the newest.
		pins.latest = recordedPinning(value)
	}
	return pins, true
}

// constraints is the legacy planning view of the newest pin. Durable v1 state
// is optional: the canonical transcript is complete without it.
func (pins providerStatePins) constraints(mode llm.PortabilityMode) state.Constraints {
	if !pins.present() {
		return state.Constraints{}
	}
	return state.Constraints{Present: true, Provider: pins.latest.Provider, EndpointID: pins.latest.EndpointID,
		AccountRegion: pins.latest.AccountRegion, Family: pins.latest.Family, ModelLineage: pins.latest.ModelLineage, TranscriptComplete: true, Portability: mode}
}

// admits applies state.CheckPinning to one candidate. It always admits a
// request without recorded provenance.
func (pins providerStatePins) admits(mode llm.PortabilityMode, candidate routing.Candidate) bool {
	if !pins.present() {
		return true
	}
	return state.CheckPinning(pins.constraints(mode), candidatePinning(candidate)).Decision != state.CompatibilityRejected
}

// candidatePinning is the pin publication records for a response from this
// candidate. It is derived from candidate fields, which every planner sets,
// rather than from Candidate.Pinning, which a custom planner may leave empty.
func candidatePinning(candidate routing.Candidate) state.Pinning {
	return state.Pinning{Provider: candidate.Provider, EndpointID: candidate.EndpointID, AccountRegion: accountKey(candidate.EndpointAccountDigest),
		Family: candidate.Family, ModelLineage: candidate.Model}
}

// planPinning is the same pin read back from a saved budget plan.
func planPinning(plan cloudstate.BudgetPlan) state.Pinning {
	account := unknownRouteAccount
	if plan.EndpointAccount != "" {
		account = plan.EndpointAccount
	}
	return state.Pinning{Provider: plan.Route.Provider, EndpointID: plan.Route.EndpointID, AccountRegion: account, Family: plan.Family, ModelLineage: plan.Route.Model}
}

// strip returns input without the provider state pinned to another lineage
// than pin, and the number of items it changed. It never mutates input.
func (pins providerStatePins) strip(input []llm.Item, pin state.Pinning) ([]llm.Item, int) {
	if !pins.present() {
		return input, 0
	}
	var result []llm.Item
	dropped := 0
	for index, item := range input {
		recorded, ok := pins.byIndex[index]
		if !ok || recorded == pin {
			if result != nil {
				result = append(result, item)
			}
			continue
		}
		if result == nil {
			result = append(make([]llm.Item, 0, len(input)), input[:index]...)
		}
		dropped++
		if kept, ok := withoutProviderState(item); ok {
			result = append(result, kept)
		}
	}
	if result == nil {
		return input, 0
	}
	return result, dropped
}

func itemHasProviderState(item llm.Item) bool {
	switch value := item.(type) {
	case llm.ProviderState, *llm.ProviderState:
		return true
	case llm.Message:
		return partsHaveProviderState(value.Content)
	case *llm.Message:
		return value != nil && partsHaveProviderState(value.Content)
	}
	return false
}

func partsHaveProviderState(parts []llm.Part) bool {
	for _, part := range parts {
		switch part.(type) {
		case llm.ProviderStatePart, *llm.ProviderStatePart:
			return true
		}
	}
	return false
}

// withoutProviderState removes provider state from one item. A provider-state
// item, or a message left with no content, is removed entirely.
func withoutProviderState(item llm.Item) (llm.Item, bool) {
	var message llm.Message
	switch value := item.(type) {
	case llm.Message:
		message = value
	case *llm.Message:
		message = *value
	default:
		return nil, false
	}
	content := make([]llm.Part, 0, len(message.Content))
	for _, part := range message.Content {
		switch part.(type) {
		case llm.ProviderStatePart, *llm.ProviderStatePart:
			continue
		}
		content = append(content, part)
	}
	if len(content) == 0 {
		return nil, false
	}
	message.Content = content
	return message, true
}

// responseProvenance records the provider state a fresh response added to the
// transcript. Output items start at offset in the child's transcript.
func responseProvenance(output []llm.Item, offset int, pin state.Pinning) []state.ProviderStateProvenance {
	var result []state.ProviderStateProvenance
	for index, item := range output {
		if itemHasProviderState(item) {
			account := pin.AccountRegion
			if account == unknownRouteAccount {
				// Recorded empty, which later reads as unproven.
				account = ""
			}
			result = append(result, state.ProviderStateProvenance{Ordinal: offset + index, Provider: pin.Provider,
				EndpointID: pin.EndpointID, EndpointFamily: pin.Family, ModelLineage: pin.ModelLineage, Account: account})
		}
	}
	return result
}

// replayedProvenance is the provenance of output replayed from the worker
// response cache. origin is the provenance recorded on the checkpoint that
// first published that output. A cache hit requires the same semantic input,
// so the origin's transcript was input followed by output, exactly as the
// replay's is: entries at or after len(input) belong to the output and keep
// their ordinals, and earlier entries (a snapshot origin repeats what it
// inherited) are the parent's, which the replay's own lineage already carries.
// Every output entry must name an output item that carries provider state;
// anything else means the origin and its cached response disagree.
func replayedProvenance(origin []state.ProviderStateProvenance, input, output []llm.Item) ([]state.ProviderStateProvenance, bool) {
	if state.ValidateProviderStateProvenance(origin) != nil {
		return nil, false
	}
	var result []state.ProviderStateProvenance
	for _, value := range origin {
		if value.Ordinal < len(input) {
			continue
		}
		index := value.Ordinal - len(input)
		if index >= len(output) || !itemHasProviderState(output[index]) {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

// compactedProvenance carries the parent's provenance into a compaction child.
// A no-work child keeps the parent transcript unchanged. Otherwise the child
// transcript is the summary followed by the retained items: references moved
// out of the prefix, then the verbatim suffix. Provider state is never a
// reference, so a retained item at parent index i lands at i-len(Prefix)+1,
// and state in the summarized prefix is gone with its provenance.
func compactedProvenance(parent state.MaterializedState, selection compaction.PrefixSelection, summarized bool) ([]state.ProviderStateProvenance, error) {
	if _, ok := newProviderStatePins(parent.Items, parent.ProviderStateProvenance); !ok {
		return nil, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	if !summarized || len(parent.ProviderStateProvenance) == 0 {
		return append([]state.ProviderStateProvenance(nil), parent.ProviderStateProvenance...), nil
	}
	if len(selection.Prefix)+len(selection.Retained) != len(parent.Items) {
		return nil, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	var result []state.ProviderStateProvenance
	next, nonReferences := 0, 0
	for index, item := range parent.Items {
		if next < len(parent.ProviderStateProvenance) && parent.ProviderStateProvenance[next].Ordinal == index {
			if nonReferences >= len(selection.Prefix) {
				value := parent.ProviderStateProvenance[next]
				value.Ordinal = index - len(selection.Prefix) + 1
				result = append(result, value)
			}
			next++
		}
		if item.ItemKind() != llm.ItemKindReference {
			nonReferences++
		}
	}
	return result, nil
}

// droppedStateDiagnostic is the best-effort portability diagnostic a v1
// response carries when its route compiled without some provider state.
func droppedStateDiagnostic() llm.Diagnostic {
	return llm.Diagnostic{Code: diagnosticProviderStateDropped, Severity: llm.DiagnosticWarning,
		Message: "optional provider state pinned to another route was not replayed"}
}

// continuationPinnedFailure marks a selection error whose candidates were
// rejected because the continuation is pinned to another route. The public
// error code set is closed, so the code is unchanged; the reason is carried as
// a safe detail and in each rejected route's reason.
func continuationPinnedFailure(failure *provider.Error, rejections []planningRejection) {
	for _, rejection := range rejections {
		if rejection.Reason == routing.RejectContinuation {
			if failure.SafeDetails == nil {
				failure.SafeDetails = map[string]string{}
			}
			failure.SafeDetails[continuationPinnedSafeDetailKey] = diagnosticContinuationPinned
			return
		}
	}
}
