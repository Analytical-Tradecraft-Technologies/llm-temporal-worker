package adaptertest

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

// Declare validates the adapter's test profile with one feature declared as
// capability, and returns the validated profile's capabilities and those an
// adapter built from it reports. An error is the NewProfile rejection.
type Declare func(capability provider.Capability) (validated, reported provider.CapabilitySet, err error)

// CapabilityCannotOutrunAdapterPort checks that a profile cannot declare
// feature native or emulated, NewProfile naming wantError, and that an
// unknown declaration with a transform is normalized to unsupported without
// it both in the validated profile and in what the adapter reports.
func CapabilityCannotOutrunAdapterPort(t *testing.T, feature provider.Feature, wantError string, declare Declare) {
	for _, state := range []provider.CapabilityState{provider.CapabilityNative, provider.CapabilityEmulated} {
		t.Run(string(state), func(t *testing.T) {
			if _, _, err := declare(provider.Capability{State: state}); err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("NewProfile() error = %v, want an %s capability error", err, wantError)
			}
		})
	}
	validated, reported, err := declare(provider.Capability{State: provider.CapabilityUnknown, Transform: "unverified-" + string(feature)})
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range []struct {
		name         string
		capabilities provider.CapabilitySet
	}{{"validated", validated}, {"reported", reported}} {
		if capability := set.capabilities.Features[feature]; capability.State != provider.CapabilityUnsupported || capability.Transform != "" {
			t.Fatalf("%s %s capability = %#v, want normalized unsupported capability", set.name, feature, capability)
		}
	}
}

// StreamingCapabilityCannotOutrunAdapterPort checks that an adapter without
// OpenStream ships a default profile without streaming, with a reason, and
// that no profile can declare streaming it cannot serve.
func StreamingCapabilityCannotOutrunAdapterPort(t *testing.T, adapter any, defaults provider.CapabilitySet, declare Declare) {
	if capability := defaults.Features[provider.FeatureStreaming]; capability.State != provider.CapabilityUnsupported || capability.Reason == "" {
		t.Fatalf("default streaming capability = %#v, want unsupported with a reason", capability)
	}
	CapabilityCannotOutrunAdapterPort(t, provider.FeatureStreaming, "OpenStream", declare)
	if _, ok := adapter.(provider.StreamingAdapter); ok {
		t.Fatal("adapter advertises streaming capability without an OpenStream implementation")
	}
}

// UndeclaredServiceClassesAreUnsupported checks that a profile mapping only
// the standard class validates and refuses the others as unsupported, and
// that one mapping no class is rejected. validate validates the adapter's
// default profile with tiers and returns its tier lookup.
func UndeclaredServiceClassesAreUnsupported(t *testing.T, validate func(tiers map[llm.ServiceClass]string) (providerTier func(llm.ServiceClass) (string, error), err error)) {
	providerTier, err := validate(map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: ""})
	if err != nil {
		t.Fatalf("standard-only profile rejected: %v", err)
	}
	if tier, err := providerTier(llm.ServiceClassStandard); err != nil || tier != "default" {
		t.Fatalf("standard tier = %q, %v", tier, err)
	}
	if _, err := providerTier(llm.ServiceClassEconomy); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("economy tier error = %v, want unsupported", err)
	}
	if _, err := validate(map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "", llm.ServiceClassPriority: ""}); err == nil || !strings.Contains(err.Error(), "at least one service class") {
		t.Fatalf("profile with no supported class error = %v", err)
	}
}

// StrictCompactionRequest is the summarizer request compaction builds from a
// request with application instructions, as an adapter must lower it under
// strict portability.
func StrictCompactionRequest(t *testing.T) llm.Request {
	t.Helper()
	input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	source := llm.Request{OperationKey: "generate", Model: "claude-test", Input: input, Instructions: []llm.Instruction{
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "You are a helpful assistant"},
	}}
	request, err := compaction.PrepareRequest(source, "generate/compact", input, compaction.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	return compaction.FlattenSummarizerInstructions(request)
}

// SameJSON reports whether got and want encode the same JSON value,
// comparing numbers by their exact literal rather than as float64.
func SameJSON(t *testing.T, got, want []byte) bool {
	t.Helper()
	decode := func(data []byte) any {
		t.Helper()
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	return reflect.DeepEqual(decode(got), decode(want))
}

// ToolHistoryPreservesJSONNumbers checks that a replayed tool call's
// arguments keep numbers a float64 cannot hold exactly. toolInput lowers the
// request and returns the encoded input of its first tool call; the encoded
// provider document is asserted, not a float64-decoded approximation.
func ToolHistoryPreservesJSONNumbers(t *testing.T, toolInput func(t *testing.T, request llm.Request) []byte) {
	for _, arguments := range []string{
		`{"account_id":9007199254740993}`,
		`{"negative":-9007199254740993,"unsigned":18446744073709551615}`,
		`{"nested":[{"decimal":0.123456789012345678901,"exponent":1.234567890123456789e+25},true,null,"9007199254740993"]}`,
	} {
		t.Run(arguments, func(t *testing.T) {
			request := llm.Request{Model: "contract-model", Input: []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(arguments)}}}
			if actual := toolInput(t, request); !SameJSON(t, actual, []byte(arguments)) {
				t.Fatalf("tool input changed: got %s, want %s", actual, arguments)
			}
		})
	}
}

// CachedInputCostUsesDisjointComponents checks that lift, which lifts a
// response "resp" reporting an inclusive input total with read and write
// cache counts, splits it into disjoint ordinary, cache-read and cache-write
// tokens that price correctly, keeps the provider's raw totals under
// inputKey and total_tokens, and rejects inconsistent counts as an invalid
// response the provider accepted.
func CachedInputCostUsesDisjointComponents(t *testing.T, inputKey string, lift func(total, read, write int64) (llm.Response, error)) {
	for _, tc := range []struct {
		name                         string
		total, read, write, ordinary int64
		cost                         string
		invalid                      bool
	}{
		{name: "no cache", total: 1_000_000, ordinary: 1_000_000, cost: "1.000000000000000000"},
		{name: "partial hit", total: 1_000_000, read: 800_000, ordinary: 200_000, cost: "0.280000000000000000"},
		{name: "full hit", total: 1_000_000, read: 1_000_000, cost: "0.100000000000000000"},
		{name: "cache write", total: 1_000_000, write: 1_000_000, cost: "1.250000000000000000"},
		{name: "mixed", total: 1_000_000, read: 600_000, write: 200_000, ordinary: 200_000, cost: "0.510000000000000000"},
		{name: "zero", cost: "0.000000000000000000"},
		{name: "negative total", total: -1, invalid: true},
		{name: "negative read", total: 1, read: -1, invalid: true},
		{name: "negative write", total: 1, write: -1, invalid: true},
		{name: "read exceeds total", total: 1, read: 2, invalid: true},
		{name: "combined exceeds total", total: 3, read: 2, write: 2, invalid: true},
		{name: "overflowing cache sum", total: math.MaxInt64, read: math.MaxInt64, write: math.MaxInt64, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lifted, err := lift(tc.total, tc.read, tc.write)
			if tc.invalid {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted || mapped.Provider.ResponseID != "resp" {
					t.Fatalf("error = %#v, want accepted invalid response", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			usage := lifted.Usage
			if tc.total > 0 && string(usage.ProviderRaw[inputKey]) != strconv.FormatInt(tc.total, 10) {
				t.Fatalf("inclusive input total lost: %+v", usage.ProviderRaw)
			}
			if usage.InputTokens != tc.ordinary || usage.CacheReadTokens != tc.read || usage.CacheWriteTokens != tc.write {
				t.Fatalf("usage = %+v", usage)
			}
			cost, err := pricing.CostFromUsage(pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1"), CacheReadPerMillion: pricing.MustDecimalUSD("0.10"), CacheWritePerMillion: pricing.MustDecimalUSD("1.25")}}, pricing.Usage{InputTokens: usage.InputTokens, CacheReadTokens: usage.CacheReadTokens, CacheWriteTokens: usage.CacheWriteTokens})
			if err != nil {
				t.Fatal(err)
			}
			if cost.USD.String() != tc.cost {
				t.Fatalf("cost = %s, want %s", cost.USD.String(), tc.cost)
			}
			if tc.total > 0 && string(usage.ProviderRaw["total_tokens"]) != strconv.FormatInt(tc.total, 10) {
				t.Fatalf("provider total lost: %+v", usage.ProviderRaw)
			}
		})
	}
}
