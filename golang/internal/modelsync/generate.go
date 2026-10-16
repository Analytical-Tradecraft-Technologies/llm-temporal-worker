package modelsync

import (
	"bytes"
	"cmp"
	"fmt"
	"math/big"
	"path"
	"slices"
	"strings"
)

// generatedProvider describes how the default rules generator reads one
// first-party provider's prices from OpenRouter's upstream endpoints.
type generatedProvider struct {
	name    string
	prefix  string
	modelID string
	// tiers maps a conventional endpoint provider_value to the OpenRouter
	// endpoint tags, in preference order, that publish its price.
	tiers []generatedTier
	// skip lists OpenRouter IDs that are not model IDs of the provider's own
	// API (OpenRouter presets, media-only models, completions-only models).
	skip []string
	// keep lists OpenRouter IDs a skip pattern matches that are nonetheless
	// model IDs of the provider's own API.
	keep []string
	// windowAtLongPrompt caps the context window at the first long-prompt
	// price threshold instead of rendering a long-prompt price, for a
	// provider whose window beyond that threshold needs a request option the
	// adapters never send, so the provider itself rejects longer prompts.
	windowAtLongPrompt bool
	// longPromptModels lists OpenRouter IDs whose full window needs no such
	// option although windowAtLongPrompt is set, so their long-prompt price
	// is rendered instead.
	longPromptModels []string
}

// capsWindowAtLongPrompt reports whether id's window is capped at its first
// long-prompt price threshold.
func (provider generatedProvider) capsWindowAtLongPrompt(id string) bool {
	return provider.windowAtLongPrompt && !slices.Contains(provider.longPromptModels, id)
}

type generatedTier struct {
	tier string
	tags []string
}

var generatedProviders = []generatedProvider{
	{
		name: "openai", prefix: "openai/", modelID: ModelIDVerbatim,
		tiers: []generatedTier{
			{tier: "flex", tags: []string{"openai/flex"}},
			{tier: "default", tags: []string{"openai", "openai/default"}},
			{tier: "priority", tags: []string{"openai/fast"}},
		},
		// OpenRouter's "-high" and most "-pro" IDs are reasoning presets of a
		// base model (openai/gpt-6-astra-pro is gpt-6-astra served with
		// reasoning.mode pro) that OpenAI's API does not serve, so only the
		// Pro models OpenAI itself lists are kept.
		skip: []string{"openai/*-high", "openai/*-pro", "openai/*image*", "openai/*audio*", "openai/gpt-chat-latest", "openai/gpt-3.5-turbo-instruct"},
		keep: []string{"openai/gpt-5-pro", "openai/gpt-5.2-pro", "openai/gpt-5.4-pro", "openai/gpt-5.5-pro", "openai/o1-pro", "openai/o3-pro"},
	},
	{
		name: "anthropic", prefix: "anthropic/", modelID: ModelIDDotsToDashes,
		tiers: []generatedTier{
			{tier: "standard_only", tags: []string{"anthropic"}},
			{tier: "auto", tags: []string{"anthropic"}},
		},
		// Older Anthropic models' long-context window needs a beta header
		// (Claude 4.6 and later serve 1M at one price, with no override).
		// Haiku 5.5 serves its 1M window by default and bills prompts over
		// 100,000 tokens at higher prices.
		// https://platform.claude.com/docs/en/build-with-claude/context-windows
		windowAtLongPrompt: true,
		longPromptModels:   []string{"anthropic/claude-haiku-5.5"},
	},
}

// exaRules are maintained by hand: OpenRouter does not list Exa.
const exaRules = `  exa:
    prefix: exa/
    model_id: verbatim
    models:
      # Exa's Answer API, served by its Chat Completions endpoint as model
      # "exa" and billed per request (https://exa.ai/pricing). Exa reports
      # each call's actual cost.
      exa/answer:
        model: exa
        family: openai_chat
        prices:
          standard:
            input_per_million: "0"
            output_per_million: "0"
            cache_read_per_million: "0"
            cache_write_per_million: "0"
            per_request: "0.005"
      # Exa's Agent API, served by its Responses endpoint as model
      # "exa-agent". A fixed effort bills one flat price per request:
      # minimal $0.012, low $0.025, medium $0.10. The adapter always sends a
      # fixed effort no higher than medium (Exa rejects high and above on a
      # synchronous request), so medium bounds the reservation; Exa reports
      # each call's actual cost.
      exa/agent:
        model: exa-agent
        family: openai_responses
        prices:
          standard:
            input_per_million: "0"
            output_per_million: "0"
            cache_read_per_million: "0"
            cache_write_per_million: "0"
            per_request: "0.10"
`

// GenerateDefaultRules renders the built-in rules file from an OpenRouter
// Document. Direct prices are the first-party upstream endpoint prices
// OpenRouter publishes (for example its "openai/flex" or "anthropic"
// endpoint), which are those providers' list prices. A direct route carries
// the first long-prompt price as a long_prompt tier, or, where the provider
// rejects longer prompts without a request option the adapters never send,
// has its context window capped at that threshold. Models a provider's own API no longer serves have no
// first-party endpoint and are left out. An endpoint OpenRouter lists at a
// discount (a provider promotion) is rendered at its undiscounted price under
// a REVIEW comment, so a temporary price is never checked in silently.
func GenerateDefaultRules(document Document) ([]byte, error) {
	if err := document.Validate(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	fmt.Fprintf(&out, `# Built-in model-sync rules, compiled into the worker binary and therefore
# shipped in its image. Operators override them at runtime with
# model_sync.rules files; a later file overrides an earlier one field by
# field, and prices merge per tier.
#
# Every model is named by its OpenRouter ID. Direct prices are keyed by the
# endpoint's service_classes provider_value.
#
# GENERATED by "go run ./tools/modelsyncdefaults" from the first-party
# upstream endpoint prices OpenRouter published at %s.
# Review the diff before committing; the Exa section is maintained by hand in
# golang/internal/modelsync/generate.go.
version: %s

providers:
`, document.FetchedAt.UTC().Format("2006-01-02T15:04:05Z"), RulesVersion)
	for _, provider := range generatedProviders {
		fmt.Fprintf(&out, "  %s:\n    prefix: %s\n    model_id: %s\n    models:\n", provider.name, provider.prefix, provider.modelID)
		count := 0
		for _, model := range document.Models {
			if !strings.HasPrefix(model.ID, provider.prefix) || provider.skipped(model.ID) {
				continue
			}
			rendered, ok, err := renderGeneratedModel(provider, model)
			if err != nil {
				return nil, fmt.Errorf("model %q: %w", model.ID, err)
			}
			if ok {
				out.WriteString(rendered)
				count++
			}
		}
		if count == 0 {
			return nil, fmt.Errorf("no %s models have first-party endpoints", provider.name)
		}
	}
	out.WriteString(exaRules)
	rendered := out.Bytes()
	parsed, err := ParseRules(rendered)
	if err != nil {
		return nil, err
	}
	if _, err := MergeRules(parsed); err != nil {
		return nil, err
	}
	return rendered, nil
}

// skipped reports whether id matches a skip pattern and is not kept.
func (provider generatedProvider) skipped(id string) bool {
	if slices.Contains(provider.keep, id) {
		return false
	}
	for _, pattern := range provider.skip {
		if matched, _ := path.Match(pattern, id); matched {
			return true
		}
	}
	return false
}

func renderGeneratedModel(provider generatedProvider, model Model) (string, bool, error) {
	byTag := make(map[string]Endpoint, len(model.Endpoints))
	for _, endpoint := range model.Endpoints {
		byTag[endpoint.Tag] = endpoint
	}
	type tierPrice struct {
		tier     string
		endpoint Endpoint
	}
	priced := make([]tierPrice, 0, len(provider.tiers))
	for _, tier := range provider.tiers {
		for _, tag := range tier.tags {
			if endpoint, ok := byTag[tag]; ok {
				priced = append(priced, tierPrice{tier: tier.tier, endpoint: endpoint})
				break
			}
		}
	}
	if len(priced) == 0 {
		return "", false, nil
	}
	contextTokens, outputTokens := model.ContextLength, model.MaxCompletionTokens
	capAtLongPrompt := provider.capsWindowAtLongPrompt(model.ID)
	for _, value := range priced {
		contextTokens = minPositive(contextTokens, value.endpoint.ContextLength)
		outputTokens = minPositive(outputTokens, value.endpoint.MaxCompletionTokens)
		// Rules carry at most one long-prompt tier; a further threshold caps
		// the window so no request reaches a price the rules do not hold.
		if overrides := value.endpoint.Pricing.Overrides; capAtLongPrompt && len(overrides) > 0 {
			contextTokens = minPositive(contextTokens, overrides[0].MinPromptTokens)
		} else if len(overrides) > 1 {
			contextTokens = minPositive(contextTokens, overrides[1].MinPromptTokens)
		}
	}
	var out strings.Builder
	fmt.Fprintf(&out, "      %s:\n", model.ID)
	if contextTokens > 0 {
		fmt.Fprintf(&out, "        context_tokens: %d\n", contextTokens)
	}
	if outputTokens > 0 {
		fmt.Fprintf(&out, "        output_tokens: %d\n", outputTokens)
	}
	out.WriteString("        prices:\n")
	for _, value := range priced {
		prices, err := undiscountedPricing(value.endpoint.Pricing)
		if err != nil {
			return "", false, fmt.Errorf("%s endpoint %q: %w", value.tier, value.endpoint.Tag, err)
		}
		cacheRead := prices.InputCacheRead
		if cacheRead == "" {
			cacheRead = prices.Prompt
		}
		// The adapters never request a one-hour cache, so the five-minute
		// write price applies; an unlisted write price is not billed.
		cacheWrite := prices.InputCacheWrite
		if cacheWrite == "" {
			cacheWrite = "0"
		}
		request := prices.Request
		if request == "" {
			request = "0"
		}
		fields := []struct{ name, perToken string }{
			{"input_per_million", prices.Prompt}, {"output_per_million", prices.Completion},
			{"cache_read_per_million", cacheRead}, {"cache_write_per_million", cacheWrite},
		}
		if discount := value.endpoint.Pricing.Discount; discount != "" {
			fmt.Fprintf(&out, "          # REVIEW: OpenRouter lists endpoint %q at a %s discount; these are its undiscounted\n", value.endpoint.Tag, discount)
			out.WriteString("          # prices. Confirm them against the provider's published list price.\n")
		}
		fmt.Fprintf(&out, "          %s:\n", value.tier)
		if err := writePriceFields(&out, "            ", fields, request); err != nil {
			return "", false, fmt.Errorf("%s %w", value.tier, err)
		}
		if capAtLongPrompt || len(prices.Overrides) == 0 {
			continue
		}
		// An override component it omits keeps the base price.
		override := prices.Overrides[0]
		longFields := []struct{ name, perToken string }{
			{"input_per_million", cmp.Or(override.Prompt, prices.Prompt)}, {"output_per_million", cmp.Or(override.Completion, prices.Completion)},
			{"cache_read_per_million", cmp.Or(override.InputCacheRead, prices.InputCacheRead, prices.Prompt)},
			{"cache_write_per_million", cmp.Or(override.InputCacheWrite, cacheWrite)},
		}
		fmt.Fprintf(&out, "            long_prompt:\n              min_prompt_tokens: %d\n", override.MinPromptTokens)
		if err := writePriceFields(&out, "              ", longFields, request); err != nil {
			return "", false, fmt.Errorf("%s long_prompt %w", value.tier, err)
		}
	}
	return out.String(), true, nil
}

// writePriceFields renders per-token prices as per-million rule fields.
func writePriceFields(out *strings.Builder, indent string, fields []struct{ name, perToken string }, request string) error {
	for _, field := range fields {
		converted, err := perMillion(field.perToken)
		if err != nil {
			return fmt.Errorf("%s: %w", field.name, err)
		}
		fmt.Fprintf(out, "%s%s: %q\n", indent, field.name, converted.CanonicalString())
	}
	fmt.Fprintf(out, "%sper_request: %q\n", indent, request)
	return nil
}

// undiscountedPricing returns prices with OpenRouter's listed discount taken
// back off, so a promotional price is never rendered as a list price. It fails
// when a discount leaves no list price or the list price is not an exact
// decimal.
func undiscountedPricing(prices Pricing) (Pricing, error) {
	if prices.Discount == "" {
		return prices, nil
	}
	discount, ok := new(big.Rat).SetString(prices.Discount)
	if !ok {
		return Pricing{}, fmt.Errorf("discount %q is not a decimal", prices.Discount)
	}
	remaining := new(big.Rat).Sub(big.NewRat(1, 1), discount)
	if discount.Sign() < 0 || remaining.Sign() <= 0 {
		return Pricing{}, fmt.Errorf("discount %q is outside [0, 1)", prices.Discount)
	}
	targets := []*string{&prices.Prompt, &prices.Completion, &prices.InputCacheRead, &prices.InputCacheWrite, &prices.InputCacheWrite1h, &prices.Request}
	prices.Overrides = append([]PricingOverride(nil), prices.Overrides...)
	for index := range prices.Overrides {
		override := &prices.Overrides[index]
		targets = append(targets, &override.Prompt, &override.Completion, &override.InputCacheRead, &override.InputCacheWrite, &override.InputCacheWrite1h)
	}
	for _, target := range targets {
		if *target == "" {
			continue
		}
		price, ok := new(big.Rat).SetString(*target)
		if !ok {
			return Pricing{}, fmt.Errorf("price %q is not a decimal", *target)
		}
		list, err := exactDecimal(price.Quo(price, remaining))
		if err != nil {
			return Pricing{}, fmt.Errorf("price %q at discount %q: %w", *target, prices.Discount, err)
		}
		*target = list
	}
	return prices, nil
}

// exactDecimal formats a rational as a terminating decimal, or fails when it
// has none (its reduced denominator has a prime factor other than 2 and 5).
func exactDecimal(value *big.Rat) (string, error) {
	denominator := new(big.Int).Set(value.Denom())
	places := 0
	for _, factor := range []int64{2, 5} {
		divisor, count := big.NewInt(factor), 0
		for new(big.Int).Rem(denominator, divisor).Sign() == 0 {
			denominator.Quo(denominator, divisor)
			count++
		}
		places = max(places, count)
	}
	if denominator.Cmp(big.NewInt(1)) != 0 {
		return "", fmt.Errorf("undiscounted price %s is not an exact decimal", value.RatString())
	}
	return value.FloatString(places), nil
}
