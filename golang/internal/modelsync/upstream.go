package modelsync

import "sort"

// UpstreamPolicy decides which OpenRouter upstream endpoints may serve a
// request. Every OpenRouter request names the endpoints it allows, so an
// endpoint the policy rejects is never used.
type UpstreamPolicy struct {
	// Countries holds the ISO 3166-1 alpha-2 codes a provider must be in: its
	// headquarters and every datacenter it lists. A provider that publishes
	// no headquarters or no datacenter never qualifies.
	Countries map[string]struct{}
	// NativePrecision allows only endpoints whose published quantization the
	// rules list as native for the model.
	NativePrecision bool
	// ZeroRetention allows only endpoints OpenRouter lists as retaining no
	// prompts or completions, and asks OpenRouter to enforce the same.
	ZeroRetention bool
	// DenyPromptTraining asks OpenRouter to route only to providers that do
	// not collect prompts for training. OpenRouter does not publish this per
	// endpoint, so it is enforced by OpenRouter rather than filtered here.
	DenyPromptTraining bool
}

// Upstream is the set of OpenRouter endpoints a model may be served by.
type Upstream struct {
	// Tags are the allowed endpoint tags, sorted; never empty.
	Tags []string
	// Quantizations are the allowed quantizations when native precision is
	// required, sorted; otherwise nil.
	Quantizations []string
}

// NewUpstreamPolicy builds a policy for the named country list.
func NewUpstreamPolicy(countryList string, nativePrecision, zeroRetention, denyPromptTraining bool) (UpstreamPolicy, error) {
	countries, err := CountryList(countryList)
	if err != nil {
		return UpstreamPolicy{}, err
	}
	return UpstreamPolicy{Countries: countries, NativePrecision: nativePrecision, ZeroRetention: zeroRetention, DenyPromptTraining: denyPromptTraining}, nil
}

// Upstreams returns the allowed endpoints of every Document model that has
// at least one. A model absent from the result may not be sent to OpenRouter.
func (policy UpstreamPolicy) Upstreams(document *Document, rules Rules) map[string]Upstream {
	result := map[string]Upstream{}
	if document == nil {
		return result
	}
	providers := make(map[string]bool, len(document.Providers))
	for _, provider := range document.Providers {
		providers[provider.Slug] = policy.providerAllowed(provider)
	}
	for _, model := range document.Models {
		var upstream Upstream
		allowedQuantization := map[string]struct{}{}
		if policy.NativePrecision {
			quantizations, listed := rules.NativeQuantizations(model.ID)
			if !listed {
				continue
			}
			for _, quantization := range quantizations {
				allowedQuantization[quantization] = struct{}{}
			}
			upstream.Quantizations = quantizations
			sort.Strings(upstream.Quantizations)
		}
		for _, endpoint := range model.Endpoints {
			if !providers[endpoint.Provider] {
				continue
			}
			if policy.ZeroRetention && !endpoint.ZeroRetention {
				continue
			}
			if policy.NativePrecision {
				if _, allowed := allowedQuantization[endpoint.Quantization]; !allowed {
					continue
				}
			}
			upstream.Tags = append(upstream.Tags, endpoint.Tag)
		}
		if len(upstream.Tags) > 0 {
			sort.Strings(upstream.Tags)
			result[model.ID] = upstream
		}
	}
	return result
}

// Allowed filters a model's endpoints to the tags in upstream.
func (upstream Upstream) Allowed(endpoints []Endpoint) []Endpoint {
	allowed := make(map[string]struct{}, len(upstream.Tags))
	for _, tag := range upstream.Tags {
		allowed[tag] = struct{}{}
	}
	result := make([]Endpoint, 0, len(upstream.Tags))
	for _, endpoint := range endpoints {
		if _, ok := allowed[endpoint.Tag]; ok {
			result = append(result, endpoint)
		}
	}
	return result
}

func (policy UpstreamPolicy) providerAllowed(provider Provider) bool {
	if provider.Headquarters == "" || len(provider.Datacenters) == 0 {
		return false
	}
	if _, ok := policy.Countries[provider.Headquarters]; !ok {
		return false
	}
	for _, country := range provider.Datacenters {
		if _, ok := policy.Countries[country]; !ok {
			return false
		}
	}
	return true
}
