package config

import "fmt"

// DefaultLangfuseMaxInFlight is the per-worker export concurrency used when
// langfuse.max_in_flight is omitted.
const DefaultLangfuseMaxInFlight = 2

// LangfuseConfig holds references only; resolved credentials never enter a
// configuration digest or a Temporal payload. An omitted block disables export.
type LangfuseConfig struct {
	BaseURL   string    `yaml:"base_url" json:"base_url"`
	PublicKey SecretRef `yaml:"public_key" json:"public_key"`
	SecretKey SecretRef `yaml:"secret_key" json:"secret_key"`
	// MaxInFlight bounds the export requests one worker sends to Langfuse at
	// once, and so the Activity slots a slow Langfuse can hold. Zero means
	// DefaultLangfuseMaxInFlight.
	MaxInFlight int `yaml:"max_in_flight,omitempty" json:"max_in_flight,omitempty"`
}

type LangfuseEndpointConfig struct {
	Enabled *bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
}

// EffectiveMaxInFlight applies the default to an omitted limit.
func (c LangfuseConfig) EffectiveMaxInFlight() int {
	if c.MaxInFlight == 0 {
		return DefaultLangfuseMaxInFlight
	}
	return c.MaxInFlight
}

// validate checks the block on its own; activitySlots is the worker's
// Activity concurrency. An explicit export limit must leave at least one slot
// for model Activities.
func (c LangfuseConfig) validate(activitySlots int) error {
	if _, err := normalizedHTTPSURLHost(c.BaseURL, "langfuse.base_url", false); err != nil {
		return err
	}
	if err := c.PublicKey.Validate("langfuse.public_key"); err != nil {
		return err
	}
	if err := c.SecretKey.Validate("langfuse.secret_key"); err != nil {
		return err
	}
	if c.MaxInFlight < 0 || (c.MaxInFlight > 0 && c.MaxInFlight >= activitySlots) {
		return fmt.Errorf("langfuse.max_in_flight must be positive and less than temporal.worker.max_concurrent_activities")
	}
	return nil
}

// LangfuseEnabled uses the endpoint's configured dialect rather than its model
// name: OpenAI models served by OpenRouter retain the OpenRouter default.
func (e EndpointConfig) LangfuseEnabled() bool {
	if e.Langfuse != nil && e.Langfuse.Enabled != nil {
		return *e.Langfuse.Enabled
	}
	_, router := e.Extensions["openrouter"]
	return !router
}
