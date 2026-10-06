package config

// LangfuseConfig holds references only; resolved credentials never enter a
// configuration digest or a Temporal payload. An omitted block disables export.
type LangfuseConfig struct {
	BaseURL   string    `yaml:"base_url" json:"base_url"`
	PublicKey SecretRef `yaml:"public_key" json:"public_key"`
	SecretKey SecretRef `yaml:"secret_key" json:"secret_key"`
}

type LangfuseEndpointConfig struct {
	Enabled *bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`
}

func (c LangfuseConfig) validate() error {
	if _, err := normalizedHTTPSURLHost(c.BaseURL, "langfuse.base_url", false); err != nil {
		return err
	}
	if err := c.PublicKey.Validate("langfuse.public_key"); err != nil {
		return err
	}
	return c.SecretKey.Validate("langfuse.secret_key")
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
