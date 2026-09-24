package config

import (
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func (endpoint EndpointConfig) validateCodexCLI(path string, providerTimeout Duration) error {
	if endpoint.CodexCLI == nil {
		return fmt.Errorf("%s.codex_cli explicit private approval is required", path)
	}
	if err := endpoint.CodexCLI.Validate(); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if endpoint.BaseURL != "" || len(endpoint.OutboundHosts) != 0 || endpoint.AWSWorkspaceID != "" || len(endpoint.Extensions) != 0 || endpoint.ProviderStorage.Permitted {
		return fmt.Errorf("%s codex_cli cannot configure HTTP transport, extensions or provider-hosted state", path)
	}
	if endpoint.Auth.Kind != "chatgpt_cli" {
		return fmt.Errorf("%s codex_cli requires chatgpt_cli auth; API-key fallback is forbidden", path)
	}
	if err := endpoint.Auth.Validate(path + ".auth"); err != nil {
		return err
	}
	if endpoint.AccountRegion == "" || endpoint.Region == "" || endpoint.CapabilityProfile == "" || endpoint.PriceCatalog == "" {
		return fmt.Errorf("%s codex_cli requires explicit account/region, capability and estimate pricing identities", path)
	}
	if endpoint.Timeout <= 0 || endpoint.Timeout > providerTimeout || time.Duration(endpoint.Timeout) > 30*time.Minute {
		return fmt.Errorf("%s codex_cli timeout must be positive and no greater than provider_timeout or 30m", path)
	}
	if len(endpoint.ServiceClasses) != 1 || endpoint.ServiceClasses[llm.ServiceClassStandard].ProviderValue != "subscription" {
		return fmt.Errorf("%s codex_cli supports only standard mapped to subscription", path)
	}
	return nil
}
