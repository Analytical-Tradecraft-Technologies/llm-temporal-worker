package runtime

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
)

// Endpoint extensions are identity-mapped per namespace, so two namespaces
// declaring the same key would map to one provider wire field. Building the
// profile must refuse that configuration rather than leave the effective
// value to map iteration order.
func TestEndpointProfilesRejectExtensionNamespacesSharingAWireField(t *testing.T) {
	colliding := map[string]map[string]any{"alpha": {"value": nil}, "beta": {"value": nil}}
	distinct := map[string]map[string]any{"alpha": {"value": nil}, "beta": {"other": nil}}
	factory := &ProductionEngineFactory{}
	classes := map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: "default"}}

	chat := func(extensions map[string]map[string]any) error {
		endpoint := config.EndpointConfig{Family: "openai_chat", BaseURL: "https://chat.example.test/v1", ServiceClasses: classes, Extensions: extensions}
		_, err := factory.chatProfile("chat", endpoint, completeCapabilities(provider.CapabilitySet{Version: "cap-v1"}), EndpointProfile{}, false, openRouterUpstreams(nil), config.OpenRouterUpstreamConfig{})
		return err
	}
	anthropic := func(extensions map[string]map[string]any) error {
		endpoint := config.EndpointConfig{Family: "anthropic_messages", BaseURL: "https://anthropic.example.test", ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassStandard: {ProviderValue: "standard_only"},
		}, Extensions: extensions}
		_, err := factory.anthropicProfile("anthropic", endpoint, anthropicmessages.DefaultProfile("capabilities").Capabilities, EndpointProfile{})
		return err
	}
	for name, build := range map[string]func(map[string]map[string]any) error{"chat": chat, "anthropic": anthropic} {
		t.Run(name, func(t *testing.T) {
			testassert.NoError(t, build(distinct), "nonoverlapping extensions rejected")
			testassert.ErrorContains(t, build(colliding), `both map to wire field "value"`)
		})
	}
}
