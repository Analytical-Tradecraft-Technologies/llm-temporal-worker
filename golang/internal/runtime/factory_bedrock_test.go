package runtime

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/bedrockconverse"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/bedrockmessages"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

func TestProductionFactoryBuildsBedrockAdaptersWithAWSRegion(t *testing.T) {
	for _, test := range []struct {
		name       string
		family     string
		endpointID string
		wantFamily provider.Family
		wantName   string
		profile    EndpointProfile
		wantRegion string
	}{
		{
			name:       "anthropic messages",
			family:     "bedrock_anthropic_messages",
			endpointID: "bedrock-messages",
			wantFamily: provider.FamilyBedrockMessages,
			wantName:   "bedrock.messages/bedrock-messages",
			profile: EndpointProfile{Bedrock: func() *bedrockmessages.Profile {
				value, _ := bedrockmessages.NewDefaultProfile("bedrock-messages")
				return &value
			}()},
			wantRegion: "us-west-2",
		},
		{
			name:       "converse",
			family:     "bedrock_converse",
			endpointID: "bedrock-converse",
			wantFamily: provider.FamilyBedrockConverse,
			wantName:   "bedrock.converse/bedrock-converse",
			profile: EndpointProfile{BedrockConverse: func() *bedrockconverse.Profile {
				value, _ := bedrockconverse.NewDefaultProfile("bedrock-converse")
				return &value
			}()},
			wantRegion: "eu-west-1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := endpointFamily(test.family); got != test.wantFamily {
				t.Fatalf("endpointFamily(%q) = %q, want %q", test.family, got, test.wantFamily)
			}
			var regions []string
			resolvedSecret := false
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				HTTPClient: &http.Client{},
				Profiles:   map[string]EndpointProfile{test.endpointID: test.profile},
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					resolvedSecret = true
					return nil, errors.New("Bedrock default-chain auth must not resolve provider secrets")
				}),
				AWSConfigFactory: func(_ context.Context, region string) (aws.Config, error) {
					regions = append(regions, region)
					return aws.Config{Region: region}, nil
				},
			}}
			endpointID := test.endpointID
			value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: {
				Family: test.family, BaseURL: "https://bedrock-runtime.example.test", OutboundHosts: []string{"bedrock-runtime.example.test"},
				Region: test.wantRegion, Auth: config.AuthConfig{Kind: "aws_default_chain"},
			}}}
			snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
				"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "bedrock/v1"}}}},
			}}}

			adapter, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
			if err != nil {
				t.Fatalf("buildAdapter() error = %v", err)
			}
			if adapter == nil || adapter.Name() != test.wantName {
				t.Fatalf("adapter = %#v, want %s", adapter, test.wantName)
			}
			if resolvedSecret {
				t.Fatal("Bedrock aws_default_chain auth attempted provider-secret resolution")
			}
			if len(regions) != 1 || regions[0] != test.wantRegion {
				t.Fatalf("AWS config factory regions = %#v, want [%q]", regions, test.wantRegion)
			}
		})
	}
}

func TestProductionFactoryRejectsNonDefaultBedrockAuthBeforeDependencies(t *testing.T) {
	for _, family := range []string{"bedrock_anthropic_messages", "bedrock_converse"} {
		t.Run(family, func(t *testing.T) {
			resolverCalled := false
			awsConfigCalled := false
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				HTTPClient: &http.Client{},
				Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
					resolverCalled = true
					return []byte("unexpected"), nil
				}),
				AWSConfigFactory: func(context.Context, string) (aws.Config, error) {
					awsConfigCalled = true
					return aws.Config{Region: "us-east-1"}, nil
				},
			}}
			endpointID := "bedrock"
			value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: {
				Family: family, BaseURL: "https://bedrock-runtime.example.test", OutboundHosts: []string{"bedrock-runtime.example.test"},
				Region: "us-east-1", Auth: config.AuthConfig{Kind: "bearer_env", Name: "BEDROCK_KEY"},
			}}}
			snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
				"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "bedrock/v1"}}}},
			}}}

			_, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
			if !errors.Is(err, ErrUnsupportedProviderAuth) {
				t.Fatalf("buildAdapter() error = %v, want ErrUnsupportedProviderAuth", err)
			}
			if resolverCalled {
				t.Fatal("unsupported Bedrock auth attempted provider-secret resolution")
			}
			if awsConfigCalled {
				t.Fatal("unsupported Bedrock auth constructed AWS config")
			}
		})
	}
}

func TestProductionFactoryBuildsBedrockMessagesWithGeneratedProfile(t *testing.T) {
	endpointID := "bedrock-messages"
	region := "us-west-2"
	awsConfigCalled := false
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		HTTPClient: &http.Client{},
		AWSConfigFactory: func(_ context.Context, gotRegion string) (aws.Config, error) {
			awsConfigCalled = true
			if gotRegion != region {
				t.Fatalf("AWS config region = %q, want %q", gotRegion, region)
			}
			return aws.Config{Region: gotRegion}, nil
		},
	}}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: {
		Family: "bedrock_anthropic_messages", BaseURL: "https://bedrock-runtime.example.test", OutboundHosts: []string{"bedrock-runtime.example.test"},
		Region: region, Auth: config.AuthConfig{Kind: "aws_default_chain"},
		ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassEconomy:  {ProviderValue: "flex"},
			llm.ServiceClassStandard: {ProviderValue: "default"},
			llm.ServiceClassPriority: {ProviderValue: "priority"},
		},
	}}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "bedrock/v1"}}}},
	}}}
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	bedrockAdapter, ok := adapter.(*bedrockmessages.Adapter)
	if !ok {
		t.Fatalf("adapter = %T, want *bedrockmessages.Adapter", adapter)
	}
	profile := bedrockAdapter.Profile()
	if profile.ExpectedBaseURL != value.Endpoints[endpointID].BaseURL+"/" || profile.ServiceTiers[llm.ServiceClassEconomy] != "flex" || profile.ServiceTiers[llm.ServiceClassStandard] != "default" || profile.ServiceTiers[llm.ServiceClassPriority] != "priority" {
		t.Fatalf("generated Bedrock Messages profile = %#v", profile)
	}
	if !awsConfigCalled {
		t.Fatal("AWS config factory was not called")
	}
}

func TestProductionFactoryBuildsBedrockConverseWithGeneratedProfile(t *testing.T) {
	endpointID := "bedrock-converse"
	region := "eu-west-1"
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		HTTPClient: &http.Client{},
		AWSConfigFactory: func(_ context.Context, gotRegion string) (aws.Config, error) {
			if gotRegion != region {
				t.Fatalf("AWS config region = %q, want %q", gotRegion, region)
			}
			return aws.Config{Region: gotRegion}, nil
		},
	}}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: {
		Family: "bedrock_converse", BaseURL: "https://bedrock-runtime.example.test", OutboundHosts: []string{"bedrock-runtime.example.test"},
		Region: region, Auth: config.AuthConfig{Kind: "aws_default_chain"},
		ServiceClasses: map[llm.ServiceClass]config.TierConfig{
			llm.ServiceClassEconomy:  {ProviderValue: "flex"},
			llm.ServiceClassStandard: {ProviderValue: "default"},
			llm.ServiceClassPriority: {ProviderValue: "priority"},
		},
	}}}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "bedrock/v1"}}}},
	}}}
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	bedrockAdapter, ok := adapter.(*bedrockconverse.Adapter)
	if !ok {
		t.Fatalf("adapter = %T, want *bedrockconverse.Adapter", adapter)
	}
	profile := bedrockAdapter.Profile()
	if profile.ExpectedBaseURL != value.Endpoints[endpointID].BaseURL+"/" || profile.ServiceTiers[llm.ServiceClassEconomy] != "flex" || profile.ServiceTiers[llm.ServiceClassStandard] != "default" || profile.ServiceTiers[llm.ServiceClassPriority] != "priority" {
		t.Fatalf("generated Bedrock Converse profile = %#v", profile)
	}
	if got := profile.Capabilities.Features[provider.FeatureStreaming].State; got != provider.CapabilityUnsupported {
		t.Fatalf("generated Bedrock Converse streaming capability = %q, want unsupported", got)
	}
}

func TestProductionFactoryFailsClosedWhenBedrockAWSConfigFactoryFails(t *testing.T) {
	wantErr := errors.New("credential chain unavailable")
	for _, family := range []string{"bedrock_anthropic_messages", "bedrock_converse"} {
		t.Run(family, func(t *testing.T) {
			const region = "us-east-2"
			var gotRegion string
			factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
				HTTPClient: &http.Client{},
				AWSConfigFactory: func(_ context.Context, region string) (aws.Config, error) {
					gotRegion = region
					return aws.Config{}, wantErr
				},
			}}
			endpointID := "bedrock"
			value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: {
				Family: family, BaseURL: "https://bedrock-runtime.example.test", OutboundHosts: []string{"bedrock-runtime.example.test"},
				Region: region, Auth: config.AuthConfig{Kind: "aws_default_chain"},
				ServiceClasses: map[llm.ServiceClass]config.TierConfig{
					llm.ServiceClassEconomy:  {ProviderValue: "flex"},
					llm.ServiceClassStandard: {ProviderValue: "default"},
					llm.ServiceClassPriority: {ProviderValue: "priority"},
				},
			}}}
			snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
				"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "bedrock/v1"}}}},
			}}}
			_, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID)
			if !errors.Is(err, wantErr) {
				t.Fatalf("buildAdapter() error = %v, want AWS config error", err)
			}
			if gotRegion != region {
				t.Fatalf("AWS config factory region = %q, want %q", gotRegion, region)
			}
		})
	}
}
