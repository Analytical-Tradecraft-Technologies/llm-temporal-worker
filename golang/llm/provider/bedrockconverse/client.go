package bedrockconverse

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider/internal/clientconfig"
)

// ClientConfig contains resolved, non-secret client settings. AWS credentials
// remain inside AWSConfig and are never copied into a provider-neutral call.
type ClientConfig struct {
	BaseURL    string
	HTTPClient *http.Client
	AWSConfig  aws.Config
}

// Client owns the official AWS Bedrock Runtime client. Converse is deliberately
// the only operation exposed by this package: Temporal activities use a
// deterministic one-shot request boundary and do not expose live streaming.
type Client struct {
	converse converseService
}

type converseService interface {
	Converse(context.Context, *bedrockruntime.ConverseInput, ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error)
}

// NewClient constructs an AWS-signed Bedrock Runtime client without performing
// network I/O. BaseURL is optional for production (the SDK resolves the
// regional endpoint) and useful for deterministic contract tests.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if ctx == nil {
		return nil, fmt.Errorf("bedrock converse: context is required")
	}
	if config.HTTPClient == nil {
		return nil, fmt.Errorf("bedrock converse: HTTP client is required")
	}
	if config.AWSConfig.Region == "" {
		return nil, fmt.Errorf("bedrock converse: AWS region is required")
	}
	awsConfig := config.AWSConfig
	awsConfig.HTTPClient = config.HTTPClient
	awsConfig.Retryer = func() aws.Retryer { return aws.NopRetryer{} }
	if config.BaseURL != "" {
		validated, err := clientconfig.BaseURL(config.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("bedrock converse: %w", err)
		}
		awsConfig.BaseEndpoint = aws.String(validated)
	}
	return &Client{converse: bedrockruntime.NewFromConfig(awsConfig)}, nil
}
