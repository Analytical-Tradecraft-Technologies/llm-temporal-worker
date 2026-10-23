package openaichat

import (
	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaierror"
)

func mapError(err error, profileName string) *provider.Error {
	return openaierror.MapError(err, profileName)
}

func mapAPIError(apiErr *openai.Error, profileName string) *provider.Error {
	return openaierror.MapAPIError(apiErr, profileName)
}
