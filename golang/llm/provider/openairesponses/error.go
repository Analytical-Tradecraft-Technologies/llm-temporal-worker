package openairesponses

import (
	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/openaierror"
)

func mapError(err error) *provider.Error {
	return openaierror.MapError(err, adapterName)
}

func mapAPIError(apiErr *openai.Error) *provider.Error {
	return openaierror.MapAPIError(apiErr, adapterName)
}
