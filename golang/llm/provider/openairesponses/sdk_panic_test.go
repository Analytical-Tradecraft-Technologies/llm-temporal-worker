package openairesponses

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	adapter, call := compileOpenAIProdCall(t, &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)}, "op-panic")
	_, err := adapter.Invoke(context.Background(), call, nil)
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
}
