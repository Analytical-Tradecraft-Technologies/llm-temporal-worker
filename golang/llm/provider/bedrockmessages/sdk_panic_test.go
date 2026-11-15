package bedrockmessages

import (
	"context"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

func TestInvokeRecoversAnSDKPanicWhileDecodingTheResponse(t *testing.T) {
	client := newBedrockTestClient(t, "http://127.0.0.1", &http.Client{Transport: adaptertest.PanickingTransport(http.StatusOK)}, nil)
	adapter, call := compileBedrockProdCall(t, client, "http://127.0.0.1", "op-panic")
	_, err := adapter.Invoke(context.Background(), call, nil)
	adaptertest.RecoveredDecodePanic(t, err, "op-panic")
}
