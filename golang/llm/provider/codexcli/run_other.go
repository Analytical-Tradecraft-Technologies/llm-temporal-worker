//go:build !linux

package codexcli

import (
	"context"
	"fmt"
	"os"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// The descriptor-pinned ELF execution and process-group policy are Linux-only.
// Other platforms must fail before quota can be consumed, not run a weaker path.
func privateDirectory(string) error {
	return fmt.Errorf("codex_cli requires Linux execution isolation")
}

func pinnedExecutable(Config) (*os.File, error) {
	return nil, fmt.Errorf("codex_cli requires Linux execution isolation")
}

func (*Adapter) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{}, rejected(provider.CodeConfiguration, "codex_cli requires Linux execution isolation")
}
