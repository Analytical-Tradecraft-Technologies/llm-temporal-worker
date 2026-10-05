package architecturetest

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestComposeLogRedactionTerminatesWhenSecretMatchesMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "sh", filepath.Join(moduleRoot(t), "scripts", "redact-compose-logs.sh"))
	command.Env = []string{
		"LLMTW_LOG_REDACT_REDIS_PASSWORD=REDACTED",
		"LLMTW_LOG_REDACT_POSTGRES_PASSWORD=ACT",
		"LLMTW_LOG_REDACT_MOCK_API_KEY=mock-key",
		"LLMTW_LOG_REDACT_CONTINUATION_HMAC=hmac-value",
	}
	command.Stdin = strings.NewReader("connection failed: password=REDACTED key=mock-key hmac=hmac-value\n")
	output, err := command.Output()
	if ctx.Err() != nil {
		t.Fatal("redaction did not terminate when a secret occurs in its own replacement marker")
	}
	if err != nil {
		t.Fatal(err)
	}
	got := string(output)
	for _, secret := range []string{"mock-key", "hmac-value", "password=REDACTED "} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted output still contains %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "password=[RED") {
		t.Fatalf("password was not redacted: %q", got)
	}
}
