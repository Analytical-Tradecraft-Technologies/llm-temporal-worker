package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// Every invalid invocation exits 2 with one line saying why (#1533).
func TestInvalidInvocationsExplainExitStatusTwo(t *testing.T) {
	config := exampleConfigPath(t)
	for _, args := range [][]string{
		{},
		{"no-such-command"},
		{"health-server", "extra"},
		{"health-server", "--no-such-flag"},
		{"healthcheck"},
		{"healthcheck", "--no-such-flag"},
		{"healthcheck", "--url", "relative/path"},
		{"validate-config", "--no-such-flag"},
		{"print-effective-config", "--no-such-flag"},
		{"worker", "--no-such-flag"},
		{"budget-initialize", "--config", config, "extra"},
		{"budget-initialize", "--config", config, "--timeout", "0s"},
		{"budget-initialize", "--config", config, "--timeout", "-1s"},
		{"budget-initialize", "--config", config, "--timeout", "10m"},
		{"budget-initialize", "--config", config, "--timeout", "not-a-duration"},
		{"generation-recover", "--config", config},
		{"pending-list", "--config", config, "extra"},
		{"pending-list", "--config", config, "--stale-after", "0s"},
		{"pending-list", "--config", config, "--timeout", "11m"},
		{"pending-recover", "--config", config},
		{"pending-recover", "--config", config, "--stale", "--request-id", "llmtw_req_00000000-0000-4000-8000-000000000001"},
	} {
		var stderr bytes.Buffer
		code := Execute(context.Background(), args, CommandOptions{
			ErrOut: &stderr,
			RunBudgetInitialize: func(context.Context, []byte, bool, bool, io.Writer) error {
				t.Fatalf("%v reached the runtime", args)
				return nil
			},
			RunGenerationRecover: func(context.Context, []byte, string, string, bool, io.Writer) error {
				t.Fatalf("%v reached the runtime", args)
				return nil
			},
			RunPendingList: func(context.Context, []byte, time.Duration, io.Writer) error {
				t.Fatalf("%v reached the runtime", args)
				return nil
			},
			RunPendingRecover: func(context.Context, []byte, []string, bool, time.Duration, bool, io.Writer) error {
				t.Fatalf("%v reached the runtime", args)
				return nil
			},
		})
		if code != 2 || strings.TrimSpace(stderr.String()) == "" {
			t.Errorf("%v: exit %d, stderr %q; want exit 2 with a reason", args, code, stderr.String())
		}
	}
}
