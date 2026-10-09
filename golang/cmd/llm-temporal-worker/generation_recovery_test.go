package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestGenerationRecoverCommandRequiresExplicitRunAndBoundedArguments(t *testing.T) {
	for _, extra := range [][]string{{}, {"--workflow-id", "private-workflow"}, {"--run-id", "private-run"}, {"--workflow-id", "private-workflow", "--run-id", "private-run", "--timeout", "3m"}, {"--workflow-id", "private-workflow", "--run-id", "private-run", "--history-file", "private-file"}} {
		var stderr bytes.Buffer
		args := append([]string{"generation-recover", "--config", exampleConfigPath(t)}, extra...)
		code := Execute(context.Background(), args, CommandOptions{ErrOut: &stderr, RunGenerationRecover: func(context.Context, []byte, string, string, bool, io.Writer) error {
			t.Fatal("invalid invocation reached runtime")
			return nil
		}})
		if code != 2 || strings.Contains(stderr.String(), "private-") {
			t.Fatalf("invalid/unsafe invocation: %d %q", code, stderr.String())
		}
	}
}

func TestGenerationRecoverCommandPassesExplicitIdentityAndApply(t *testing.T) {
	for _, apply := range []bool{false, true} {
		args := []string{"generation-recover", "--config", exampleConfigPath(t), "--workflow-id", "private-workflow", "--run-id", "private-run", "--timeout", "2s"}
		if apply {
			args = append(args, "--apply")
		}
		called := false
		code := Execute(context.Background(), args, CommandOptions{RunGenerationRecover: func(ctx context.Context, data []byte, workflowID, runID string, mutate bool, out io.Writer) error {
			called = true
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 2*time.Second || workflowID != "private-workflow" || runID != "private-run" || mutate != apply || len(data) == 0 {
				t.Fatal("recovery arguments changed")
			}
			return nil
		}})
		if code != 0 || !called {
			t.Fatalf("recovery not called: %d", code)
		}
	}
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"generation-recover", "--config", exampleConfigPath(t), "--workflow-id", "private-workflow", "--run-id", "private-run"}, CommandOptions{ErrOut: &stderr, RunGenerationRecover: func(context.Context, []byte, string, string, bool, io.Writer) error {
		return errors.New("private-storage-detail")
	}})
	if code != 1 || strings.Contains(stderr.String(), "private-") {
		t.Fatalf("unsafe runtime error: %d %q", code, stderr.String())
	}
}
