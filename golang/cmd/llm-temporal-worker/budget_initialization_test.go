package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBudgetInitializeCommandIsExplicitAndBounded(t *testing.T) {
	path := exampleConfigPath(t)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, apply := range []bool{false, true} {
		var output, stderr bytes.Buffer
		called := false
		args := []string{"budget-initialize", "--config", path, "--timeout", "2s"}
		if apply {
			args = append(args, "--apply")
		}
		code := Execute(context.Background(), args, CommandOptions{
			Out: &output, ErrOut: &stderr,
			RunBudgetInitialize: func(ctx context.Context, data []byte, mutate bool, out io.Writer) error {
				called = true
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 2*time.Second {
					t.Fatalf("initializer did not receive the bounded context: %v", deadline)
				}
				if mutate != apply || !bytes.Equal(data, want) {
					t.Fatal("initializer received a different configuration or mutation flag")
				}
				_, err := io.WriteString(out, "{\"status\":\"ready\"}\n")
				return err
			},
		})
		if code != 0 || !called || stderr.Len() != 0 || !strings.Contains(output.String(), `"status":"ready"`) {
			t.Fatalf("code=%d called=%t stdout=%q stderr=%q", code, called, output.String(), stderr.String())
		}
	}
}

func TestBudgetInitializeCommandRejectsInvalidFlagsBeforeCallingRuntime(t *testing.T) {
	for _, extra := range [][]string{
		{"--timeout", "0s"}, {"--timeout", "-1s"}, {"--timeout", "6m"},
		{"--timeout", "not-a-duration"}, {"--reset"}, {"unexpected"},
	} {
		args := append([]string{"budget-initialize", "--config", exampleConfigPath(t)}, extra...)
		code := Execute(context.Background(), args, CommandOptions{
			RunBudgetInitialize: func(context.Context, []byte, bool, io.Writer) error {
				t.Fatal("invalid invocation reached runtime")
				return nil
			},
		})
		if code != 2 {
			t.Fatalf("flags %v returned %d", extra, code)
		}
	}
}

func TestBudgetInitializeCommandPropagatesDeadlineAndReturnsFailure(t *testing.T) {
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"budget-initialize", "--config", exampleConfigPath(t), "--apply", "--timeout", "1ms"}, CommandOptions{
		ErrOut: &stderr,
		RunBudgetInitialize: func(ctx context.Context, _ []byte, _ bool, _ io.Writer) error {
			<-ctx.Done()
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatalf("context error = %v", ctx.Err())
			}
			return ctx.Err()
		},
	})
	if code != 1 || !strings.Contains(stderr.String(), "deadline exceeded") {
		t.Fatalf("deadline code=%d error=%q", code, stderr.String())
	}
}
