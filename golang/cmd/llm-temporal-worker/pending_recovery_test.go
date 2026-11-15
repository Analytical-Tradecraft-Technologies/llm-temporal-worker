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

const (
	pendingTestID      = "llmtw_req_00000000-0000-4000-8000-000000000001"
	pendingOtherTestID = "llmtw_req_00000000-0000-4000-8000-000000000002"
)

func TestPendingListCommandIsReadOnlyAndBounded(t *testing.T) {
	path := exampleConfigPath(t)
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		args  []string
		stale time.Duration
	}{{nil, 24 * time.Hour}, {[]string{"--stale-after", "2h", "--timeout", "3s"}, 2 * time.Hour}} {
		var output, stderr bytes.Buffer
		called := false
		args := append([]string{"pending-list", "--config", path}, test.args...)
		code := Execute(context.Background(), args, CommandOptions{Out: &output, ErrOut: &stderr, RunPendingList: func(ctx context.Context, data []byte, staleAfter time.Duration, out io.Writer) error {
			called = true
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 10*time.Minute || !bytes.Equal(data, want) || staleAfter != test.stale {
				t.Fatalf("pending-list arguments changed: %v %v", staleAfter, deadline)
			}
			_, err := io.WriteString(out, `{"type":"summary"}`+"\n")
			return err
		}})
		if code != 0 || !called || stderr.Len() != 0 || !strings.Contains(output.String(), "summary") {
			t.Fatalf("code=%d called=%t stdout=%q stderr=%q", code, called, output.String(), stderr.String())
		}
	}
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"pending-list", "--config", path}, CommandOptions{ErrOut: &stderr, RunPendingList: func(context.Context, []byte, time.Duration, io.Writer) error {
		return errors.New("some pending requests could not be read or recovered; see their error codes, then rerun")
	}})
	if code != 1 || !strings.Contains(stderr.String(), "could not be read") {
		t.Fatalf("failure code=%d stderr=%q", code, stderr.String())
	}
}

func TestPendingRecoverCommandPassesExplicitTargetsAndApply(t *testing.T) {
	path := exampleConfigPath(t)
	for _, test := range []struct {
		args  []string
		ids   []string
		stale bool
		apply bool
	}{
		{[]string{"--request-id", pendingTestID}, []string{pendingTestID}, false, false},
		{[]string{"--request-id", pendingTestID, "--request-id", pendingOtherTestID, "--apply"}, []string{pendingTestID, pendingOtherTestID}, false, true},
		{[]string{"--stale", "--stale-after", "1h", "--apply", "--timeout", "5m"}, nil, true, true},
	} {
		called := false
		args := append([]string{"pending-recover", "--config", path}, test.args...)
		code := Execute(context.Background(), args, CommandOptions{RunPendingRecover: func(ctx context.Context, data []byte, ids []string, stale bool, staleAfter time.Duration, apply bool, _ io.Writer) error {
			called = true
			if _, ok := ctx.Deadline(); !ok || len(data) == 0 || strings.Join(ids, ",") != strings.Join(test.ids, ",") || stale != test.stale || apply != test.apply || staleAfter <= 0 {
				t.Fatalf("pending-recover arguments changed: %v %t %t %v", ids, stale, apply, staleAfter)
			}
			return nil
		}})
		if code != 0 || !called {
			t.Fatalf("%v: code=%d called=%t", test.args, code, called)
		}
	}
}

func TestPendingRecoverCommandRejectsInvalidInvocationsWithoutEchoingValues(t *testing.T) {
	path := exampleConfigPath(t)
	many := []string{}
	for range 101 {
		many = append(many, "--request-id", pendingTestID)
	}
	for _, extra := range [][]string{
		{},
		{"--stale", "--request-id", pendingTestID},
		{"--request-id", "private-value"},
		{"--request-id", pendingTestID, "extra"},
		{"--request-id", pendingTestID, "--timeout", "11m"},
		{"--stale", "--stale-after", "0s"},
		{"--stale", "--stale-after", "721h"},
		{"--private-flag"},
		many,
	} {
		var stderr bytes.Buffer
		args := append([]string{"pending-recover", "--config", path}, extra...)
		code := Execute(context.Background(), args, CommandOptions{ErrOut: &stderr, RunPendingRecover: func(context.Context, []byte, []string, bool, time.Duration, bool, io.Writer) error {
			t.Fatal("invalid invocation reached runtime")
			return nil
		}})
		if code != 2 || strings.Contains(stderr.String(), "private") || strings.TrimSpace(stderr.String()) == "" {
			t.Fatalf("%v: code=%d stderr=%q", extra[:min(len(extra), 4)], code, stderr.String())
		}
	}
}
