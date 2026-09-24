package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	redisclient "github.com/redis/go-redis/v9"
)

func TestAdmissionWireUsesLuaFieldNames(t *testing.T) {
	attempt := admission.AttemptFacts{RouteID: "route", ProviderRequestID: "request", AttemptNumber: 3}
	attemptData, err := encodeAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	attemptJSON := string(attemptData)
	if !strings.Contains(attemptJSON, `"route_id":"route"`) || strings.Contains(attemptJSON, `"RouteID"`) {
		t.Fatalf("attempt wire uses Go field names: %s", attemptJSON)
	}
	decodedAttempt, err := decodeAttempt(attemptData)
	if err != nil || decodedAttempt != attempt {
		t.Fatalf("attempt round trip = %#v, %v", decodedAttempt, err)
	}

	outcomeData, err := encodeOutcome(admission.AttemptOutcome{Certainty: admission.Rejected, Incurred: 7, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	outcomeJSON := string(outcomeData)
	for _, field := range []string{`"certainty":"rejected"`, `"incurred":"7"`, `"attempt":{"route_id":"route"`} {
		if !strings.Contains(outcomeJSON, field) {
			t.Fatalf("outcome wire missing Lua field %q: %s", field, outcomeJSON)
		}
	}
	decodedOutcome, err := decodeOutcome(outcomeData)
	if err != nil || decodedOutcome.Certainty != admission.Rejected || decodedOutcome.Incurred != 7 || decodedOutcome.Attempt != attempt {
		t.Fatalf("outcome round trip = %#v, %v", decodedOutcome, err)
	}
}

func TestEncodeAttemptEnforcesFunctionFieldBounds(t *testing.T) {
	tooLong := strings.Repeat("x", maxAttemptRouteBytes+1)
	if _, err := encodeAttempt(admission.AttemptFacts{RouteID: tooLong}); err == nil {
		t.Fatal("oversized route identifier accepted")
	}
	if _, err := encodeAttempt(admission.AttemptFacts{ProviderRequestID: strings.Repeat("x", maxAttemptRequestIDBytes+1)}); err == nil {
		t.Fatal("oversized provider request ID accepted")
	}
	if _, err := encodeAttempt(admission.AttemptFacts{ProviderRequestID: strings.Repeat("x", maxAttemptRequestIDBytes)}); err != nil {
		t.Fatalf("maximum provider request ID rejected: %v", err)
	}
	if _, err := encodeAttempt(admission.AttemptFacts{AttemptNumber: maxAttemptNumber}); err != nil {
		t.Fatalf("maximum persisted attempt number rejected: %v", err)
	}
	if _, err := encodeAttempt(admission.AttemptFacts{AttemptNumber: maxAttemptNumber + 1}); err == nil {
		t.Fatal("oversized attempt number accepted")
	}
}

func TestAdmissionFunctionLibraryAndLuaFallbackHaveExplicitDigests(t *testing.T) {
	library := AdmissionFunctionSource()
	functionDigest := sha256.Sum256([]byte(library))
	if got, want := AdmissionFunctionDigest(), hex.EncodeToString(functionDigest[:]); got != want {
		t.Fatalf("function digest = %q, want %q", got, want)
	}
	lua := AdmissionLuaSource()
	luaDigest := sha256.Sum256([]byte(lua))
	if got, want := AdmissionLuaDigest(), hex.EncodeToString(luaDigest[:]); got != want {
		t.Fatalf("Lua digest = %q, want %q", got, want)
	}
	if AdmissionLuaDigest() == AdmissionFunctionDigest() {
		t.Fatal("Function library and Lua compatibility source unexpectedly share a digest")
	}
}

func TestAdmissionInvocationModesNeverAutoLoadOrReplaceRedisCode(t *testing.T) {
	for _, test := range []struct {
		name string
		mode AdmissionMode
	}{
		{name: "function", mode: AdmissionModeFunction},
		{name: "lua", mode: AdmissionModeLua},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &functionInvocationClient{}
			invoker := redisInvoker{client: client, mode: test.mode, version: AdmissionFunctionVersion}
			if _, err := invoker.Run(context.Background(), AdmissionFunctionVersion, []string{"{admission}:key"}, "begin"); err != nil {
				t.Fatal(err)
			}
			if client.scriptLoadCalls != 0 || client.evalCalls != 0 {
				t.Fatalf("invocation mutated Redis code: SCRIPT LOAD=%d EVAL=%d", client.scriptLoadCalls, client.evalCalls)
			}
			switch test.mode {
			case AdmissionModeFunction:
				if client.fcallCalls != 1 || client.evalShaCalls != 0 {
					t.Fatalf("function invocation calls = FCALL %d EVALSHA %d", client.fcallCalls, client.evalShaCalls)
				}
			case AdmissionModeLua:
				if client.evalShaCalls != 1 || client.fcallCalls != 0 {
					t.Fatalf("Lua invocation calls = EVALSHA %d FCALL %d", client.evalShaCalls, client.fcallCalls)
				}
			}
		})
	}
}

type functionInvocationClient struct {
	fcallCalls      int
	evalCalls       int
	evalShaCalls    int
	scriptLoadCalls int
}

func (client *functionInvocationClient) Eval(ctx context.Context, _ string, _ []string, _ ...interface{}) *redisclient.Cmd {
	client.evalCalls++
	return functionInvocationReply(ctx)
}

func (client *functionInvocationClient) EvalSha(ctx context.Context, _ string, _ []string, _ ...interface{}) *redisclient.Cmd {
	client.evalShaCalls++
	return functionInvocationReply(ctx)
}

func (client *functionInvocationClient) EvalRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redisclient.Cmd {
	return functionInvocationReply(ctx)
}

func (client *functionInvocationClient) EvalShaRO(ctx context.Context, _ string, _ []string, _ ...interface{}) *redisclient.Cmd {
	return functionInvocationReply(ctx)
}

func (client *functionInvocationClient) ScriptExists(ctx context.Context, _ ...string) *redisclient.BoolSliceCmd {
	command := redisclient.NewBoolSliceCmd(ctx)
	command.SetVal([]bool{true})
	return command
}

func (client *functionInvocationClient) ScriptLoad(ctx context.Context, _ string) *redisclient.StringCmd {
	client.scriptLoadCalls++
	command := redisclient.NewStringCmd(ctx)
	command.SetVal("unexpected")
	return command
}

func (client *functionInvocationClient) FCall(ctx context.Context, _ string, _ []string, _ ...interface{}) *redisclient.Cmd {
	client.fcallCalls++
	return functionInvocationReply(ctx)
}

func functionInvocationReply(ctx context.Context) *redisclient.Cmd {
	command := redisclient.NewCmd(ctx)
	command.SetVal([]interface{}{"ok"})
	return command
}
