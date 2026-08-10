package redis

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	redisclient "github.com/redis/go-redis/v9"
)

type coldStartFake struct {
	mu          sync.Mutex
	libraries   []redisclient.Library
	functionErr error
	evalErr     error
	active      string
	manifest    string
	streamID    string
	streamEvent string
	windows     string
	evalCalls   int
	mutations   int
}

func healthyColdStartFake() *coldStartFake {
	return &coldStartFake{libraries: []redisclient.Library{
		{Name: AdmissionFunctionLibrary, Engine: "LUA", Code: AdmissionFunctionSource(), Functions: []redisclient.Function{{Name: AdmissionFunctionVersion}}},
		{Name: ThrottleFunctionLibrary, Engine: "LUA", Code: ThrottleFunctionSource(), Functions: []redisclient.Function{{Name: ThrottleFunctionVersion}}},
		{Name: BudgetStatusFunctionLibrary, Engine: "LUA", Code: BudgetStatusFunctionLibrarySource(), Functions: []redisclient.Function{{Name: BudgetStatusFunctionVersion}}},
	}}
}

func (fake *coldStartFake) FunctionList(ctx context.Context, _ redisclient.FunctionListQuery) *redisclient.FunctionListCmd {
	command := redisclient.NewFunctionListCmd(ctx)
	command.SetVal(fake.libraries)
	command.SetErr(fake.functionErr)
	return command
}

func (fake *coldStartFake) Eval(ctx context.Context, _ string, _ []string, args ...interface{}) *redisclient.Cmd {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.evalCalls++
	command := redisclient.NewCmd(ctx)
	if fake.evalErr != nil {
		command.SetErr(fake.evalErr)
		return command
	}
	manifest, pointer, event, streamID, windows := args[0].(string), args[1].(string), args[2].(string), args[3].(string), args[4].(string)
	allAbsent := fake.active == "" && fake.manifest == "" && fake.streamID == "" && fake.streamEvent == "" && fake.windows == ""
	if allAbsent {
		fake.manifest, fake.active, fake.streamID, fake.streamEvent, fake.windows = manifest, pointer, streamID, event, windows
		fake.mutations += 4
		command.SetVal(int64(1))
		return command
	}
	if fake.active == pointer && fake.manifest == manifest && fake.streamID == streamID && fake.streamEvent == event && fake.windows == windows {
		command.SetVal(int64(2))
		return command
	}
	command.SetVal(int64(0))
	return command
}

func coldStartManifest(t *testing.T) BudgetManifest {
	t.Helper()
	manifest := testBudgetManifest(t)
	manifest.StreamHighWaterMark = BudgetColdStartStreamID
	manifest.JournalHighWaterMark = 0
	for index := range manifest.Members {
		manifest.Members[index].LimitNanoUSD = "1000000000"
	}
	catalog, err := MemberCatalogDigest(manifest.Members)
	if err != nil {
		t.Fatal(err)
	}
	manifest.MemberCatalogDigest = catalog
	return manifest
}

func coldStartKeys(t *testing.T) BudgetKeySpace {
	t.Helper()
	keys, err := NewBudgetKeySpace(KeyOptions{Prefix: "worker", HashTag: "admission", KeySecret: []byte(strings.Repeat("k", 32))})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestBootstrapBudgetColdStartCreatesAbsentStateAndReplaysExactly(t *testing.T) {
	client := healthyColdStartFake()
	options := BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: coldStartManifest(t)}
	first, err := BootstrapBudgetColdStart(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BootstrapBudgetColdStart(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.Schema != BudgetColdStartReceiptSchema || len(first.ReceiptID) != 64 {
		t.Fatalf("receipts first=%#v second=%#v", first, second)
	}
	if client.mutations != 4 || client.evalCalls != 2 {
		t.Fatalf("mutations=%d evals=%d, want 4 and 2", client.mutations, client.evalCalls)
	}
}

func TestBootstrapBudgetColdStartConcurrentCallsHaveOneCreate(t *testing.T) {
	client := healthyColdStartFake()
	options := BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: coldStartManifest(t)}
	const callers = 16
	receipts := make(chan BudgetColdStartReceipt, callers)
	errorsOut := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			receipt, err := BootstrapBudgetColdStart(context.Background(), options)
			receipts <- receipt
			errorsOut <- err
		}()
	}
	wait.Wait()
	close(receipts)
	close(errorsOut)
	for err := range errorsOut {
		if err != nil {
			t.Fatal(err)
		}
	}
	var want BudgetColdStartReceipt
	for receipt := range receipts {
		if want == (BudgetColdStartReceipt{}) {
			want = receipt
		} else if receipt != want {
			t.Fatalf("concurrent receipt=%#v, want %#v", receipt, want)
		}
	}
	if client.mutations != 4 || client.evalCalls != callers {
		t.Fatalf("mutations=%d evals=%d", client.mutations, client.evalCalls)
	}
}

func TestBootstrapBudgetColdStartRejectsEveryPartialOrMismatchedStateWithoutMutation(t *testing.T) {
	manifest := coldStartManifest(t)
	canonical, pointer, event, windows, _, err := canonicalColdStartState(manifest)
	if err != nil {
		t.Fatal(err)
	}
	states := []struct {
		name                               string
		active, manifest, id, evt, windows string
	}{
		{name: "pointer only", active: string(pointer)},
		{name: "manifest only", manifest: string(canonical)},
		{name: "stream only", id: BudgetColdStartStreamID, evt: string(event)},
		{name: "windows only", windows: string(windows)},
		{name: "generation mismatch", active: `{"generation_id":"other"}`, manifest: string(canonical), id: BudgetColdStartStreamID, evt: string(event), windows: string(windows)},
		{name: "manifest tampered", active: string(pointer), manifest: string(canonical) + " ", id: BudgetColdStartStreamID, evt: string(event), windows: string(windows)},
		{name: "incarnation mismatch", active: strings.Replace(string(pointer), "redis-incarnation-1", "redis-incarnation-2", 1), manifest: string(canonical), id: BudgetColdStartStreamID, evt: string(event), windows: string(windows)},
		{name: "stream tampered", active: string(pointer), manifest: string(canonical), id: BudgetColdStartStreamID, evt: string(event) + " ", windows: string(windows)},
		{name: "window tampered", active: string(pointer), manifest: string(canonical), id: BudgetColdStartStreamID, evt: string(event), windows: string(windows) + " "},
	}
	for _, state := range states {
		t.Run(state.name, func(t *testing.T) {
			client := healthyColdStartFake()
			client.active, client.manifest, client.streamID, client.streamEvent, client.windows = state.active, state.manifest, state.id, state.evt, state.windows
			before := []string{client.active, client.manifest, client.streamID, client.streamEvent, client.windows}
			_, err := BootstrapBudgetColdStart(context.Background(), BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: manifest})
			if !errors.Is(err, ErrBudgetColdStartStateConflict) {
				t.Fatalf("error=%v, want conflict", err)
			}
			after := []string{client.active, client.manifest, client.streamID, client.streamEvent, client.windows}
			if !reflect.DeepEqual(before, after) || client.mutations != 0 {
				t.Fatalf("failure mutated state before=%q after=%q", before, after)
			}
		})
	}
}

func TestBootstrapBudgetColdStartRejectsFunctionDigestBeforeMutation(t *testing.T) {
	client := healthyColdStartFake()
	client.libraries[1].Code += "--tampered"
	_, err := BootstrapBudgetColdStart(context.Background(), BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: coldStartManifest(t)})
	if !errors.Is(err, ErrBudgetColdStartFunctionMismatch) {
		t.Fatalf("error=%v, want function mismatch", err)
	}
	if client.evalCalls != 0 || client.mutations != 0 {
		t.Fatalf("function failure evals=%d mutations=%d", client.evalCalls, client.mutations)
	}
}

func TestVerifyBudgetColdStartFunctionsRejectsNonExactMetadataWithoutMutation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*coldStartFake)
	}{
		{name: "missing library", change: func(client *coldStartFake) {
			client.libraries = client.libraries[:2]
		}},
		{name: "duplicate library", change: func(client *coldStartFake) {
			client.libraries = append(client.libraries, client.libraries[0])
		}},
		{name: "wrong engine", change: func(client *coldStartFake) {
			client.libraries[0].Engine = "other"
		}},
		{name: "wrong version", change: func(client *coldStartFake) {
			client.libraries[0].Functions[0].Name = "admission_v2"
		}},
		{name: "extra function", change: func(client *coldStartFake) {
			client.libraries[0].Functions = append(client.libraries[0].Functions, redisclient.Function{Name: "unexpected"})
		}},
		{name: "function flags", change: func(client *coldStartFake) {
			client.libraries[0].Functions[0].Flags = []string{"no-writes"}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client := healthyColdStartFake()
			test.change(client)
			err := VerifyBudgetColdStartFunctions(context.Background(), client)
			if !errors.Is(err, ErrBudgetColdStartFunctionMismatch) {
				t.Fatalf("error=%v, want function mismatch", err)
			}
			if client.evalCalls != 0 || client.mutations != 0 {
				t.Fatalf("function failure evals=%d mutations=%d", client.evalCalls, client.mutations)
			}
		})
	}
}

func TestBootstrapBudgetColdStartTransportTimeoutDoesNotMutate(t *testing.T) {
	client := healthyColdStartFake()
	client.functionErr = context.DeadlineExceeded
	_, err := BootstrapBudgetColdStart(context.Background(), BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: coldStartManifest(t)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want deadline", err)
	}
	if client.evalCalls != 0 || client.mutations != 0 {
		t.Fatalf("timeout evals=%d mutations=%d", client.evalCalls, client.mutations)
	}
}

func TestBootstrapBudgetColdStartMutationTimeoutReturnsNoReceipt(t *testing.T) {
	client := healthyColdStartFake()
	client.evalErr = context.DeadlineExceeded
	receipt, err := BootstrapBudgetColdStart(context.Background(), BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: coldStartManifest(t)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want deadline", err)
	}
	if receipt != (BudgetColdStartReceipt{}) || client.evalCalls != 1 || client.mutations != 0 {
		t.Fatalf("timeout receipt=%#v evals=%d mutations=%d", receipt, client.evalCalls, client.mutations)
	}
}

func TestBootstrapBudgetColdStartRequiresImmutableMemberBudgetsBeforeRedis(t *testing.T) {
	client := healthyColdStartFake()
	manifest := coldStartManifest(t)
	manifest.Members[0].LimitNanoUSD = ""
	catalog, err := MemberCatalogDigest(manifest.Members)
	if err != nil {
		t.Fatal(err)
	}
	manifest.MemberCatalogDigest = catalog
	_, err = BootstrapBudgetColdStart(context.Background(), BudgetColdStartOptions{Client: client, Keys: coldStartKeys(t), Manifest: manifest})
	if !errors.Is(err, ErrBudgetManifestInvalid) {
		t.Fatalf("error=%v, want invalid manifest", err)
	}
	if client.evalCalls != 0 || client.mutations != 0 {
		t.Fatalf("invalid budget evals=%d mutations=%d", client.evalCalls, client.mutations)
	}
}

func TestValidateBudgetColdStartManifestRequiresInitialWatermarks(t *testing.T) {
	cases := []struct {
		name   string
		change func(*BudgetManifest)
	}{
		{name: "stream", change: func(manifest *BudgetManifest) {
			manifest.StreamHighWaterMark = "1-1"
		}},
		{name: "journal", change: func(manifest *BudgetManifest) {
			manifest.JournalHighWaterMark = 1
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manifest := coldStartManifest(t)
			test.change(&manifest)
			if err := ValidateBudgetColdStartManifest(manifest); !errors.Is(err, ErrBudgetManifestInvalid) {
				t.Fatalf("error=%v, want invalid manifest", err)
			}
		})
	}
}
