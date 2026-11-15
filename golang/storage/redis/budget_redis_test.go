package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	redisclient "github.com/redis/go-redis/v9"
)

type fakeBudgetGenerationRedis struct {
	values   map[string]string
	getErr   error
	evalVal  interface{}
	evalErr  error
	evalKeys []string
	evalArgs []interface{}
	script   string
}

func (fake *fakeBudgetGenerationRedis) Get(_ context.Context, key string) *redisclient.StringCmd {
	if fake.getErr != nil {
		return redisclient.NewStringResult("", fake.getErr)
	}
	value, ok := fake.values[key]
	if !ok {
		return redisclient.NewStringResult("", redisclient.Nil)
	}
	return redisclient.NewStringResult(value, nil)
}

func (fake *fakeBudgetGenerationRedis) Eval(_ context.Context, script string, keys []string, args ...interface{}) *redisclient.Cmd {
	fake.script = script
	fake.evalKeys = append([]string(nil), keys...)
	fake.evalArgs = append([]interface{}(nil), args...)
	return redisclient.NewCmdResult(fake.evalVal, fake.evalErr)
}

type fakeBudgetEventRedis struct {
	appendArgs   []interface{}
	appendKeys   []string
	appendScript string
	appendID     string
	appendErr    error
	infoValue    *redisclient.XInfoStream
	infoErr      error
	readArgs     *redisclient.XReadArgs
	readValue    []redisclient.XStream
	readErr      error
}

func (fake *fakeBudgetEventRedis) Eval(_ context.Context, script string, keys []string, args ...interface{}) *redisclient.Cmd {
	fake.appendArgs = args
	fake.appendKeys = keys
	fake.appendScript = script
	return redisclient.NewCmdResult(fake.appendID, fake.appendErr)
}

func (fake *fakeBudgetEventRedis) XInfoStream(_ context.Context, _ string) *redisclient.XInfoStreamCmd {
	cmd := redisclient.NewXInfoStreamCmd(context.Background(), "budget:events")
	cmd.SetVal(fake.infoValue)
	cmd.SetErr(fake.infoErr)
	return cmd
}

func (fake *fakeBudgetEventRedis) XRead(_ context.Context, args *redisclient.XReadArgs) *redisclient.XStreamSliceCmd {
	copyArgs := *args
	copyArgs.Streams = append([]string(nil), args.Streams...)
	fake.readArgs = &copyArgs
	return redisclient.NewXStreamSliceCmdResult(fake.readValue, fake.readErr)
}

func testBudgetKeySpaceForAdapter(t *testing.T) BudgetKeySpace {
	t.Helper()
	return mustCall(t, NewBudgetKeySpace, KeyOptions{Prefix: "worker", HashTag: "budget", KeySecret: []byte(strings.Repeat("k", 32))})
}

// testBudgetKeyOptions are the "test" prefix options under hashTag.
func testBudgetKeyOptions(hashTag string) KeyOptions {
	return KeyOptions{Prefix: "test", HashTag: hashTag, KeySecret: []byte(strings.Repeat("s", 32))}
}

func newFakeBudgetGenerationPort(t *testing.T, fake *fakeBudgetGenerationRedis, keys BudgetKeySpace) *RedisBudgetGenerationPort {
	t.Helper()
	port, err := NewRedisBudgetGenerationPort(fake, keys)
	testassert.NoError(t, err)
	return port
}

func newFakeBudgetEventPort(t *testing.T, fake *fakeBudgetEventRedis, keys BudgetKeySpace) *RedisBudgetEventPort {
	t.Helper()
	port := mustBudgetEventPort(t, fake, keys)
	return port
}

func TestRedisBudgetGenerationPortPublishesAtomicallyAndLoadsPointer(t *testing.T) {
	keys := testBudgetKeySpaceForAdapter(t)
	fake := &fakeBudgetGenerationRedis{evalVal: int64(1), values: make(map[string]string)}
	port := newFakeBudgetGenerationPort(t, fake, keys)
	manifest := testBudgetManifest(t)
	pointer, err := port.PublishGeneration(context.Background(), manifest)
	testassert.NoError(t, err, "PublishGeneration")
	if pointer.GenerationID != manifest.GenerationID || len(fake.evalKeys) != 2 || fake.evalKeys[0] != keys.ActiveGenerationKey() || fake.evalKeys[1] != keys.ManifestKey(manifest.GenerationID) {
		t.Fatalf("publish keys/pointer = %#v, %#v", fake.evalKeys, pointer)
	}
	if !strings.Contains(fake.script, "redis.call('GET', KEYS[2])") || !strings.Contains(fake.script, "redis.call('SET', KEYS[1], ARGV[2])") {
		t.Fatal("publish does not use an atomic manifest/pointer script")
	}
	canonical, err := manifest.Canonical()
	testassert.NoError(t, err)
	if got, ok := fake.evalArgs[0].(string); !ok || got != string(canonical) {
		t.Fatal("publish did not pass canonical manifest bytes")
	}
	pointerJSON, err := json.Marshal(pointer)
	testassert.NoError(t, err)
	if got, ok := fake.evalArgs[1].(string); !ok || got != string(pointerJSON) {
		t.Fatal("publish did not pass canonical pointer bytes")
	}

	fake.values[keys.ActiveGenerationKey()] = string(pointerJSON)
	fake.values[keys.ManifestKey(manifest.GenerationID)] = string(canonical)
	active, err := port.ActiveGeneration(context.Background())
	if err != nil || active != pointer {
		t.Fatalf("ActiveGeneration = %#v, %v", active, err)
	}
	loaded, err := port.LoadManifest(context.Background(), pointer)
	if err != nil || loaded.GenerationID != manifest.GenerationID {
		t.Fatalf("LoadManifest = %#v, %v", loaded, err)
	}
	fake.values[keys.ManifestKey(manifest.GenerationID)] = " \n" + string(canonical)
	testassert.ErrorIs(t, testassert.Err(port.LoadManifest(context.Background(), pointer)), ErrBudgetManifestInvalid, "non-canonical manifest error =")
}

func TestRedisBudgetGenerationPortRejectsImmutableConflictAndMalformedState(t *testing.T) {
	keys := testBudgetKeySpaceForAdapter(t)
	fake := &fakeBudgetGenerationRedis{evalVal: int64(0), values: make(map[string]string)}
	port := newFakeBudgetGenerationPort(t, fake, keys)
	testassert.ErrorIs(t, testassert.Err(port.PublishGeneration(context.Background(), testBudgetManifest(t))), ErrBudgetGenerationConflict, "conflict error =")
	fake.values[keys.ActiveGenerationKey()] = `{"generation_id":"g","incarnation_id":"i","manifest_digest":"bad"}`
	testassert.ErrorIs(t, testassert.Err(port.ActiveGeneration(context.Background())), ErrBudgetManifestInvalid, "malformed pointer error =")
	fake.values[keys.ActiveGenerationKey()] = ""
	testassert.ErrorIs(t, testassert.Err(port.ActiveGeneration(context.Background())), ErrBudgetManifestInvalid, "empty pointer error =")
}

func TestRedisBudgetEventPortAppendsAndReadsBroadcastCursor(t *testing.T) {
	keys := testBudgetKeySpaceForAdapter(t)
	fake := &fakeBudgetEventRedis{appendID: "42-7"}
	port := newFakeBudgetEventPort(t, fake, keys)
	event := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh, GenerationID: "generation", Revision: 2, OccurredAt: time.Unix(1, 0).UTC()}
	id, err := port.Append(context.Background(), event)
	if err != nil || id != "42-7" {
		t.Fatalf("Append = %q, %v", id, err)
	}
	if len(fake.appendKeys) != 1 || fake.appendKeys[0] != keys.EventsKey() || fake.appendScript != budgetHintAppendScript || len(fake.appendArgs) != 1 {
		t.Fatalf("atomic hint publication = %#v, %#v", fake.appendKeys, fake.appendArgs)
	}
	payload, ok := fake.appendArgs[0].(string)
	if !ok || !strings.Contains(payload, `"generation_id":"generation"`) {
		t.Fatalf("hint payload = %#v", fake.appendArgs)
	}

	fake.infoValue = &redisclient.XInfoStream{FirstEntry: redisclient.XMessage{ID: "42-7"}, LastEntry: redisclient.XMessage{ID: "42-8"}}
	current, err := port.CurrentCursor(context.Background())
	if err != nil || current != "42-8" {
		t.Fatalf("CurrentCursor = %q, %v", current, err)
	}
	fake.readValue = []redisclient.XStream{{Stream: keys.EventsKey(), Messages: []redisclient.XMessage{{ID: "42-8", Values: map[string]interface{}{budgetStreamEventField: payload}}}}}
	rows, err := port.Read(context.Background(), "42-7", 10)
	if err != nil || len(rows) != 1 || rows[0].ID != "42-8" || rows[0].Event.GenerationID != "generation" {
		t.Fatalf("Read = %#v, %v", rows, err)
	}
	if fake.readArgs == nil || fake.readArgs.ID != "42-7" || fake.readArgs.Block != -1 || fake.readArgs.Count != 10 {
		t.Fatalf("XREAD args = %#v", fake.readArgs)
	}
}

func TestRedisBudgetEventPortHandlesEmptyAndCorruptStreamsFailClosed(t *testing.T) {
	keys := testBudgetKeySpaceForAdapter(t)
	fake := &fakeBudgetEventRedis{readErr: redisclient.Nil}
	port := newFakeBudgetEventPort(t, fake, keys)
	rows, err := port.Read(context.Background(), "", 1)
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty stream = %#v, %v", rows, err)
	}
	fake.readErr = nil
	fake.readValue = []redisclient.XStream{{Stream: keys.EventsKey(), Messages: []redisclient.XMessage{{ID: "1-0", Values: map[string]interface{}{budgetStreamEventField: "{}"}}}}}
	fake.infoValue = &redisclient.XInfoStream{FirstEntry: redisclient.XMessage{ID: "1-0"}}
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "", 1)), ErrBudgetStreamInvalid, "corrupt event error =")
	fake.readValue = []redisclient.XStream{{Stream: keys.EventsKey(), Messages: []redisclient.XMessage{{ID: "1-0", Values: map[string]interface{}{budgetStreamEventField: "{}"}}}}}
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "1-0", 1)), ErrBudgetStreamInvalid, "non-advancing event error =")
	fake.infoValue = &redisclient.XInfoStream{FirstEntry: redisclient.XMessage{ID: "2-0"}}
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "1-0", 1)), ErrBudgetStreamGap, "trimmed stream gap error =")
	validEvent := BudgetStreamEvent{Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh, GenerationID: "generation", OccurredAt: time.Unix(1, 0).UTC()}
	validPayload, err := validEvent.Marshal()
	testassert.NoError(t, err)
	fake.readValue = []redisclient.XStream{{Stream: keys.EventsKey(), Messages: []redisclient.XMessage{
		{ID: "2-0", Values: map[string]interface{}{budgetStreamEventField: string(validPayload)}},
		{ID: "1-9", Values: map[string]interface{}{budgetStreamEventField: string(validPayload)}},
	}}}
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "", 10)), ErrBudgetStreamInvalid, "non-monotonic event error =")
}

func TestRedisBudgetEventPortRecoversAfterEviction(t *testing.T) {
	fake := &fakeBudgetEventRedis{infoErr: errors.New("ERR no such key"), readErr: redisclient.Nil}
	port := newFakeBudgetEventPort(t, fake, testBudgetKeySpaceForAdapter(t))
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "42-7", 10)), ErrBudgetStreamGap, "eviction must trigger authoritative reload")
	if cursor, err := port.CurrentCursor(context.Background()); err != nil || cursor != "0-0" {
		t.Fatalf("absent stream checkpoint = %q, %v", cursor, err)
	}
	if rows, err := port.Read(context.Background(), "0-0", 10); err != nil || len(rows) != 0 {
		t.Fatalf("poll after reload = %#v, %v", rows, err)
	}
	fake.infoErr = errors.New("connection reset")
	if _, err := port.Read(context.Background(), "0-0", 10); err == nil {
		t.Fatal("zero cursor ignored transport failure")
	}
	if _, err := port.CurrentCursor(context.Background()); err == nil {
		t.Fatal("transport error accepted as eviction")
	}
}

func TestRedisBudgetEventPortZeroCursorDetectsDiscardedHistory(t *testing.T) {
	for _, test := range []struct {
		name          string
		added, length int64
		deleted       string
		gap           bool
	}{
		{"untrimmed", 1, 1, "0-0", false},
		{"new empty", 0, 0, "0-0", false},
		{"trimmed prefix", 12000, 10000, "0-0", true},
		{"deleted entry", 2, 1, "42-0", true},
		{"all trimmed", 1, 0, "0-0", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeBudgetEventRedis{infoValue: &redisclient.XInfoStream{
				Length: test.length, EntriesAdded: test.added, MaxDeletedEntryID: test.deleted,
				FirstEntry: redisclient.XMessage{ID: "42-1"}, LastEntry: redisclient.XMessage{ID: "42-1"},
			}, readErr: redisclient.Nil}
			if test.length == 0 {
				fake.infoValue.FirstEntry = redisclient.XMessage{}
				fake.infoValue.LastEntry = redisclient.XMessage{}
			}
			port := newFakeBudgetEventPort(t, fake, testBudgetKeySpaceForAdapter(t))
			_, err := port.Read(context.Background(), "0-0", 10)
			if errors.Is(err, ErrBudgetStreamGap) != test.gap || (err != nil && !test.gap) {
				t.Fatalf("Read zero cursor = %v; want gap=%v", err, test.gap)
			}
			if test.gap && fake.readArgs != nil {
				t.Fatal("discarded history was read without reloading")
			}
		})
	}
}

func TestRedisBudgetEventPortRecoversEmptyTrimmedStream(t *testing.T) {
	fake := &fakeBudgetEventRedis{infoValue: &redisclient.XInfoStream{EntriesAdded: 10, LastGeneratedID: "42-7"}}
	port := newFakeBudgetEventPort(t, fake, testBudgetKeySpaceForAdapter(t))
	testassert.ErrorIs(t, testassert.Err(port.Read(context.Background(), "0-0", 10)), ErrBudgetStreamGap, "missing hints did not reload")
	cursor, err := port.CurrentCursor(context.Background())
	if err != nil || cursor != "42-7" {
		t.Fatalf("empty stream cursor = %q, %v", cursor, err)
	}
	if rows, err := port.Read(context.Background(), cursor, 10); err != nil || len(rows) != 0 {
		t.Fatalf("recovery repeated the gap: %#v, %v", rows, err)
	}
}
