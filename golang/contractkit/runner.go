package contractkit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// Codec is the consumer codec under test. It decodes data as the named
// contract and returns its own encoding of the decoded value, or an error
// when it rejects data. fixture is the fixture file name, for diagnostics.
type Codec func(contract Contract, fixture string, data []byte) ([]byte, error)

// Result is the outcome of one fixture in RoundTrip.
type Result struct {
	Fixture  string
	Contract Contract
	Negative bool
	// Err is nil when the codec met the fixture's expectation.
	Err error
}

// Check validates bytes a caller emits for contract: they must satisfy the
// published schema and, for the contracts the fixtures cover, decode with
// the worker's own strict codec, which also rejects what a schema cannot
// express (duplicate keys, cross-field rules).
func (kit *Kit) Check(contract Contract, data []byte) error {
	if err := kit.Validate(contract, data); err != nil {
		return err
	}
	if _, ok := referenceCodecs[contract]; !ok {
		return nil
	}
	_, err := referenceEncoding(contract, data)
	return err
}

// CheckEncoding checks a consumer's encoding of the named positive fixture:
// it must pass Check and carry the same value as the fixture, which is
// judged by comparing the worker codec's re-encodings of the two.
func (kit *Kit) CheckEncoding(fixture string, encoded []byte) error {
	entry, ok := kit.fixture(fixture)
	if !ok {
		return fmt.Errorf("contract kit has no fixture %s", fixture)
	}
	if entry.Negative {
		return fmt.Errorf("fixture %s is negative and has no valid encoding", fixture)
	}
	if err := kit.Check(entry.Contract, encoded); err != nil {
		return err
	}
	want, err := referenceEncoding(entry.Contract, entry.Data)
	if err != nil {
		return fmt.Errorf("fixture %s: %w", fixture, err)
	}
	got, err := referenceEncoding(entry.Contract, encoded)
	if err != nil {
		return err
	}
	if err := sameJSONValue(entry.Contract, want, got); err != nil {
		return fmt.Errorf("encoding does not carry the fixture's value: %w", err)
	}
	return nil
}

// contractLeniency names, per contract, the closed contract members where
// two encodings may legitimately differ. Paths start at "$" and write every
// array index as [*]. Everywhere else, open JSON payloads included (tool
// arguments, JSON content values, metadata), values must be equal as JSON.
type leniency struct {
	// emptyEquivalent members are optional, and writing them as null or []
	// means the same as omitting them.
	emptyEquivalent map[string]bool
	// instants are RFC 3339 date-time members, equal when they name the
	// same instant however it is spelled.
	instants map[string]bool
}

func pathSet(paths ...string) map[string]bool {
	set := make(map[string]bool, len(paths))
	for _, path := range paths {
		set[path] = true
	}
	return set
}

var contractLeniency = map[Contract]leniency{
	GenerateResponse: {emptyEquivalent: pathSet("$.diagnostics")},
	CompactResponse:  {emptyEquivalent: pathSet("$.diagnostics")},
	QueryRequest: {
		emptyEquivalent: pathSet("$.query.cursor"),
		instants:        pathSet("$.query.active_at", "$.query.start_time", "$.query.end_time"),
	},
	QueryResponse: {
		emptyEquivalent: pathSet(
			"$.next_cursor",
			"$.result.models[*].display_name",
			"$.result.endpoints[*].confirmed_at",
			"$.result.windows[*].retry_after_seconds",
			"$.result.buckets[*].group.operation_kind",
			"$.result.buckets[*].group.provider",
			"$.result.buckets[*].group.model",
		),
		instants: pathSet(
			"$.observed_at",
			"$.stale_after",
			"$.result.active_at",
			"$.result.start_time",
			"$.result.end_time",
			"$.result.windows[*].coverage_start",
			"$.result.windows[*].coverage_end",
			"$.result.endpoints[*].confirmed_at",
		),
	},
}

// sameJSONValue compares the worker codec's re-encodings of a fixture and
// of a consumer's encoding of it. Two encodings carry the same value when
// they are equal as JSON (member order and number spelling carry no
// meaning) except where contractLeniency allows a difference.
func sameJSONValue(contract Contract, want, got []byte) error {
	decode := func(data []byte) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		err := decoder.Decode(&value)
		return value, err
	}
	left, err := decode(want)
	if err != nil {
		return err
	}
	right, err := decode(got)
	if err != nil {
		return err
	}
	return contractLeniency[contract].compare("$", "$", left, right)
}

// compare reports the first difference between want and got. location is
// the reported path; pattern is the same path with [*] for array indices.
func (rules leniency) compare(location, pattern string, want, got any) error {
	switch want := want.(type) {
	case map[string]any:
		got, ok := got.(map[string]any)
		if !ok {
			return fmt.Errorf("%s: want an object, got %s", location, describeJSON(got))
		}
		names := make([]string, 0, len(want))
		for name := range want {
			names = append(names, name)
		}
		for name := range got {
			if _, ok := want[name]; !ok {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			member, memberPattern := location+"."+name, pattern+"."+name
			left, inWant := want[name]
			right, inGot := got[name]
			optional := rules.emptyEquivalent[memberPattern]
			switch {
			case !inWant && !(optional && emptyJSON(right)):
				return fmt.Errorf("%s: unexpected member %s", member, describeJSON(right))
			case !inGot && !(optional && emptyJSON(left)):
				return fmt.Errorf("%s: missing, want %s", member, describeJSON(left))
			case inWant && inGot:
				if optional && emptyJSON(left) && emptyJSON(right) {
					continue
				}
				if err := rules.compare(member, memberPattern, left, right); err != nil {
					return err
				}
			}
		}
		return nil
	case []any:
		got, ok := got.([]any)
		if !ok || len(got) != len(want) {
			return fmt.Errorf("%s: want %s, got %s", location, describeJSON(want), describeJSON(got))
		}
		for index := range want {
			if err := rules.compare(fmt.Sprintf("%s[%d]", location, index), pattern+"[*]", want[index], got[index]); err != nil {
				return err
			}
		}
		return nil
	case json.Number:
		got, ok := got.(json.Number)
		if ok {
			left, leftOK := new(big.Rat).SetString(want.String())
			right, rightOK := new(big.Rat).SetString(got.String())
			if leftOK && rightOK && left.Cmp(right) == 0 {
				return nil
			}
		}
	case string:
		got, ok := got.(string)
		if ok && (got == want || rules.instants[pattern] && sameInstant(want, got)) {
			return nil
		}
	default:
		if want == got {
			return nil
		}
	}
	return fmt.Errorf("%s: want %s, got %s", location, describeJSON(want), describeJSON(got))
}

func emptyJSON(value any) bool {
	if value == nil {
		return true
	}
	array, ok := value.([]any)
	return ok && len(array) == 0
}

func sameInstant(left, right string) bool {
	leftTime, err := time.Parse(time.RFC3339Nano, left)
	if err != nil {
		return false
	}
	rightTime, err := time.Parse(time.RFC3339Nano, right)
	return err == nil && leftTime.Equal(rightTime)
}

func describeJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%T", value)
	}
	if len(data) > 200 {
		return string(data[:200]) + "..."
	}
	return string(data)
}

// RoundTrip runs codec over every fixture. A positive fixture passes when
// the codec accepts it and its encoding passes CheckEncoding; a negative
// fixture passes when the codec rejects it. Results are in fixture order.
func (kit *Kit) RoundTrip(codec Codec) []Result {
	results := make([]Result, 0, len(kit.fixtures))
	for _, fixture := range kit.fixtures {
		result := Result{Fixture: fixture.Name, Contract: fixture.Contract, Negative: fixture.Negative}
		encoded, err := codec(fixture.Contract, fixture.Name, bytes.Clone(fixture.Data))
		switch {
		case fixture.Negative && err == nil:
			result.Err = fmt.Errorf("codec accepted a negative fixture (expected a rejection about %q)", fixture.Field)
		case fixture.Negative:
		case err != nil:
			result.Err = fmt.Errorf("codec rejected a valid fixture: %w", err)
		default:
			result.Err = kit.CheckEncoding(fixture.Name, encoded)
		}
		results = append(results, result)
	}
	return results
}

// Failed returns the results that did not pass.
func Failed(results []Result) []Result {
	var failed []Result
	for _, result := range results {
		if result.Err != nil {
			failed = append(failed, result)
		}
	}
	return failed
}

func (kit *Kit) fixture(name string) (Fixture, bool) {
	for _, fixture := range kit.fixtures {
		if fixture.Name == name {
			return fixture, true
		}
	}
	return Fixture{}, false
}

// maxCommandOutput bounds what a command codec may write per fixture, and
// commandWaitDelay how long a timed-out run waits for its pipes to close.
const (
	maxCommandOutput = 16 << 20
	commandWaitDelay = 100 * time.Millisecond
)

// Command returns a Codec that runs an external codec once per fixture: the
// fixture bytes on standard input, LLMTW_CONTRACT and LLMTW_FIXTURE in the
// environment, and its encoding on standard output. A zero exit status
// accepts the fixture and any other status rejects it. Each run is bounded
// by timeout. This is how a codec in another language joins RoundTrip.
func Command(timeout time.Duration, name string, args ...string) Codec {
	return func(contract Contract, fixture string, data []byte) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		command := exec.CommandContext(ctx, name, args...)
		// On timeout, kill the codec's whole process group where the platform
		// has one, and stop waiting for output pipes a surviving descendant
		// still holds, so the timeout bounds the run.
		killProcessGroupOnCancel(command)
		command.WaitDelay = commandWaitDelay
		command.Env = append(os.Environ(), "LLMTW_CONTRACT="+string(contract), "LLMTW_FIXTURE="+fixture)
		command.Stdin = bytes.NewReader(data)
		var stdout, stderr limitedBuffer
		stdout.limit, stderr.limit = maxCommandOutput, 4096
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("codec command did not finish within %s", timeout)
		}
		if stdout.overflow {
			return nil, errors.New("codec command wrote more than 16 MiB")
		}
		if err != nil {
			message := strings.TrimSpace(stderr.String())
			if message == "" {
				message = err.Error()
			}
			return nil, errors.New(message)
		}
		return stdout.Bytes(), nil
	}
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	if room := buffer.limit - buffer.Len(); len(data) > room {
		buffer.overflow = true
		if room > 0 {
			buffer.Buffer.Write(data[:room])
		}
		return len(data), nil
	}
	return buffer.Buffer.Write(data)
}

var _ io.Writer = (*limitedBuffer)(nil)
