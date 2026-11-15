package contractkit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// Fixture is one shared v1 fixture.
type Fixture struct {
	// Name is the fixture file name, for example "generate-root.json".
	Name string
	// Contract is the schema and codec the fixture is written for.
	Contract Contract
	// Negative fixtures must be rejected; every other fixture must decode
	// and round-trip.
	Negative bool
	// Field is, for a negative fixture, the field negative-fixtures.tsv
	// says the rejection is about. The in-repository Go and OCaml codecs
	// both name it in their error; other consumers may use it as a hint.
	Field string
	// Data is the fixture's bytes.
	Data []byte
}

// NegativeFixture is one negative-fixtures.tsv row.
type NegativeFixture struct {
	Contract Contract
	Field    string
}

// MediaURLCase is one media-url-policy.tsv row: a remote-media URL exactly
// as a caller would send it and whether the worker's policy blocks it.
type MediaURLCase struct {
	Blocked bool
	URL     string
}

// Fixtures returns every fixture in name order.
func (kit *Kit) Fixtures() []Fixture {
	fixtures := make([]Fixture, len(kit.fixtures))
	for index, fixture := range kit.fixtures {
		fixture.Data = bytes.Clone(fixture.Data)
		fixtures[index] = fixture
	}
	return fixtures
}

// NegativeManifest returns the parsed negative-fixtures.tsv.
func (kit *Kit) NegativeManifest() map[string]NegativeFixture {
	manifest := make(map[string]NegativeFixture, len(kit.negatives))
	for name, entry := range kit.negatives {
		manifest[name] = entry
	}
	return manifest
}

// MediaURLPolicy returns the parsed media-url-policy.tsv cases in file order.
func (kit *Kit) MediaURLPolicy() ([]MediaURLCase, error) {
	data, ok := kit.files[path.Join(FixtureDir, MediaURLPolicyName)]
	if !ok {
		return nil, fmt.Errorf("contract kit has no %s", MediaURLPolicyName)
	}
	var cases []MediaURLCase
	for number, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		verdict, url, found := strings.Cut(line, "\t")
		if !found || url == "" || (verdict != "blocked" && verdict != "allowed") {
			return nil, fmt.Errorf("%s line %d is not blocked|allowed<TAB>url", MediaURLPolicyName, number+1)
		}
		cases = append(cases, MediaURLCase{Blocked: verdict == "blocked", URL: url})
	}
	return cases, nil
}

// classifyFixtures assigns every fixture its contract: negative fixtures
// from negative-fixtures.tsv, which must list each of them exactly once,
// and positive fixtures by their name prefix.
func (kit *Kit) classifyFixtures() error {
	manifest, err := parseNegativeManifest(kit.files)
	if err != nil {
		return err
	}
	kit.negatives = manifest
	listed := map[string]bool{}
	for name, data := range kit.files {
		directory, file := path.Split(name)
		if directory != FixtureDir+"/" || !strings.HasSuffix(file, ".json") {
			continue
		}
		fixture := Fixture{Name: file, Data: data}
		if strings.HasPrefix(file, "negative-") {
			entry, ok := manifest[file]
			if !ok {
				return fmt.Errorf("negative fixture %s is not listed in %s", file, NegativeManifestName)
			}
			listed[file] = true
			fixture.Negative, fixture.Contract, fixture.Field = true, entry.Contract, entry.Field
		} else if fixture.Contract = positiveContract(file); fixture.Contract == "" {
			return fmt.Errorf("fixture %s does not name a v1 contract", file)
		}
		if _, ok := referenceCodecs[fixture.Contract]; !ok {
			return fmt.Errorf("fixture %s names contract %q, which has no reference codec", file, fixture.Contract)
		}
		kit.fixtures = append(kit.fixtures, fixture)
	}
	for name := range manifest {
		if !listed[name] {
			return fmt.Errorf("%s lists %s, which is not a negative fixture of the kit", NegativeManifestName, name)
		}
	}
	if len(kit.fixtures) == 0 {
		return errors.New("contract kit has no fixtures")
	}
	sort.Slice(kit.fixtures, func(left, right int) bool { return kit.fixtures[left].Name < kit.fixtures[right].Name })
	return nil
}

// parseNegativeManifest parses negative-fixtures.tsv: one
// fixture<TAB>contract<TAB>field line per negative fixture, # comments.
func parseNegativeManifest(files map[string][]byte) (map[string]NegativeFixture, error) {
	data, ok := files[path.Join(FixtureDir, NegativeManifestName)]
	if !ok {
		return nil, fmt.Errorf("contract kit has no %s", NegativeManifestName)
	}
	manifest := map[string]NegativeFixture{}
	for number, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		columns := strings.Split(line, "\t")
		if len(columns) != 3 || columns[0] == "" || columns[1] == "" || columns[2] == "" {
			return nil, fmt.Errorf("%s line %d is not fixture<TAB>contract<TAB>field", NegativeManifestName, number+1)
		}
		if _, duplicate := manifest[columns[0]]; duplicate {
			return nil, fmt.Errorf("%s lists %s twice", NegativeManifestName, columns[0])
		}
		manifest[columns[0]] = NegativeFixture{Contract: Contract(columns[1]), Field: columns[2]}
	}
	return manifest, nil
}

// positiveContract maps a positive fixture name to its contract.
func positiveContract(name string) Contract {
	switch {
	case strings.HasPrefix(name, "generate-response"):
		return GenerateResponse
	case strings.HasPrefix(name, "compact-response"):
		return CompactResponse
	case strings.HasPrefix(name, "generate-"):
		return GenerateRequest
	case strings.HasPrefix(name, "compact-"):
		return CompactRequest
	case strings.HasPrefix(name, "query-") && strings.HasSuffix(name, "-response.json"):
		return QueryResponse
	case strings.HasPrefix(name, "query-"):
		return QueryRequest
	}
	return ""
}

// referenceCodecs are the worker's own strict decoders for the contracts
// the fixtures cover.
var referenceCodecs = map[Contract]func() any{
	GenerateRequest:  func() any { return new(llm.GenerateRequestV1) },
	GenerateResponse: func() any { return new(llm.GenerateResponseV1) },
	CompactRequest:   func() any { return new(llm.CompactRequestV1) },
	CompactResponse:  func() any { return new(llm.CompactResponseV1) },
	QueryRequest:     func() any { return new(llm.QueryRequestV1) },
	QueryResponse:    func() any { return new(llm.QueryResponseV1) },
}

// referenceEncoding decodes data with the worker's codec and returns the
// worker's own encoding of it, the canonical form two encodings of one
// value share.
func referenceEncoding(contract Contract, data []byte) ([]byte, error) {
	target, ok := referenceCodecs[contract]
	if !ok {
		return nil, fmt.Errorf("contract kit has no reference codec for %q", contract)
	}
	value := target()
	if err := json.Unmarshal(data, value); err != nil {
		return nil, fmt.Errorf("worker codec rejects it: %w", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("worker codec cannot re-encode it: %w", err)
	}
	return encoded, nil
}
