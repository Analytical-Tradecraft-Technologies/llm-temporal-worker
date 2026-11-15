package contractkit

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Contract names one v1 schema: the schema file name without
// ".schema.json", for example "generate-request".
type Contract string

// The contracts with shared fixtures and a reference Go codec. Every other
// schema in the kit (activity, execution and config documents) can still be
// named in Validate.
const (
	GenerateRequest  Contract = "generate-request"
	GenerateResponse Contract = "generate-response"
	CompactRequest   Contract = "compact-request"
	CompactResponse  Contract = "compact-response"
	QueryRequest     Contract = "query-request"
	QueryResponse    Contract = "query-response"
)

// maxValidatedBytes bounds one validated document.
const maxValidatedBytes = 16 << 20

type schemaSet struct {
	compiled map[Contract]*jsonschema.Schema
}

// compileSchemas compiles every schema file of the kit. Each schema is
// registered under its $id, and also under the file name relative to that
// $id, which is how the activity schemas reference the documents they wrap.
func compileSchemas(files map[string][]byte) (*schemaSet, error) {
	compiler := jsonschema.NewCompiler()
	documents := map[string]any{}
	ids := map[Contract]string{}
	for name, data := range files {
		directory, file := path.Split(name)
		if directory != SchemaDir+"/" {
			continue
		}
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("schema %s is not JSON: %w", file, err)
		}
		object, _ := document.(map[string]any)
		id, _ := object["$id"].(string)
		if id == "" {
			return nil, fmt.Errorf("schema %s has no $id", file)
		}
		contract := Contract(strings.TrimSuffix(file, ".schema.json"))
		ids[contract] = id
		documents[id] = document
		if base := strings.LastIndex(id, "/"); base >= 0 && strings.Contains(id, "://") {
			if alias := id[:base+1] + file; alias != id {
				documents[alias] = document
			}
		}
	}
	compiler.UseLoader(kitLoader(documents))
	set := &schemaSet{compiled: map[Contract]*jsonschema.Schema{}}
	for contract, id := range ids {
		compiled, err := compiler.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("schema %s does not compile: %w", contract, err)
		}
		set.compiled[contract] = compiled
	}
	return set, nil
}

// kitLoader resolves schema references only to kit documents, so
// validation never reaches the network or the file system.
type kitLoader map[string]any

func (loader kitLoader) Load(url string) (any, error) {
	if document, ok := loader[url]; ok {
		return document, nil
	}
	return nil, fmt.Errorf("schema reference %s is not part of the contract kit", url)
}

// Contracts lists every contract the kit has a schema for.
func (kit *Kit) Contracts() []Contract {
	contracts := make([]Contract, 0, len(kit.schemas.compiled))
	for contract := range kit.schemas.compiled {
		contracts = append(contracts, contract)
	}
	sort.Slice(contracts, func(left, right int) bool { return contracts[left] < contracts[right] })
	return contracts
}

// Validate checks one JSON document against the named contract's schema.
func (kit *Kit) Validate(contract Contract, data []byte) error {
	compiled, ok := kit.schemas.compiled[contract]
	if !ok {
		return fmt.Errorf("contract kit has no schema for %q", contract)
	}
	if len(data) > maxValidatedBytes {
		return errors.New("document exceeds 16 MiB")
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("not a single JSON value: %w", err)
	}
	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("%s schema: %w", contract, err)
	}
	return nil
}
