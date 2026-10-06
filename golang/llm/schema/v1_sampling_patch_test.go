package schema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

// TestV1SamplingAndReasoningPatchBoundsAgree checks the published schema and
// the Go decoder accept and reject the same top_p, stop_sequences, seed,
// reasoning_mode and reasoning_token_budget values.
func TestV1SamplingAndReasoningPatchBoundsAgree(t *testing.T) {
	compiled := readV1Schema(t, "generate-request.schema.json")
	maxStop := make([]string, llm.MaxStopSequencesV1)
	for index := range maxStop {
		maxStop[index] = strings.Repeat("x", index+1)
	}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	for name, test := range map[string]struct {
		patch string
		valid bool
	}{
		"top_p smallest":          {patch: `{"top_p":{"set":"0.000000000000000001"}}`, valid: true},
		"top_p one":               {patch: `{"top_p":{"set":"1"}}`, valid: true},
		"top_p one with zeros":    {patch: `{"top_p":{"set":"1.000000000000000000"}}`, valid: true},
		"top_p trailing zero":     {patch: `{"top_p":{"set":"0.50"}}`, valid: true},
		"top_p zero":              {patch: `{"top_p":{"set":"0"}}`},
		"top_p zero fraction":     {patch: `{"top_p":{"set":"0.000"}}`},
		"top_p above one":         {patch: `{"top_p":{"set":"1.000000000000000001"}}`},
		"top_p two":               {patch: `{"top_p":{"set":"2"}}`},
		"top_p too precise":       {patch: `{"top_p":{"set":"0.1000000000000000001"}}`},
		"top_p number":            {patch: `{"top_p":{"set":0.5}}`},
		"top_p negative":          {patch: `{"top_p":{"set":"-0.5"}}`},
		"top_p clear":             {patch: `{"top_p":{"clear":true}}`, valid: true},
		"stop one":                {patch: `{"stop_sequences":{"set":["END"]}}`, valid: true},
		"stop maximum":            {patch: `{"stop_sequences":{"set":` + encode(maxStop) + `}}`, valid: true},
		"stop too many":           {patch: `{"stop_sequences":{"set":` + encode(append(maxStop, "extra")) + `}}`},
		"stop empty list":         {patch: `{"stop_sequences":{"set":[]}}`},
		"stop empty value":        {patch: `{"stop_sequences":{"set":[""]}}`},
		"stop duplicate":          {patch: `{"stop_sequences":{"set":["END","END"]}}`},
		"stop longest":            {patch: `{"stop_sequences":{"set":["` + strings.Repeat("é", llm.MaxStopSequenceLengthV1) + `"]}}`, valid: true},
		"stop too long":           {patch: `{"stop_sequences":{"set":["` + strings.Repeat("a", llm.MaxStopSequenceLengthV1+1) + `"]}}`},
		"stop not string":         {patch: `{"stop_sequences":{"set":[1]}}`},
		"stop null":               {patch: `{"stop_sequences":{"set":null}}`},
		"stop clear":              {patch: `{"stop_sequences":{"clear":true}}`, valid: true},
		"seed zero":               {patch: `{"seed":{"set":0}}`, valid: true},
		"seed maximum":            {patch: `{"seed":{"set":9007199254740991}}`, valid: true},
		"seed above maximum":      {patch: `{"seed":{"set":9007199254740992}}`},
		"seed negative":           {patch: `{"seed":{"set":-1}}`},
		"seed fraction":           {patch: `{"seed":{"set":1.5}}`},
		"seed string":             {patch: `{"seed":{"set":"1"}}`},
		"seed clear":              {patch: `{"seed":{"clear":true}}`, valid: true},
		"mode enabled":            {patch: `{"reasoning_mode":{"set":"enabled"}}`, valid: true},
		"mode provider default":   {patch: `{"reasoning_mode":{"set":"provider_default"}}`, valid: true},
		"mode disabled":           {patch: `{"reasoning_mode":{"set":"disabled"}}`, valid: true},
		"mode adaptive":           {patch: `{"reasoning_mode":{"set":"adaptive"}}`, valid: true},
		"mode empty":              {patch: `{"reasoning_mode":{"set":""}}`},
		"mode unknown":            {patch: `{"reasoning_mode":{"set":"on"}}`},
		"mode clear":              {patch: `{"reasoning_mode":{"clear":true}}`, valid: true},
		"budget one":              {patch: `{"reasoning_token_budget":{"set":1}}`, valid: true},
		"budget maximum":          {patch: `{"reasoning_token_budget":{"set":2147483647}}`, valid: true},
		"budget zero":             {patch: `{"reasoning_token_budget":{"set":0}}`},
		"budget above maximum":    {patch: `{"reasoning_token_budget":{"set":2147483648}}`},
		"budget fraction":         {patch: `{"reasoning_token_budget":{"set":1024.5}}`},
		"budget clear":            {patch: `{"reasoning_token_budget":{"clear":true}}`, valid: true},
		"budget set and clear":    {patch: `{"reasoning_token_budget":{"set":1024,"clear":true}}`},
		"budget clear false":      {patch: `{"reasoning_token_budget":{"clear":false}}`},
		"unknown sampling leaf":   {patch: `{"top_k":{"set":5}}`},
		"seed with other leaves":  {patch: `{"seed":{"set":7},"temperature":{"set":"0.2"},"reasoning_effort":{"set":"high"}}`, valid: true},
		"all leaves set together": {patch: `{"top_p":{"set":"0.9"},"stop_sequences":{"set":["a"]},"seed":{"set":1},"reasoning_mode":{"set":"enabled"},"reasoning_token_budget":{"set":1024}}`, valid: true},
	} {
		t.Run(name, func(t *testing.T) {
			document := []byte(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":` + test.patch + `}`)
			schemaErr := compiled.Validate(document)
			var request llm.GenerateRequestV1
			decodeErr := json.Unmarshal(document, &request)
			if test.valid != (schemaErr == nil) || test.valid != (decodeErr == nil) {
				t.Fatalf("schema error = %v, decode error = %v; want valid=%t", schemaErr, decodeErr, test.valid)
			}
			if !test.valid {
				return
			}
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if err := compiled.Validate(encoded); err != nil {
				t.Fatalf("re-marshaled request is not schema-valid: %v\n%s", err, encoded)
			}
		})
	}
}
