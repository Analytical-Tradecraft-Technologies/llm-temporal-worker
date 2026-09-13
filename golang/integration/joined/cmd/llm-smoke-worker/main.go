package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/client"
)

const taskQueue = "llm-inference"

type syntheticRuntime struct{}

func (syntheticRuntime) GenerateV1(_ context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	output := json.RawMessage(`{"synthetic":"deterministic"}`)
	if request.SettingsPatch.Output.Set != nil {
		format := request.SettingsPatch.Output.Set.Format
		if format.Kind == llm.OutputKindJSONSchema {
			var schema map[string]any
			decoder := json.NewDecoder(strings.NewReader(string(format.Schema)))
			decoder.UseNumber()
			if err := decoder.Decode(&schema); err != nil {
				return llm.GenerateResponseV1{}, fmt.Errorf("decode requested JSON schema: %w", err)
			}
			value, err := synthesize(schema, schema, "root", 0)
			if err != nil {
				return llm.GenerateResponseV1{}, fmt.Errorf("synthesize %s: %w", format.Name, err)
			}
			output, err = json.Marshal(value)
			if err != nil {
				return llm.GenerateResponseV1{}, err
			}
		}
	}
	digest := sha256.Sum256([]byte(request.OperationKey))
	operationID := "smoke-" + hex.EncodeToString(digest[:16])
	checkpoint := llm.CheckpointHandle("checkpoint-" + hex.EncodeToString(digest[:16]))
	return llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: request.OperationKey, OperationID: operationID,
		Status:     llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.JSONPart{Value: output}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: checkpoint, Parent: request.Parent, Kind: "generation", Depth: 1},
		Cache:      llm.CacheDispositionV1{Disposition: "miss", Variant: 0},
		Cost:       llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"},
	}, nil
}
func (syntheticRuntime) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, errors.New("joined smoke never dispatches compact")
}
func (syntheticRuntime) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, errors.New("joined smoke never dispatches query")
}

func synthesize(node, root map[string]any, field string, depth int) (any, error) {
	if depth > 32 {
		return nil, errors.New("schema nesting exceeds 32")
	}
	if ref, ok := node["$ref"].(string); ok {
		const prefix = "#/$defs/"
		if !strings.HasPrefix(ref, prefix) {
			return nil, fmt.Errorf("unsupported ref %q", ref)
		}
		defs, ok := root["$defs"].(map[string]any)
		if !ok {
			return nil, errors.New("schema defs are absent")
		}
		resolved, ok := defs[strings.TrimPrefix(ref, prefix)].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unresolved ref %q", ref)
		}
		return synthesize(resolved, root, field, depth+1)
	}
	if value, exists := node["const"]; exists {
		return value, nil
	}
	if values, ok := node["enum"].([]any); ok && len(values) > 0 {
		return values[0], nil
	}
	for _, keyword := range []string{"oneOf", "anyOf"} {
		if branches, ok := node[keyword].([]any); ok && len(branches) > 0 {
			for _, raw := range branches {
				branch, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				value, err := synthesize(branch, root, field, depth+1)
				if err == nil && value != nil {
					return value, nil
				}
			}
		}
	}
	typeName, _ := node["type"].(string)
	if typeName == "" {
		if _, ok := node["properties"]; ok {
			typeName = "object"
		}
		if _, ok := node["items"]; ok {
			typeName = "array"
		}
	}
	switch typeName {
	case "object":
		properties, _ := node["properties"].(map[string]any)
		required, _ := node["required"].([]any)
		result := make(map[string]any, len(required))
		for _, rawName := range required {
			name, ok := rawName.(string)
			if !ok {
				continue
			}
			property, ok := properties[name].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("required property %q has no schema", name)
			}
			value, err := synthesize(property, root, name, depth+1)
			if err != nil {
				return nil, err
			}
			result[name] = value
		}
		return result, nil
	case "array":
		minimum := numericInt(node["minItems"], 0)
		if minimum == 0 && strings.Contains(field, "quantile") {
			minimum = 9
		}
		item, _ := node["items"].(map[string]any)
		result := make([]any, minimum)
		for index := range result {
			value, err := synthesize(item, root, field, depth+1)
			if err != nil {
				return nil, err
			}
			if number, ok := value.(json.Number); ok && strings.Contains(field, "quantile_levels") {
				levels := []string{"0.01", "0.05", "0.1", "0.25", "0.5", "0.75", "0.9", "0.95", "0.99"}
				if index < len(levels) {
					number = json.Number(levels[index])
				}
				value = number
			}
			result[index] = value
		}
		return result, nil
	case "integer":
		return json.Number(strconv.Itoa(numericInt(node["minimum"], 0))), nil
	case "number":
		if strings.Contains(field, "probability") || strings.Contains(field, "confidence") || strings.Contains(field, "score") {
			return json.Number("0.5"), nil
		}
		if minimum, ok := node["minimum"].(json.Number); ok {
			return minimum, nil
		}
		return json.Number("0"), nil
	case "boolean":
		return false, nil
	case "null":
		return nil, nil
	case "string":
		return syntheticString(node, field), nil
	default:
		return nil, fmt.Errorf("unsupported schema type %q for %s", typeName, field)
	}
}

func numericInt(value any, fallback int) int {
	switch value := value.(type) {
	case json.Number:
		parsed, err := strconv.Atoi(value.String())
		if err == nil {
			return parsed
		}
	case float64:
		return int(value)
	}
	return fallback
}

func syntheticString(node map[string]any, field string) string {
	format, _ := node["format"].(string)
	switch format {
	case "date-time":
		return "2026-08-10T00:00:00Z"
	case "uri", "uri-reference":
		return "https://source.smoke.test/evidence"
	}
	pattern, _ := node["pattern"].(string)
	if strings.Contains(pattern, "0-9a-f") && strings.Contains(pattern, "64") {
		return strings.Repeat("a", 64)
	}
	if strings.Contains(pattern, "sha256") {
		return "sha256:" + strings.Repeat("a", 64)
	}
	name := strings.ToLower(field)
	switch {
	case strings.Contains(name, "sha256"):
		return strings.Repeat("a", 64)
	case strings.Contains(name, "schema_version"):
		return "synthetic/v1"
	case strings.Contains(name, "_at") || strings.Contains(name, "_utc"):
		return "2026-08-10T00:00:00Z"
	case strings.Contains(name, "url") || strings.Contains(name, "uri"):
		return "https://source.smoke.test/evidence"
	case strings.Contains(name, "language"):
		return "en"
	case strings.Contains(name, "probability"):
		return "0.5"
	}
	minimum := numericInt(node["minLength"], 1)
	value := "synthetic"
	for len(value) < minimum {
		value += "-evidence"
	}
	if maximum := numericInt(node["maxLength"], len(value)); len(value) > maximum {
		value = value[:maximum]
	}
	if pattern != "" {
		if compiled, err := regexp.Compile(pattern); err == nil && !compiled.MatchString(value) {
			if compiled.MatchString("id-1") {
				return "id-1"
			}
		}
	}
	return value
}

func main() {
	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		fmt.Fprintln(os.Stderr, "llm-smoke-worker: TEMPORAL_ADDRESS is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workflowClient, err := client.Dial(client.Options{HostPort: address, Namespace: "default", Identity: "joined-smoke-llm"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "llm-smoke-worker: connect Temporal: %v\n", err)
		os.Exit(1)
	}
	defer workflowClient.Close()
	worker, err := app.NewWorker(app.WorkerOptions{Client: workflowClient, TaskQueue: taskQueue, Identity: "joined-smoke-llm", MaxConcurrentActivities: 4, MaxConcurrentActivityTaskPolls: 2, GracefulStopTimeout: 20 * time.Second, Activities: &activity.Activities{V1Runtime: syntheticRuntime{}, PayloadLimits: activity.PayloadLimits{MaxInlineBytes: 4 << 20}}})
	if err != nil {
		fmt.Fprintf(os.Stderr, "llm-smoke-worker: construct: %v\n", err)
		os.Exit(1)
	}
	if err := worker.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "llm-smoke-worker: start: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("LLM_SMOKE_WORKER_READY task_queue=" + taskQueue)
	<-ctx.Done()
	worker.Stop()
}
