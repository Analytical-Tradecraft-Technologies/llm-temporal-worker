package state

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

const checkpointSchemaVersion = "checkpoint/v1"

// Checkpoint is an immutable graph node. Delta contains the caller's newly
// appended semantic items and Output contains the model turn produced by that
// operation. Neither slice is ever returned by reference after publication.
type Checkpoint struct {
	Handle        Handle
	Tenant        string
	Project       string
	Parent        *Handle
	OperationKey  string
	RequestDigest [32]byte
	Delta         []llm.Item
	Output        []llm.Item
	SettingsPatch SettingsPatch
	Depth         int32
	ExpiresAt     time.Time
	Snapshot      *CheckpointSnapshot
}

// CheckpointSnapshot is a self-contained replay base. Its digest is verified
// before use, so a corrupted optimization can never silently change lineage.
type CheckpointSnapshot struct {
	Items    []llm.Item
	Settings ModelState
	Depth    int32
	Lineage  []Handle
	Digest   [32]byte
}

type MaterializeLimits struct {
	MaxDepth int32
	MaxRows  int
	MaxItems int
	MaxBytes int64
}

func (limits MaterializeLimits) withDefaults() MaterializeLimits {
	if limits.MaxDepth <= 0 {
		limits.MaxDepth = 256
	}
	if limits.MaxRows <= 0 {
		limits.MaxRows = 512
	}
	if limits.MaxItems <= 0 {
		limits.MaxItems = 4096
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = 16 << 20
	}
	return limits
}

// MaterializedState is the immutable view used by validation, routing, and
// provider compilation. Callers may mutate the returned slices without
// changing the graph.
type MaterializedState struct {
	Handle           Handle
	Tenant           string
	Project          string
	Depth            int32
	Items            []llm.Item
	Settings         ModelState
	PendingToolCalls []string
	Lineage          []Handle
}

// CheckpointGraph is a concurrency-safe in-memory graph suitable for pure
// tests and as the contract exercised by a durable repository. PostgreSQL
// persistence intentionally lives in a separate implementation.
type CheckpointGraph struct {
	mu          sync.RWMutex
	checkpoints map[Handle]Checkpoint
	operations  map[string]Handle
	limits      MaterializeLimits
	// Now supplies the clock used for expiry checks. Keeping the clock on the
	// graph makes durable materialization deterministic and lets callers share
	// the same time boundary across repository validation and graph replay.
	Now func() time.Time
}

func NewCheckpointGraph(limits MaterializeLimits) *CheckpointGraph {
	return &CheckpointGraph{
		checkpoints: make(map[Handle]Checkpoint),
		operations:  make(map[string]Handle),
		limits:      limits.withDefaults(),
		Now:         time.Now,
	}
}

func (graph *CheckpointGraph) clock() time.Time {
	if graph != nil && graph.Now != nil {
		return graph.Now().UTC()
	}
	return time.Now().UTC()
}

func (graph *CheckpointGraph) PutRoot(checkpoint Checkpoint) error {
	if checkpoint.Parent != nil {
		return fmt.Errorf("root checkpoint cannot have a parent")
	}
	checkpoint.Depth = 0
	return graph.put(checkpoint)
}

func (graph *CheckpointGraph) PutChild(checkpoint Checkpoint) error {
	if checkpoint.Parent == nil || *checkpoint.Parent == "" {
		return fmt.Errorf("child checkpoint requires a parent")
	}
	graph.mu.RLock()
	parent, ok := graph.checkpoints[*checkpoint.Parent]
	graph.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}
	if checkpoint.Tenant == "" {
		checkpoint.Tenant = parent.Tenant
	}
	if checkpoint.Project == "" {
		checkpoint.Project = parent.Project
	}
	if checkpoint.Tenant != parent.Tenant || checkpoint.Project != parent.Project {
		return ErrTenantMismatch
	}
	checkpoint.Depth = parent.Depth + 1
	return graph.put(checkpoint)
}

func (graph *CheckpointGraph) put(checkpoint Checkpoint) error {
	if graph == nil || checkpoint.Handle == "" || checkpoint.Tenant == "" || checkpoint.OperationKey == "" {
		return fmt.Errorf("checkpoint handle, tenant, and operation key are required")
	}
	if checkpoint.Depth < 0 || checkpoint.Depth > graph.limits.MaxDepth {
		return fmt.Errorf("checkpoint depth exceeds configured limit")
	}
	if err := checkpoint.SettingsPatch.Validate(); err != nil {
		return err
	}
	if err := validateItemEncoding(appendItems(checkpoint.Delta, checkpoint.Output...)); err != nil {
		return err
	}
	if checkpoint.Snapshot != nil {
		if err := checkpoint.Snapshot.validate(); err != nil {
			return err
		}
	}
	value := checkpoint.clone()
	value.RequestDigest = checkpointRequestDigest(value)
	graph.mu.Lock()
	defer graph.mu.Unlock()
	if existing, exists := graph.checkpoints[value.Handle]; exists {
		if existing.RequestDigest == value.RequestDigest {
			return nil
		}
		return ErrConflict
	}
	if previous, exists := graph.operations[value.OperationKey]; exists {
		existing := graph.checkpoints[previous]
		if !existing.ExpiresAt.IsZero() && !graph.clock().Before(existing.ExpiresAt) {
			return ErrExpired
		}
		if previous == value.Handle && existing.RequestDigest == value.RequestDigest {
			return nil
		}
		return ErrConflict
	}
	graph.checkpoints[value.Handle] = value
	graph.operations[value.OperationKey] = value.Handle
	return nil
}

func checkpointRequestDigest(checkpoint Checkpoint) [32]byte {
	data, err := json.Marshal(struct {
		Schema        string
		Tenant        string
		Project       string
		Parent        *Handle
		OperationKey  string
		Delta         []llm.Item
		Output        []llm.Item
		SettingsPatch SettingsPatch
	}{checkpointSchemaVersion, checkpoint.Tenant, checkpoint.Project, checkpoint.Parent, checkpoint.OperationKey, checkpoint.Delta, checkpoint.Output, checkpoint.SettingsPatch})
	if err != nil {
		return [32]byte{}
	}
	canonical, err := llm.CanonicalJSON(data)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(canonical)
}

func (graph *CheckpointGraph) Get(handle Handle) (Checkpoint, error) {
	if graph == nil {
		return Checkpoint{}, ErrNotFound
	}
	graph.mu.RLock()
	checkpoint, ok := graph.checkpoints[handle]
	graph.mu.RUnlock()
	if !ok {
		return Checkpoint{}, ErrNotFound
	}
	return checkpoint.clone(), nil
}

func (graph *CheckpointGraph) Materialize(tenant string, handle Handle) (MaterializedState, error) {
	if graph == nil || handle == "" {
		return MaterializedState{}, ErrNotFound
	}
	path := make([]Checkpoint, 0, 8)
	seen := make(map[Handle]struct{})
	current := handle
	for current != "" {
		if _, duplicate := seen[current]; duplicate {
			return MaterializedState{}, fmt.Errorf("checkpoint graph contains a cycle")
		}
		seen[current] = struct{}{}
		checkpoint, err := graph.Get(current)
		if err != nil {
			return MaterializedState{}, err
		}
		if checkpoint.Tenant != tenant {
			return MaterializedState{}, ErrTenantMismatch
		}
		if !checkpoint.ExpiresAt.IsZero() && !graph.clock().Before(checkpoint.ExpiresAt) {
			return MaterializedState{}, ErrExpired
		}
		path = append(path, checkpoint)
		if int32(len(path)-1) > graph.limits.MaxDepth || len(path) > graph.limits.MaxRows {
			return MaterializedState{}, fmt.Errorf("checkpoint materialization exceeds depth/row limit")
		}
		if checkpoint.Parent == nil {
			break
		}
		current = *checkpoint.Parent
	}
	if len(path) == 0 || path[len(path)-1].Parent != nil {
		return MaterializedState{}, fmt.Errorf("checkpoint graph has no root")
	}

	result := MaterializedState{Handle: handle, Tenant: tenant, Project: path[0].Project, Settings: RootModelState("")}
	start := len(path) - 1
	for index, checkpoint := range path {
		if checkpoint.Snapshot == nil {
			continue
		}
		if err := checkpoint.Snapshot.validate(); err != nil {
			return MaterializedState{}, err
		}
		if checkpoint.Snapshot.Depth != checkpoint.Depth {
			return MaterializedState{}, fmt.Errorf("checkpoint snapshot depth does not match checkpoint")
		}
		wantLineage := make([]Handle, 0, len(path)-index)
		for lineageIndex := len(path) - 1; lineageIndex >= index; lineageIndex-- {
			wantLineage = append(wantLineage, path[lineageIndex].Handle)
		}
		if !sameHandles(checkpoint.Snapshot.Lineage, wantLineage) {
			return MaterializedState{}, fmt.Errorf("checkpoint snapshot lineage mismatch")
		}
		result.Items = cloneItems(checkpoint.Snapshot.Items)
		result.Settings = checkpoint.Snapshot.Settings.Clone()
		result.Depth = checkpoint.Snapshot.Depth
		if result.Depth < 0 || result.Depth > graph.limits.MaxDepth {
			return MaterializedState{}, fmt.Errorf("checkpoint snapshot exceeds depth limit")
		}
		if err := graph.validateMaterializedLimits(result.Items); err != nil {
			return MaterializedState{}, err
		}
		start = index - 1
		break
	}
	if start == len(path)-1 {
		result.Settings = RootModelState("")
	}
	for index := start; index >= 0; index-- {
		checkpoint := path[index]
		var err error
		result.Settings, err = ApplySettingsPatch(result.Settings, checkpoint.SettingsPatch)
		if err != nil {
			return MaterializedState{}, fmt.Errorf("checkpoint %s settings: %w", checkpoint.Handle, err)
		}
		result.Items = appendItems(result.Items, checkpoint.Delta...)
		result.Items = appendItems(result.Items, checkpoint.Output...)
		result.Depth = checkpoint.Depth
		if err := graph.validateMaterializedLimits(result.Items); err != nil {
			return MaterializedState{}, err
		}
	}
	if result.Settings.Model == "" {
		return MaterializedState{}, fmt.Errorf("materialized root model is required")
	}
	pending, err := validateItems(result.Items)
	if err != nil {
		return MaterializedState{}, err
	}
	result.Items = cloneItems(result.Items)
	result.PendingToolCalls = append([]string(nil), pending...)
	result.Lineage = make([]Handle, 0, len(path))
	for index := len(path) - 1; index >= 0; index-- {
		result.Lineage = append(result.Lineage, path[index].Handle)
	}
	result.Settings = result.Settings.Clone()
	return result, nil
}

func (graph *CheckpointGraph) validateMaterializedLimits(items []llm.Item) error {
	if len(items) > graph.limits.MaxItems {
		return fmt.Errorf("checkpoint materialization exceeds item limit")
	}
	bytes, err := canonicalItemsWithLimit(items, int(graph.limits.MaxBytes))
	if err != nil {
		return err
	}
	if int64(len(bytes)) > graph.limits.MaxBytes {
		return fmt.Errorf("checkpoint materialization exceeds byte limit")
	}
	return nil
}

func NewCheckpointSnapshot(materialized MaterializedState) *CheckpointSnapshot {
	snapshot := &CheckpointSnapshot{Items: cloneItems(materialized.Items), Settings: materialized.Settings.Clone(), Depth: materialized.Depth, Lineage: append([]Handle(nil), materialized.Lineage...)}
	snapshot.Digest = snapshot.digest()
	return snapshot
}

func (snapshot CheckpointSnapshot) validate() error {
	if snapshot.Depth < 0 || snapshot.Settings.Model == "" {
		return fmt.Errorf("checkpoint snapshot is incomplete")
	}
	if err := snapshot.Settings.Validate(); err != nil {
		return err
	}
	if _, err := validateItems(snapshot.Items); err != nil {
		return err
	}
	if snapshot.Digest != snapshot.digest() {
		return fmt.Errorf("checkpoint snapshot digest mismatch")
	}
	return nil
}

func (snapshot CheckpointSnapshot) digest() [32]byte {
	data, _ := json.Marshal(struct {
		Schema   string
		Items    []llm.Item
		Settings ModelState
		Depth    int32
		Lineage  []Handle
	}{checkpointSchemaVersion, snapshot.Items, snapshot.Settings, snapshot.Depth, snapshot.Lineage})
	return sha256.Sum256(data)
}

func sameHandles(left, right []Handle) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (checkpoint Checkpoint) clone() Checkpoint {
	result := checkpoint
	if checkpoint.Parent != nil {
		parent := *checkpoint.Parent
		result.Parent = &parent
	}
	result.Delta = cloneItems(checkpoint.Delta)
	result.Output = cloneItems(checkpoint.Output)
	result.SettingsPatch = cloneSettingsPatch(checkpoint.SettingsPatch)
	if checkpoint.Snapshot != nil {
		snapshot := *checkpoint.Snapshot
		snapshot.Items = cloneItems(checkpoint.Snapshot.Items)
		snapshot.Lineage = append([]Handle(nil), checkpoint.Snapshot.Lineage...)
		snapshot.Settings = checkpoint.Snapshot.Settings.Clone()
		result.Snapshot = &snapshot
	}
	return result
}

func cloneSettingsPatch(patch SettingsPatch) SettingsPatch {
	result := patch
	result.Model.Set = clonePointer(patch.Model.Set)
	result.ServiceClass.Set = clonePointer(patch.ServiceClass.Set)
	result.ServiceClassFallbacks.Set = clonePointer(patch.ServiceClassFallbacks.Set)
	if patch.ServiceClassFallbacks.Set != nil {
		result.ServiceClassFallbacks.Set = &[]llm.ServiceClass{}
		*result.ServiceClassFallbacks.Set = append([]llm.ServiceClass(nil), (*patch.ServiceClassFallbacks.Set)...)
	}
	result.Portability.Set = clonePointer(patch.Portability.Set)
	if patch.Instructions.Set != nil {
		value := cloneInstructions(*patch.Instructions.Set)
		result.Instructions.Set = &value
	}
	if patch.Tools.Set != nil {
		value := cloneTools(*patch.Tools.Set)
		result.Tools.Set = &value
	}
	result.ToolPolicy.Set = clonePointer(patch.ToolPolicy.Set)
	result.Output.Set = cloneOutput(patch.Output.Set)
	result.Temperature.Set = clonePointer(patch.Temperature.Set)
	result.TemperatureDecimal.Set = clonePointer(patch.TemperatureDecimal.Set)
	result.ReasoningEffort.Set = clonePointer(patch.ReasoningEffort.Set)
	result.ReasoningSummary.Set = clonePointer(patch.ReasoningSummary.Set)
	if patch.CompactionPolicy.Set != nil {
		value := append(json.RawMessage(nil), (*patch.CompactionPolicy.Set)...)
		result.CompactionPolicy.Set = &value
	}
	if patch.Extensions.Set != nil {
		value := cloneRawMap(*patch.Extensions.Set)
		result.Extensions.Set = &value
	}
	return result
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func appendItems(base []llm.Item, values ...llm.Item) []llm.Item {
	result := append([]llm.Item(nil), base...)
	return append(result, values...)
}

func cloneItems(values []llm.Item) []llm.Item {
	if values == nil {
		return nil
	}
	result := make([]llm.Item, len(values))
	for index, item := range values {
		result[index] = cloneItem(item)
	}
	return result
}

// cloneItem performs a structural copy of the closed llm.Item union. The
// previous JSON round trip was needlessly expensive for large immutable
// snapshots (and made the race-detector lineage proof exceed its CI budget).
// Keep this copy local to the state package so callers still receive fully
// detached slices and maps without paying serialization and decoding costs.
func cloneItem(item llm.Item) llm.Item {
	switch value := item.(type) {
	case llm.Message:
		value.Content = cloneParts(value.Content)
		return value
	case llm.ToolCall:
		value.Arguments = append(json.RawMessage(nil), value.Arguments...)
		return value
	case llm.ToolResult:
		value.Content = cloneParts(value.Content)
		return value
	case llm.ProviderState:
		value.Opaque = append([]byte(nil), value.Opaque...)
		return value
	case llm.Reference:
		value.Metadata = cloneRawMap(value.Metadata)
		return value
	default:
		// Item is a sealed interface in llm. Keep a defensive shallow copy for
		// any future implementation until its mutable fields are added here.
		return item
	}
}

func canonicalItems(values []llm.Item) ([]byte, error) {
	return canonicalItemsWithLimit(values, llm.DefaultCanonicalMaxBytes)
}

func canonicalItemsWithLimit(values []llm.Item, maxBytes int) ([]byte, error) {
	data, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return llm.CanonicalJSONWithLimits(data, maxBytes, llm.DefaultCanonicalMaxDepth)
}
