package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

// loadOnceChild prepares a Generate child of a finished parent turn.
func loadOnceChild(t *testing.T, f *boundedCloudFixture, key string) (llm.GenerateRequestV1, llm.ExecutionResultV1) {
	t.Helper()
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	child := llm.GenerateRequestV1{OperationKey: key, Context: f.request.Context, Parent: &handle, Append: []llm.Item{preparationMessage("next")}}
	v, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &child})
	return child, boundedState(t, v, err, llm.ExecutionBudgetRequired)
}

// TestCloudPreparedInputMatchesPreparation proves the input a step prepares
// once (#1112) is what preparing it again returns: every copy equals a fresh
// PrepareGenerateInput, copies are independent, and a different manifest or
// a cancelled context behaves exactly as PrepareGenerateInput does.
func TestCloudPreparedInputMatchesPreparation(t *testing.T) {
	f := boundedCloud(t, false, withParentSnapshotBlob)
	child, v := loadOnceChild(t, f, "child")
	ctx := context.Background()
	p, err := f.runtime.preparation.Load(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context})
	if err != nil {
		t.Fatal(err)
	}
	if p.input == nil || p.input.generate == nil || !p.loaded || p.parentSnapshot == nil {
		t.Fatalf("Load did not keep the step's prepared input: %+v", p.input)
	}
	want, err := PrepareGenerateInput(ctx, *p.Generate, p.GenerateReplay)
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.generateInput(ctx)
	if err != nil || !reflect.DeepEqual(first, want) {
		t.Fatalf("reused input differs from a fresh preparation: %v", err)
	}
	// A caller changing its copy changes no other copy.
	first.Request.Input[0] = preparationMessage("changed")
	first.Request.Instructions = append(first.Request.Instructions, llm.Instruction{Kind: llm.InstructionKindText, Text: "changed"})
	second, err := p.generateInput(ctx)
	if err != nil || !reflect.DeepEqual(second, want) {
		t.Fatalf("a copy shares the prepared request: %v", err)
	}
	// Another manifest is prepared afresh, never answered from this input.
	other := *p.Generate
	other.Append = []llm.Item{preparationMessage("other")}
	manifest, _ := json.Marshal(other)
	wantOther, err := PrepareGenerateInput(ctx, other, p.GenerateReplay)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.input.generateInput(ctx, manifest, other, p.GenerateReplay); err != nil || !reflect.DeepEqual(got, wantOther) {
		t.Fatalf("another manifest used the prepared input: %v", err)
	}
	// Admission reads the attempt child, whose manifest is the root's.
	var nilInput *cloudPreparedInput
	if got, err := nilInput.generateInput(ctx, p.Record.Request.Manifest, *p.Generate, p.GenerateReplay); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("no prepared input: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, wantErr := PrepareGenerateInput(cancelled, *p.Generate, p.GenerateReplay)
	if _, err := p.generateInput(cancelled); !errors.Is(err, context.Canceled) || !errors.Is(wantErr, context.Canceled) {
		t.Fatalf("cancelled: %v, want %v", err, wantErr)
	}
	// Restore without a store (completed replay) keeps no input.
	restored, err := f.runtime.preparation.restore(ctx, p.Record, p.Preparation, "trusted-scope")
	if err != nil || restored.loaded || !reflect.DeepEqual(restored.input, p.input) {
		t.Fatalf("restore: loaded=%t err=%v", restored.loaded, err)
	}
}

// TestCloudExecutionRuntimeReadsParentOncePerStep counts reads of the
// referenced parent snapshot blob in each step of one child turn (#1112).
// Before, completion read it three times: once on load and once for each
// attempt-result check.
func TestCloudExecutionRuntimeReadsParentOncePerStep(t *testing.T) {
	f := boundedCloud(t, false, withParentSnapshotBlob)
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	child := llm.GenerateRequestV1{OperationKey: "child", Context: f.request.Context, Parent: &handle, Append: []llm.Item{preparationMessage("next")}}
	ctx := context.Background()
	var parentKey blob.BlobKey
	opens := 0
	f.blobs.open = func(key blob.BlobKey) error {
		if key == parentKey {
			opens++
		}
		return nil
	}
	step := func(name string, want int, run func() (llm.ExecutionResultV1, error), state llm.ExecutionStateV1) llm.ExecutionResultV1 {
		t.Helper()
		opens = 0
		v, err := run()
		boundedState(t, v, err, state)
		if parentKey != "" && opens != want {
			t.Errorf("%s read the parent snapshot %d times, want %d", name, opens, want)
		}
		return v
	}
	// The key is known once the preparation exists; prepare writes the blob,
	// reads it back and reads the durable winner.
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &child})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	record, err := f.repository.Read(ctx, cloudstate.Scope{Tenant: child.Context.Tenant, Project: child.Context.Project}, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	var progress struct {
		Preparation struct {
			Ref struct {
				Blob string `json:"blob"`
			} `json:"parent_snapshot_ref"`
		} `json:"request_preparation"`
	}
	if json.Unmarshal(record.Progress, &progress) != nil || progress.Preparation.Ref.Blob == "" {
		t.Fatalf("no parent reference in %s", record.Progress)
	}
	parentKey = blob.BlobKey(progress.Preparation.Ref.Blob)
	f.now = f.now.Add(time.Second)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context}
	step("acquire", 1, func() (llm.ExecutionResultV1, error) { return f.runtime.AcquireBudgetV1(ctx, ref) }, llm.ExecutionAcquired)
	step("generate", 1, func() (llm.ExecutionResultV1, error) { return f.runtime.GenerateStepV1(ctx, child) }, llm.ExecutionProviderCompleted)
	step("complete", 1, func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) }, llm.ExecutionCompleted)
	// A replay of the completed operation reads no parent at all.
	step("replay", 0, func() (llm.ExecutionResultV1, error) { return f.runtime.CompleteExecutionV1(ctx, ref) }, llm.ExecutionCompleted)
}

// TestCloudExecutionRuntimeTurnStillRejectsCorruptParent: skipping repeated
// parent reads within a step never skips the first one. A parent blob
// tampered with between steps fails the next step closed.
func TestCloudExecutionRuntimeTurnStillRejectsCorruptParent(t *testing.T) {
	f := boundedCloud(t, false, withParentSnapshotBlob)
	child, v := loadOnceChild(t, f, "child")
	ctx := context.Background()
	record, err := f.repository.Read(ctx, cloudstate.Scope{Tenant: child.Context.Tenant, Project: child.Context.Project}, cloudstate.RequestID(v.RequestID))
	if err != nil {
		t.Fatal(err)
	}
	var progress struct {
		Preparation struct {
			Ref struct {
				Blob string `json:"blob"`
			} `json:"parent_snapshot_ref"`
		} `json:"request_preparation"`
	}
	if json.Unmarshal(record.Progress, &progress) != nil || progress.Preparation.Ref.Blob == "" {
		t.Fatal("no parent reference")
	}
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: child.Context}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.GenerateStepV1(ctx, child)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	f.blobs.mu.Lock()
	f.blobs.values[blob.BlobKey(progress.Preparation.Ref.Blob)][20] ^= 1
	f.blobs.mu.Unlock()
	if v, err := f.runtime.CompleteExecutionV1(ctx, ref); err == nil {
		t.Fatalf("completed over a corrupt parent: %+v", v)
	}
}

// TestCandidateResolutionMatchesResolveCandidateRequest proves that sharing
// one resolution between the recovery plausibility check and compilation
// (#1112) derives the same requests and digests as resolving each time.
func TestCandidateResolutionMatchesResolveCandidateRequest(t *testing.T) {
	prompt, err := compaction.Prompt(compaction.PromptVersion)
	if err != nil {
		t.Fatal(err)
	}
	plain := llm.Request{APIVersion: llm.APIVersion, OperationKey: "op", Model: "logical", ServiceClass: llm.ServiceClassStandard,
		Instructions: []llm.Instruction{{Kind: llm.InstructionKindText, Text: "be brief"}}, Input: []llm.Item{preparationMessage("hello")}}
	summarizer := plain
	summarizer.Instructions = []llm.Instruction{
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: prompt},
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "Summary style: concise"},
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "application"},
	}
	none := plain
	none.Instructions = nil
	candidate := routing.Candidate{Model: "provider-model", AttemptedClass: llm.ServiceClassStandard}
	// The plausibility check, as it was before #1112.
	previous := func(semantic llm.Request, digest [32]byte) bool {
		resolved := candidateRequest(semantic, providerStatePins{}, candidate)
		for _, request := range []llm.Request{resolved, compaction.FlattenSummarizerInstructions(resolved)} {
			if got, err := llm.RequestDigest(request); err == nil && got == digest {
				return true
			}
		}
		return false
	}
	for name, request := range map[string]llm.Request{"plain": plain, "summarizer": summarizer, "no instructions": none} {
		t.Run(name, func(t *testing.T) {
			semantic, err := llm.NormalizeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			var digests [][32]byte
			for _, family := range []provider.Family{provider.FamilyOpenAIResponses, provider.FamilyAnthropicMessages} {
				want := resolveCandidateRequest(semantic, providerStatePins{}, routing.Candidate{Model: candidate.Model, AttemptedClass: candidate.AttemptedClass, Family: string(family)}, nil)
				wantDigest, err := llm.RequestDigest(want)
				if err != nil {
					t.Fatal(err)
				}
				flatten := !preservesInstructionHierarchy(family, nil)
				resolution := newCandidateResolution(semantic, providerStatePins{}, candidate)
				if got := resolution.request(flatten); !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: resolved request differs", family)
				}
				if got, err := resolution.digest(flatten); err != nil || got != wantDigest {
					t.Fatalf("%s: digest differs: %v", family, err)
				}
				digests = append(digests, wantDigest)
			}
			if name == "summarizer" && digests[0] == digests[1] {
				t.Fatal("the summarizer request was not flattened")
			}
			if name != "summarizer" && digests[0] != digests[1] {
				t.Fatal("flattening changed a request that is not a summarizer")
			}
			for _, digest := range append(digests, [32]byte{1}) {
				resolution := newCandidateResolution(semantic, providerStatePins{}, candidate)
				if got, want := resolution.plausible(digest), previous(semantic, digest); got != want {
					t.Fatalf("plausible(%x)=%t, want %t", digest[:4], got, want)
				}
				// A request that flattening leaves unchanged is hashed once,
				// also when the adapter then compiles the flattened form.
				if _, err := resolution.digest(true); err != nil {
					t.Fatal(err)
				}
				if name != "summarizer" && resolution.digests[1] != nil {
					t.Fatal("hashed an unchanged request twice")
				}
			}
		})
	}
	if !strings.HasPrefix(summarizer.Instructions[1].Text, "Summary style: ") {
		t.Fatal("fixture is not a summarizer request")
	}
}
