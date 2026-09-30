package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// The repository adapter tests validate the full checkpoint and real storage
// writes. This fixture isolates the runtime's ordering and typed replay gate.
func checkpointFinalizerFixture(t *testing.T, kind, mode string) (*cloudRequestRuntime, *recordingCloudRequests, *replayEffectsStore, func() ([]byte, error), func() error) {
	t.Helper()
	r, repository, store, run, save, effects := finalizationFixture(t, kind, mode)
	if err := save(effects); err != nil {
		t.Fatal(err)
	}
	var payload cloudFinalizationPayload
	if err := json.Unmarshal(repository.handoff.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	checkpoint := state.DurableCheckpoint{ID: repository.handoff.CheckpointID, ScopeID: repository.handoff.CheckpointScope}
	manifest := append(json.RawMessage(nil), repository.operation.Manifest...)
	repository.handoff = nil
	repository.record.Progress = json.RawMessage(`{"version":1}`)
	commit := func() error {
		if kind == "generate" {
			var request llm.GenerateRequestV1
			var response llm.GenerateResponseV1
			if err := json.Unmarshal(manifest, &request); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(payload.Response, &response); err != nil {
				t.Fatal(err)
			}
			return r.finalizer.CommitGenerate(context.Background(), request, checkpoint, response, effects)
		}
		var request llm.CompactRequestV1
		var response llm.CompactResponseV1
		if err := json.Unmarshal(manifest, &request); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload.Response, &response); err != nil {
			t.Fatal(err)
		}
		return r.finalizer.CommitCompact(context.Background(), request, checkpoint, response, effects)
	}
	return r, repository, store, run, commit
}

func TestCloudCheckpointFinalizationResumesBeforeInnerExecution(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, mode := range []string{"provider", "cache"} {
			for _, failure := range []string{"plan", "checkpoint", "handoff", "effects"} {
				t.Run(kind+"/"+mode+"/"+failure, func(t *testing.T) {
					r, repository, store, run, commit := checkpointFinalizerFixture(t, kind, mode)
					switch failure {
					case "plan":
						repository.savePlanErr = contracts.ErrOutcomeUnknown
					case "checkpoint":
						repository.resumePlanErr = contracts.ErrOutcomeUnknown
					case "handoff":
						repository.saveHandoffErr = contracts.ErrOutcomeUnknown
					case "effects":
						if mode == "provider" {
							store.fail = "budget"
						} else {
							store.fail = "use"
						}
					}
					if err := commit(); err == nil {
						t.Fatal("failure hidden")
					}
					if failure != "effects" && len(store.events) != 0 {
						t.Fatal("effects before checkpoint and handoff", store.events)
					}
					if repository.record.Status != cloudstate.StatusRunning {
						t.Fatal("terminal before settlement")
					}
					originalPlan, _ := json.Marshal(repository.checkpointPlan)
					repository.savePlanErr, repository.resumePlanErr, repository.saveHandoffErr = nil, nil, nil
					store.fail, store.events = "", nil
					repository.planSteps = nil
					// Lose the finalizer's process-local state and change the current clock.
					restarted, err := newCloudFinalizer(repository, store, store, store, func() time.Time { return r.clock().Add(24 * time.Hour) })
					if err != nil {
						t.Fatal(err)
					}
					r.finalizer = restarted
					if _, err := run(); err != nil {
						t.Fatal(err)
					}
					wantSteps := []string{"commit"}
					if failure == "handoff" || failure == "effects" {
						wantSteps = nil
					}
					if !reflect.DeepEqual(repository.planSteps, wantSteps) {
						t.Fatal("publication was skipped or repeated", repository.planSteps)
					}
					wantEvents := []string{"publish", "budget", "fill"}
					if mode == "cache" {
						wantEvents = []string{"use"}
					}
					if !reflect.DeepEqual(store.events, wantEvents) {
						t.Fatal("incorrect effects", store.events)
					}
					currentPlan, _ := json.Marshal(repository.checkpointPlan)
					if string(currentPlan) != string(originalPlan) {
						t.Fatal("original checkpoint/receipts changed")
					}
					if repository.record.Status != cloudstate.StatusCompleted {
						t.Fatal("not completed")
					}
					count := len(store.events)
					if _, err := run(); err != nil || len(store.events) != count {
						t.Fatal("terminal replay reapplied effects", err)
					}
				})
			}
		}
	}
}

func TestCloudCheckpointFinalizationValidatesBeforePublication(t *testing.T) {
	for _, failure := range []string{"version", "cost", "missing_plan", "missing_blob", "unavailable"} {
		t.Run(failure, func(t *testing.T) {
			_, repository, store, run, commit := checkpointFinalizerFixture(t, "generate", "provider")
			repository.savePlanErr = contracts.ErrOutcomeUnknown
			if err := commit(); err == nil {
				t.Fatal("lost plan acknowledgement hidden")
			}
			repository.savePlanErr = nil
			var payload cloudFinalizationPayload
			if err := json.Unmarshal(repository.checkpointPlan.Handoff.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "version":
				payload.Version = 2
			case "cost":
				payload.Effects.Provider.Budget.Events = nil
			case "missing_plan":
				repository.loadPlanErr = cloudstate.ErrCheckpointFinalizationMissing
			case "missing_blob":
				repository.resumePlanErr = contracts.ErrNotFound
			case "unavailable":
				repository.loadPlanErr = contracts.ErrUnavailable
			}
			repository.checkpointPlan.Handoff.Payload, _ = json.Marshal(payload)
			repository.planSteps = nil
			if _, err := run(); err == nil {
				t.Fatal("invalid plan accepted")
			}
			if len(store.events) != 0 {
				t.Fatal("effects on invalid/unreadable plan", store.events)
			}
			if failure != "missing_blob" && len(repository.planSteps) != 0 {
				t.Fatal("invalid plan published", repository.planSteps)
			}
			if repository.record.Status != cloudstate.StatusRunning {
				t.Fatal("failed publication terminalized")
			}
		})
	}
}

func TestCloudCheckpointFinalizationCommitOrdersEffects(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			_, repository, store, run, commit := checkpointFinalizerFixture(t, kind, "provider")
			if err := commit(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(repository.planSteps, []string{"save", "commit"}) || repository.handoff == nil {
				t.Fatal("missing durable publication gate")
			}
			if !reflect.DeepEqual(store.events, []string{"publish", "budget", "fill"}) {
				t.Fatal("wrong effect order", store.events)
			}
			if _, err := run(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
