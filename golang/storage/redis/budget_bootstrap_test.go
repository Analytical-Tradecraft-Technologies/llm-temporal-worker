package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type bootstrapGenerationFake struct {
	pointer   ActiveBudgetGeneration
	manifest  BudgetManifest
	activeErr error
	loadErr   error
}

func (fake *bootstrapGenerationFake) ActiveGeneration(context.Context) (ActiveBudgetGeneration, error) {
	if fake.activeErr != nil {
		return ActiveBudgetGeneration{}, fake.activeErr
	}
	return fake.pointer, nil
}

func (fake *bootstrapGenerationFake) LoadManifest(context.Context, ActiveBudgetGeneration) (BudgetManifest, error) {
	if fake.loadErr != nil {
		return BudgetManifest{}, fake.loadErr
	}
	return fake.manifest, nil
}

func (fake *bootstrapGenerationFake) PublishGeneration(context.Context, BudgetManifest) (ActiveBudgetGeneration, error) {
	return ActiveBudgetGeneration{}, errors.New("metadata-only publish must not be used")
}

type bootstrapWorkingSetFake struct {
	proof        BudgetWorkingSetProof
	inspectErr   error
	inspectCalls int
	publishCalls int
	published    BudgetBootstrapCandidate
}

func (fake *bootstrapWorkingSetFake) Inspect(context.Context, ActiveBudgetGeneration, BudgetManifestExpectation) (BudgetWorkingSetProof, error) {
	fake.inspectCalls++
	if fake.inspectErr != nil {
		return BudgetWorkingSetProof{}, fake.inspectErr
	}
	return fake.proof, nil
}

func (fake *bootstrapWorkingSetFake) PublishAndSwitch(_ context.Context, lease string, candidate BudgetBootstrapCandidate, _ BudgetStreamEvent) (ActiveBudgetGeneration, error) {
	if lease == "" {
		return ActiveBudgetGeneration{}, errors.New("missing fence lease")
	}
	fake.publishCalls++
	fake.published = candidate
	return candidate.Manifest.Pointer()
}

type bootstrapFleetFake struct {
	proof BudgetBootstrapFleetProof
	calls int
}

func (fake *bootstrapFleetFake) Prove(context.Context, BudgetGenerationID, BudgetIncarnationID) (BudgetBootstrapFleetProof, error) {
	fake.calls++
	return fake.proof, nil
}

type bootstrapFenceFake struct {
	acquires int
	releases int
	token    string
	empty    bool
}

func (fake *bootstrapFenceFake) Acquire(context.Context, BudgetGenerationID, BudgetIncarnationID) (string, error) {
	fake.acquires++
	if !fake.empty && fake.token == "" {
		return "fence-token", nil
	}
	return fake.token, nil
}

func (fake *bootstrapFenceFake) Release(context.Context, string) error {
	fake.releases++
	return nil
}

type bootstrapBuilderFake struct {
	candidate BudgetBootstrapCandidate
	calls     int
}

func (fake *bootstrapBuilderFake) Build(context.Context, BudgetBootstrapBuildRequest) (BudgetBootstrapCandidate, error) {
	fake.calls++
	return fake.candidate, nil
}

func bootstrapMissingActiveError() error {
	return fmt.Errorf("%w: %w", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMissing)
}

func bootstrapExpectation(manifest BudgetManifest) BudgetManifestExpectation {
	return BudgetManifestExpectation{
		GenerationID: manifest.GenerationID, IncarnationID: manifest.IncarnationID,
		ConfigVersion: manifest.ConfigVersion, PriceVersion: manifest.PriceVersion,
		PolicyHash: manifest.PolicyHash, WindowHash: manifest.WindowHash,
		CoverageStart: manifest.CoverageStart, CoverageEnd: manifest.CoverageEnd,
		StreamHighWaterMark: manifest.StreamHighWaterMark, RoundingVersion: manifest.RoundingVersion,
		JournalHighWaterMark: manifest.JournalHighWaterMark, Members: append([]BudgetManifestMember(nil), manifest.Members...),
	}
}

func bootstrapWorkingSet(t *testing.T, manifest BudgetManifest) BudgetWorkingSetProof {
	t.Helper()
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		t.Fatal(err)
	}
	return BudgetWorkingSetProof{Complete: true, ManifestDigest: digest, MemberCatalogDigest: manifest.MemberCatalogDigest, MemberCount: len(manifest.Members), BucketCount: manifest.BucketCount, StreamHighWaterMark: manifest.StreamHighWaterMark}
}

func TestBudgetBootstrapAdoptsIntactGenerationWithoutFleetOrPostgres(t *testing.T) {
	manifest := testBudgetManifest(t)
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	generation := &bootstrapGenerationFake{pointer: pointer, manifest: manifest}
	workingSet := &bootstrapWorkingSetFake{proof: bootstrapWorkingSet(t, manifest)}
	fleet := &bootstrapFleetFake{}
	fence := &bootstrapFenceFake{}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: workingSet, Fleet: fleet, Fence: fence, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest)})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if result.Outcome != BudgetBootstrapAdopted || result.Reason != BudgetBootstrapIntact || result.ReadsPostgres {
		t.Fatalf("result = %#v", result)
	}
	if fleet.calls != 0 || fence.acquires != 0 || builder.calls != 0 {
		t.Fatalf("adoption invoked rebuild ports: fleet=%d fence=%d builder=%d", fleet.calls, fence.acquires, builder.calls)
	}
}

func TestBudgetBootstrapBlocksSameIncarnationLossWithoutPostgres(t *testing.T) {
	manifest := testBudgetManifest(t)
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	generation := &bootstrapGenerationFake{pointer: pointer, loadErr: ErrBudgetManifestInvalid}
	workingSet := &bootstrapWorkingSetFake{}
	fleet := &bootstrapFleetFake{}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: workingSet, Fleet: fleet, Fence: &bootstrapFenceFake{}, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, ErrBudgetBootstrapSameIncarnation) || result.Reason != BudgetBootstrapSameIncarnationLoss {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if fleet.calls != 0 || builder.calls != 0 {
		t.Fatalf("same-incarnation loss reached rebuild ports: fleet=%d builder=%d", fleet.calls, builder.calls)
	}
}

func TestBudgetBootstrapBlocksLiveOrReconnectingFleet(t *testing.T) {
	manifest := testBudgetManifest(t)
	generation := &bootstrapGenerationFake{activeErr: bootstrapMissingActiveError()}
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{LiveCount: 1}}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: &bootstrapWorkingSetFake{}, Fleet: fleet, Fence: &bootstrapFenceFake{}, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, ErrBudgetBootstrapLiveWorkers) || result.Reason != BudgetBootstrapLiveSession {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if builder.calls != 0 {
		t.Fatalf("live fleet reached builder: %d", builder.calls)
	}
}

func TestBudgetBootstrapColdRebuildPublishesOnlyAfterFence(t *testing.T) {
	manifest := testBudgetManifest(t)
	manifest.GenerationID = "generation-2"
	manifest.IncarnationID = "redis-incarnation-2"
	manifest.MemberCatalogDigest, _ = MemberCatalogDigest(manifest.Members)
	generation := &bootstrapGenerationFake{activeErr: bootstrapMissingActiveError()}
	workingSet := &bootstrapWorkingSetFake{proof: bootstrapWorkingSet(t, manifest)}
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{VerifiedColdStart: true}}
	fence := &bootstrapFenceFake{}
	builder := &bootstrapBuilderFake{candidate: BudgetBootstrapCandidate{Manifest: manifest, WorkingSet: bootstrapWorkingSet(t, manifest)}}
	expected := bootstrapExpectation(testBudgetManifest(t))
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: workingSet, Fleet: fleet, Fence: fence, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: expected, Rebuild: BudgetBootstrapColdRebuild, GenerationID: manifest.GenerationID, IncarnationID: manifest.IncarnationID})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if result.Outcome != BudgetBootstrapRebuilt || !result.ReadsPostgres || result.Manifest.GenerationID != manifest.GenerationID {
		t.Fatalf("result = %#v", result)
	}
	if fence.acquires != 1 || fence.releases != 1 || builder.calls != 1 || workingSet.publishCalls != 1 {
		t.Fatalf("rebuild sequencing fence=(%d,%d) builder=%d publish=%d", fence.acquires, fence.releases, builder.calls, workingSet.publishCalls)
	}
}

func TestBudgetBootstrapRequiresVerifiedNewIncarnation(t *testing.T) {
	manifest := testBudgetManifest(t)
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{IncarnationID: "redis-incarnation-unverified"}}
	coordinator := BudgetBootstrapCoordinator{Generation: &bootstrapGenerationFake{activeErr: bootstrapMissingActiveError()}, WorkingSet: &bootstrapWorkingSetFake{}, Fleet: fleet, Fence: &bootstrapFenceFake{}, Builder: &bootstrapBuilderFake{}}
	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapNewRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-unverified"})
	if !errors.Is(err, ErrBudgetBootstrapProofInvalid) || result.Reason != BudgetBootstrapProofUnavailable {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestBudgetBootstrapBlocksMalformedActivePointerEvenWithColdProof(t *testing.T) {
	manifest := testBudgetManifest(t)
	generation := &bootstrapGenerationFake{activeErr: fmt.Errorf("%w: %w", ErrBudgetManifestInvalid, ErrBudgetActiveGenerationMalformed)}
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{VerifiedColdStart: true}}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: &bootstrapWorkingSetFake{}, Fleet: fleet, Fence: &bootstrapFenceFake{}, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, ErrBudgetBootstrapSameIncarnation) || result.Reason != BudgetBootstrapSameIncarnationLoss {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if builder.calls != 0 {
		t.Fatalf("malformed pointer reached builder: %d", builder.calls)
	}
}

func TestBudgetBootstrapRejectsEmptyFenceToken(t *testing.T) {
	manifest := testBudgetManifest(t)
	generation := &bootstrapGenerationFake{activeErr: bootstrapMissingActiveError()}
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{VerifiedColdStart: true}}
	fence := &bootstrapFenceFake{empty: true}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: &bootstrapWorkingSetFake{}, Fleet: fleet, Fence: fence, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, ErrBudgetBootstrapFenceUnavailable) || result.Reason != BudgetBootstrapProofUnavailable {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if builder.calls != 0 {
		t.Fatalf("unfenced path reached builder: %d", builder.calls)
	}
}

func TestBudgetBootstrapRejectsCandidateIdentityMismatch(t *testing.T) {
	expectedManifest := testBudgetManifest(t)
	candidate := expectedManifest
	candidate.GenerationID = "wrong-generation"
	candidate.IncarnationID = "wrong-incarnation"
	generation := &bootstrapGenerationFake{activeErr: bootstrapMissingActiveError()}
	fleet := &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{VerifiedColdStart: true}}
	builder := &bootstrapBuilderFake{candidate: BudgetBootstrapCandidate{Manifest: candidate, WorkingSet: bootstrapWorkingSet(t, candidate)}}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: &bootstrapWorkingSetFake{}, Fleet: fleet, Fence: &bootstrapFenceFake{}, Builder: builder}

	result, err := coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(expectedManifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, ErrBudgetBootstrapProofInvalid) || result.Reason != BudgetBootstrapCandidateInvalid {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestBudgetBootstrapDoesNotRebuildOnWorkingSetTransportFailure(t *testing.T) {
	manifest := testBudgetManifest(t)
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	generation := &bootstrapGenerationFake{pointer: pointer, manifest: manifest}
	workingSet := &bootstrapWorkingSetFake{inspectErr: context.DeadlineExceeded}
	builder := &bootstrapBuilderFake{}
	coordinator := BudgetBootstrapCoordinator{Generation: generation, WorkingSet: workingSet, Fleet: &bootstrapFleetFake{proof: BudgetBootstrapFleetProof{VerifiedColdStart: true}}, Fence: &bootstrapFenceFake{}, Builder: builder}

	_, err = coordinator.Bootstrap(context.Background(), BudgetBootstrapRequest{Expected: bootstrapExpectation(manifest), Rebuild: BudgetBootstrapColdRebuild, GenerationID: "generation-2", IncarnationID: "redis-incarnation-new"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transport error = %v", err)
	}
	if builder.calls != 0 {
		t.Fatalf("working-set transport failure reached builder: %d", builder.calls)
	}
}
