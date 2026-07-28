package redis

import (
	"context"
	"errors"
	"fmt"
)

// BudgetBootstrapOutcome is the only successful state exposed by the
// bootstrap coordinator. A caller must not treat a candidate or a fence as
// readiness; readiness is true only after Adopted or Rebuilt is returned.
type BudgetBootstrapOutcome string

const (
	BudgetBootstrapAdopted BudgetBootstrapOutcome = "adopted"
	BudgetBootstrapRebuilt BudgetBootstrapOutcome = "rebuilt"
	BudgetBootstrapBlocked BudgetBootstrapOutcome = "blocked"
)

// BudgetBootstrapReason describes why an outcome was reached. Reasons are
// intentionally bounded because they are safe to use in metrics and audit
// records without including deployment identifiers or provider data.
type BudgetBootstrapReason string

const (
	BudgetBootstrapIntact              BudgetBootstrapReason = "intact"
	BudgetBootstrapColdFleet           BudgetBootstrapReason = "cold_fleet"
	BudgetBootstrapNewIncarnation      BudgetBootstrapReason = "new_incarnation"
	BudgetBootstrapSameIncarnationLoss BudgetBootstrapReason = "same_incarnation_loss"
	BudgetBootstrapLiveSession         BudgetBootstrapReason = "live_or_reconnecting_session"
	BudgetBootstrapProofUnavailable    BudgetBootstrapReason = "proof_unavailable"
	BudgetBootstrapCandidateInvalid    BudgetBootstrapReason = "candidate_invalid"
	BudgetBootstrapPublishFailed       BudgetBootstrapReason = "publish_failed"
)

var (
	// ErrBudgetBootstrapBlocked means that paid work must remain disabled. It
	// is returned for an expected, fail-closed state rather than a transport
	// failure.
	ErrBudgetBootstrapBlocked          = errors.New("budget bootstrap blocked")
	ErrBudgetBootstrapSameIncarnation  = errors.New("same-incarnation budget loss")
	ErrBudgetBootstrapLiveWorkers      = errors.New("budget workers are live or reconnecting")
	ErrBudgetBootstrapProofInvalid     = errors.New("budget bootstrap proof is invalid")
	ErrBudgetBootstrapFenceUnavailable = errors.New("budget bootstrap fence is unavailable")
	ErrBudgetWorkingSetInvalid         = errors.New("budget working set is invalid")
)

// BudgetWorkingSetProof is produced by a deployment-owned inspector. A
// manifest proves metadata only; adoption additionally requires this proof to
// show that all generation members and buckets are present in Redis.
// ReservationCount is bounded and may be zero for an idle generation.
type BudgetWorkingSetProof struct {
	Complete            bool
	ManifestDigest      string
	MemberCatalogDigest string
	MemberCount         int
	BucketCount         int
	ReservationCount    int
	StreamHighWaterMark string
}

// BudgetGenerationWorkingSetPort is deliberately separate from
// BudgetGenerationPort. The latter can read and publish metadata; this port
// must inspect the complete generation working set and atomically publish a
// rebuilt generation together with its generation-switch event.
type BudgetGenerationWorkingSetPort interface {
	Inspect(context.Context, ActiveBudgetGeneration, BudgetManifestExpectation) (BudgetWorkingSetProof, error)
	PublishAndSwitch(context.Context, string, BudgetBootstrapCandidate, BudgetStreamEvent) (ActiveBudgetGeneration, error)
}

// BudgetBootstrapFleetProof is an explicit quiescence assertion. Empty Redis
// keys are not evidence of a new dataset incarnation, so a deployment must
// provide VerifiedNewIncarnation when it wants to use that recovery path.
type BudgetBootstrapFleetProof struct {
	LiveCount         int
	ReconnectingCount int
	// VerifiedColdStart is an explicit deployment assertion that the Redis
	// dataset is intentionally empty for a first bootstrap. It must not be
	// inferred from a missing pointer or an empty key scan.
	VerifiedColdStart      bool
	VerifiedNewIncarnation bool
	IncarnationID          BudgetIncarnationID
}

// BudgetBootstrapFleetPort reports liveness and reconnecting sessions without
// reading PostgreSQL. A worker roster is persistent across lease expiry and is
// therefore counted as reconnecting until deployment-owned maintenance proves
// it no longer represents a process that can resume.
type BudgetBootstrapFleetPort interface {
	Prove(context.Context, BudgetGenerationID, BudgetIncarnationID) (BudgetBootstrapFleetProof, error)
}

// BudgetBootstrapFence is a Redis-side lease which prevents two rebuilds from
// publishing competing generations. Its implementation must use a fenced
// Redis Function or equivalent atomic primitive.
type BudgetBootstrapFence interface {
	Acquire(context.Context, BudgetGenerationID, BudgetIncarnationID) (string, error)
	Release(context.Context, string) error
}

// BudgetBootstrapBuilder is the exceptional rebuild boundary. Implementations
// may read PostgreSQL only after the coordinator has proved rebuild eligibility
// and acquired the Redis fence. Build must create a new immutable candidate;
// it must not mutate the active generation in place.
type BudgetBootstrapBuilder interface {
	Build(context.Context, BudgetBootstrapBuildRequest) (BudgetBootstrapCandidate, error)
}

type BudgetBootstrapBuildRequest struct {
	Expected      BudgetManifestExpectation
	GenerationID  BudgetGenerationID
	IncarnationID BudgetIncarnationID
}

// BudgetBootstrapCandidate contains both immutable metadata and the complete
// working-set proof that the committer will validate before switching the
// active pointer.
type BudgetBootstrapCandidate struct {
	Manifest   BudgetManifest
	WorkingSet BudgetWorkingSetProof
}

// BudgetBootstrapResult is safe to retain as release evidence. ReadsPostgres
// is an assertion for tests/metrics and is true only for a successful fenced
// rebuild.
type BudgetBootstrapResult struct {
	Outcome       BudgetBootstrapOutcome
	Reason        BudgetBootstrapReason
	Active        ActiveBudgetGeneration
	Manifest      BudgetManifest
	ReadsPostgres bool
}

// BudgetBootstrapRequest carries immutable expectations and an explicit
// rebuild authorization. ColdFleet is permitted only after zero live and zero
// reconnecting workers. NewIncarnation additionally requires a verified
// incarnation proof matching IncarnationID.
type BudgetBootstrapRequest struct {
	Expected      BudgetManifestExpectation
	Rebuild       BudgetBootstrapRebuildMode
	GenerationID  BudgetGenerationID
	IncarnationID BudgetIncarnationID
}

type BudgetBootstrapRebuildMode string

const (
	BudgetBootstrapNoRebuild   BudgetBootstrapRebuildMode = ""
	BudgetBootstrapColdRebuild BudgetBootstrapRebuildMode = "cold_fleet"
	BudgetBootstrapNewRebuild  BudgetBootstrapRebuildMode = "new_incarnation"
)

// BudgetBootstrapCoordinator implements the fail-closed observe → fence →
// re-observe → build → atomic publish transition. It is intentionally not
// wired into the default runtime: deployment code must provide all ports,
// credentials, and protected cross-store tests before enabling it.
type BudgetBootstrapCoordinator struct {
	Generation BudgetGenerationPort
	WorkingSet BudgetGenerationWorkingSetPort
	Fleet      BudgetBootstrapFleetPort
	Fence      BudgetBootstrapFence
	Builder    BudgetBootstrapBuilder
}

// Bootstrap first attempts Redis-only adoption. PostgreSQL is reachable only
// after adoption fails, fleet quiescence/new-incarnation proof succeeds, and a
// fence is held. A same-incarnation loss is never repaired online.
func (coordinator BudgetBootstrapCoordinator) Bootstrap(ctx context.Context, request BudgetBootstrapRequest) (BudgetBootstrapResult, error) {
	if err := coordinator.validate(request); err != nil {
		return BudgetBootstrapResult{}, err
	}
	observation, observed := coordinator.observe(ctx, request.Expected)
	if observed == nil {
		return coordinator.adopted(observation)
	}
	if !errors.Is(observed, ErrBudgetManifestInvalid) {
		return BudgetBootstrapResult{}, observed
	}
	if observation.activeErr != nil && !errors.Is(observation.activeErr, ErrBudgetActiveGenerationMissing) && request.Rebuild != BudgetBootstrapNewRebuild {
		return coordinator.blocked(BudgetBootstrapSameIncarnationLoss, ErrBudgetBootstrapSameIncarnation)
	}
	if observation.pointer.IncarnationID != "" && observation.pointer.IncarnationID == request.Expected.IncarnationID {
		return coordinator.blocked(BudgetBootstrapSameIncarnationLoss, ErrBudgetBootstrapSameIncarnation)
	}
	if err := coordinator.ensureEligible(ctx, request, observation); err != nil {
		return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: reasonForError(err)}, fmt.Errorf("%w: %w", ErrBudgetBootstrapBlocked, err)
	}
	lease, err := coordinator.Fence.Acquire(ctx, request.Expected.GenerationID, request.IncarnationID)
	if err != nil {
		return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: BudgetBootstrapProofUnavailable}, fmt.Errorf("%w: acquire bootstrap fence: %v", ErrBudgetBootstrapFenceUnavailable, err)
	}
	if lease == "" {
		return coordinator.blocked(BudgetBootstrapProofUnavailable, ErrBudgetBootstrapFenceUnavailable)
	}
	defer func() { _ = coordinator.Fence.Release(context.WithoutCancel(ctx), lease) }()

	// Another worker may have completed the generation while this worker was
	// waiting for the fence. Adoption always wins over a rebuild.
	observation, observed = coordinator.observe(ctx, request.Expected)
	if observed == nil {
		return coordinator.adopted(observation)
	}
	if !errors.Is(observed, ErrBudgetManifestInvalid) {
		return BudgetBootstrapResult{}, observed
	}
	if observation.activeErr != nil && !errors.Is(observation.activeErr, ErrBudgetActiveGenerationMissing) && request.Rebuild != BudgetBootstrapNewRebuild {
		return coordinator.blocked(BudgetBootstrapSameIncarnationLoss, ErrBudgetBootstrapSameIncarnation)
	}
	if observation.pointer.IncarnationID != "" && observation.pointer.IncarnationID == request.Expected.IncarnationID {
		return coordinator.blocked(BudgetBootstrapSameIncarnationLoss, ErrBudgetBootstrapSameIncarnation)
	}
	if err := coordinator.ensureEligible(ctx, request, observation); err != nil {
		return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: reasonForError(err)}, fmt.Errorf("%w: %w", ErrBudgetBootstrapBlocked, err)
	}
	candidate, err := coordinator.Builder.Build(ctx, BudgetBootstrapBuildRequest{Expected: request.Expected, GenerationID: request.GenerationID, IncarnationID: request.IncarnationID})
	if err != nil {
		return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: BudgetBootstrapCandidateInvalid}, fmt.Errorf("%w: build candidate: %v", ErrBudgetBootstrapBlocked, err)
	}
	if err := validateBootstrapCandidate(candidate, request); err != nil {
		return coordinator.blocked(BudgetBootstrapCandidateInvalid, err)
	}
	event := BudgetStreamEvent{
		Schema:       budgetStreamEventSchema,
		Kind:         BudgetEventGenerationSwitch,
		GenerationID: candidate.Manifest.GenerationID,
		Revision:     candidate.Manifest.JournalHighWaterMark,
		OccurredAt:   candidate.Manifest.CoverageEnd,
	}
	active, err := coordinator.WorkingSet.PublishAndSwitch(ctx, lease, candidate, event)
	if err != nil {
		return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: BudgetBootstrapPublishFailed}, fmt.Errorf("%w: publish candidate: %v", ErrBudgetBootstrapBlocked, err)
	}
	if err := active.ValidateAgainst(candidate.Manifest); err != nil {
		return coordinator.blocked(BudgetBootstrapPublishFailed, err)
	}
	return BudgetBootstrapResult{Outcome: BudgetBootstrapRebuilt, Reason: rebuildReason(request.Rebuild), Active: active, Manifest: candidate.Manifest, ReadsPostgres: true}, nil
}

type bootstrapObservation struct {
	pointer   ActiveBudgetGeneration
	manifest  BudgetManifest
	activeErr error
}

func (coordinator BudgetBootstrapCoordinator) observe(ctx context.Context, expected BudgetManifestExpectation) (bootstrapObservation, error) {
	pointer, err := coordinator.Generation.ActiveGeneration(ctx)
	if err != nil {
		return bootstrapObservation{activeErr: err}, err
	}
	manifest, err := coordinator.Generation.LoadManifest(ctx, pointer)
	if err != nil {
		return bootstrapObservation{pointer: pointer}, err
	}
	if err := pointer.ValidateAgainst(manifest); err != nil {
		return bootstrapObservation{pointer: pointer, manifest: manifest}, err
	}
	if err := manifest.ValidateAgainst(expected); err != nil {
		return bootstrapObservation{pointer: pointer, manifest: manifest}, err
	}
	proof, err := coordinator.WorkingSet.Inspect(ctx, pointer, expected)
	if err != nil {
		if errors.Is(err, ErrBudgetWorkingSetInvalid) || errors.Is(err, ErrBudgetManifestInvalid) {
			return bootstrapObservation{pointer: pointer, manifest: manifest}, fmt.Errorf("%w: inspect working set: %w", ErrBudgetManifestInvalid, err)
		}
		return bootstrapObservation{pointer: pointer, manifest: manifest}, err
	}
	if err := validateWorkingSetProof(manifest, proof); err != nil {
		return bootstrapObservation{pointer: pointer, manifest: manifest}, err
	}
	return bootstrapObservation{pointer: pointer, manifest: manifest}, nil
}

func (coordinator BudgetBootstrapCoordinator) ensureEligible(ctx context.Context, request BudgetBootstrapRequest, observation bootstrapObservation) error {
	generationID := observation.pointer.GenerationID
	if generationID == "" {
		generationID = request.Expected.GenerationID
	}
	incarnationID := observation.pointer.IncarnationID
	if incarnationID == "" {
		incarnationID = request.Expected.IncarnationID
	}
	proof, err := coordinator.Fleet.Prove(ctx, generationID, incarnationID)
	if err != nil {
		return fmt.Errorf("%w: fleet proof: %v", ErrBudgetBootstrapProofInvalid, err)
	}
	if proof.LiveCount < 0 || proof.ReconnectingCount < 0 || (proof.VerifiedNewIncarnation && proof.IncarnationID == "") {
		return fmt.Errorf("%w: malformed fleet proof", ErrBudgetBootstrapProofInvalid)
	}
	if proof.LiveCount != 0 || proof.ReconnectingCount != 0 {
		return ErrBudgetBootstrapLiveWorkers
	}
	switch request.Rebuild {
	case BudgetBootstrapColdRebuild:
		if request.IncarnationID == "" || request.GenerationID == "" || !proof.VerifiedColdStart {
			return fmt.Errorf("%w: cold rebuild requires a target incarnation", ErrBudgetBootstrapProofInvalid)
		}
	case BudgetBootstrapNewRebuild:
		if request.GenerationID == "" || !proof.VerifiedNewIncarnation || proof.IncarnationID != request.IncarnationID || request.IncarnationID == request.Expected.IncarnationID {
			return fmt.Errorf("%w: new-incarnation proof does not match request", ErrBudgetBootstrapProofInvalid)
		}
	default:
		return fmt.Errorf("%w: rebuild authorization is required", ErrBudgetBootstrapProofInvalid)
	}
	return nil
}

func (coordinator BudgetBootstrapCoordinator) validate(request BudgetBootstrapRequest) error {
	if coordinator.Generation == nil || coordinator.WorkingSet == nil || coordinator.Fleet == nil || coordinator.Fence == nil || coordinator.Builder == nil {
		return errors.New("all budget bootstrap ports are required")
	}
	if request.Expected.IncarnationID == "" {
		return errors.New("expected Redis incarnation is required")
	}
	if request.Expected.ConfigVersion == "" || request.Expected.PriceVersion == "" || request.Expected.PolicyHash == "" || request.Expected.WindowHash == "" || request.Expected.CoverageStart.IsZero() || request.Expected.CoverageEnd.IsZero() || request.Expected.RoundingVersion == "" || request.Expected.Members == nil {
		return errors.New("complete immutable budget expectation is required")
	}
	if request.Rebuild != BudgetBootstrapNoRebuild && request.IncarnationID == "" {
		return errors.New("target Redis incarnation is required")
	}
	if request.Rebuild != BudgetBootstrapNoRebuild && request.GenerationID == "" {
		return errors.New("target budget generation is required")
	}
	if request.Rebuild != BudgetBootstrapNoRebuild && request.IncarnationID == request.Expected.IncarnationID {
		return errors.New("rebuild target must use a new Redis incarnation")
	}
	if request.Rebuild != BudgetBootstrapNoRebuild && request.Expected.GenerationID != "" && request.GenerationID == request.Expected.GenerationID {
		return errors.New("rebuild target must use a new budget generation")
	}
	return nil
}

func validateWorkingSetProof(manifest BudgetManifest, proof BudgetWorkingSetProof) error {
	if !proof.Complete || proof.ManifestDigest == "" || proof.ManifestDigest != mustManifestDigest(manifest) || proof.MemberCatalogDigest != manifest.MemberCatalogDigest || proof.MemberCount != len(manifest.Members) || proof.BucketCount != manifest.BucketCount || proof.ReservationCount < 0 || proof.StreamHighWaterMark != manifest.StreamHighWaterMark {
		return fmt.Errorf("%w: %w: working set is incomplete or does not match manifest", ErrBudgetManifestInvalid, ErrBudgetWorkingSetInvalid)
	}
	return nil
}

func validateBootstrapCandidate(candidate BudgetBootstrapCandidate, request BudgetBootstrapRequest) error {
	if err := candidate.Manifest.Validate(); err != nil {
		return err
	}
	if candidate.Manifest.GenerationID != request.GenerationID || candidate.Manifest.IncarnationID != request.IncarnationID {
		return fmt.Errorf("%w: candidate identity does not match rebuild target", ErrBudgetBootstrapProofInvalid)
	}
	if candidate.Manifest.JournalHighWaterMark < request.Expected.JournalHighWaterMark {
		return fmt.Errorf("%w: candidate journal watermark regressed", ErrBudgetBootstrapProofInvalid)
	}
	expected := request.Expected
	// A rebuild advances generation/incarnation and may advance the Stream and
	// journal watermarks. The immutable catalog/provenance and active horizon
	// remain required to match the deployment expectation.
	expected.GenerationID = ""
	expected.IncarnationID = ""
	expected.StreamHighWaterMark = ""
	expected.JournalHighWaterMark = 0
	if err := candidate.Manifest.ValidateAgainst(expected); err != nil {
		return err
	}
	return validateWorkingSetProof(candidate.Manifest, candidate.WorkingSet)
}

func mustManifestDigest(manifest BudgetManifest) string {
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		return ""
	}
	return digest
}

func (coordinator BudgetBootstrapCoordinator) adopted(observation bootstrapObservation) (BudgetBootstrapResult, error) {
	return BudgetBootstrapResult{Outcome: BudgetBootstrapAdopted, Reason: BudgetBootstrapIntact, Active: observation.pointer, Manifest: observation.manifest}, nil
}

func (coordinator BudgetBootstrapCoordinator) blocked(reason BudgetBootstrapReason, err error) (BudgetBootstrapResult, error) {
	return BudgetBootstrapResult{Outcome: BudgetBootstrapBlocked, Reason: reason}, fmt.Errorf("%w: %w", ErrBudgetBootstrapBlocked, err)
}

func reasonForError(err error) BudgetBootstrapReason {
	switch {
	case errors.Is(err, ErrBudgetBootstrapSameIncarnation):
		return BudgetBootstrapSameIncarnationLoss
	case errors.Is(err, ErrBudgetBootstrapLiveWorkers):
		return BudgetBootstrapLiveSession
	default:
		return BudgetBootstrapProofUnavailable
	}
}

func rebuildReason(mode BudgetBootstrapRebuildMode) BudgetBootstrapReason {
	if mode == BudgetBootstrapNewRebuild {
		return BudgetBootstrapNewIncarnation
	}
	return BudgetBootstrapColdFleet
}
