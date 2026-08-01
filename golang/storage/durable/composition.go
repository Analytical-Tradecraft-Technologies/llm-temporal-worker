package durable

import (
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

// ErrCompositionBuilderInvalid identifies a snapshot composition that cannot
// prove that all of its durable ports belong to the same state identity.
// Composition construction is deliberately side-effect free: callers must
// validate the complete value before handing it to a runtime or an Activity.
var ErrCompositionBuilderInvalid = errors.New("durable composition builder is invalid")

// CompositionPorts is the complete storage-neutral port set owned by one
// immutable runtime snapshot. Keeping the ports in one value makes it
// possible for a factory to copy and validate the set atomically instead of
// composing Redis and PostgreSQL capabilities independently during a reload.
// The interfaces intentionally expose no concrete client, pool, or keyring.
type CompositionPorts struct {
	Operations    admission.AdmissionStore
	Continuations state.ContinuationStore
	Results       ResultStore
	Journal       Journal
	Materializer  BudgetMaterializer
}

// CompositionBuilder constructs one snapshot-owned Composition. It does not
// dial services, create schema, read PostgreSQL budget state, or dispatch a
// provider request. Deployment code supplies already-scoped ports and may
// retain the resulting value for the lifetime of its immutable configuration
// snapshot.
type CompositionBuilder struct {
	Identity StateIdentity
	Ports    CompositionPorts
}

// Build validates every port and returns a value whose BudgetBoundary and
// Lifecycle helpers are bound to the same identity. A missing or typed-nil
// port fails closed before any port method can be called.
func (builder CompositionBuilder) Build() (Composition, error) {
	composition := Composition{
		Identity:      builder.Identity,
		Operations:    builder.Ports.Operations,
		Continuations: builder.Ports.Continuations,
		Results:       builder.Ports.Results,
		Journal:       builder.Ports.Journal,
		Materializer:  builder.Ports.Materializer,
	}
	if err := composition.Validate(); err != nil {
		return Composition{}, fmt.Errorf("%w: %v", ErrCompositionBuilderInvalid, err)
	}
	return composition, nil
}

// BudgetBoundary returns the Redis/PostgreSQL handoff for this composition.
// The boundary is reconstructed from the composition's immutable ports so it
// cannot accidentally use a journal or materializer from another snapshot.
func (composition Composition) BudgetBoundary() BudgetBoundary {
	return BudgetBoundary{
		Identity:     composition.Identity,
		Journal:      composition.Journal,
		Materializer: composition.Materializer,
	}
}

// NewLifecycle returns an empty lifecycle for an operation handled by this
// composition. Lifecycle values carry only per-operation phase and replay
// identity; they never retain mutable state in the snapshot composition.
func (composition Composition) NewLifecycle() Lifecycle { return Lifecycle{} }
