package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	events "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/eventsourcing"
	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/google/uuid"
	"time"
)

type langfuseState struct {
	Captures map[string]string `json:"captures"`
	Owner    string            `json:"owner,omitempty"`
	Until    time.Time         `json:"until"`
	Done     bool              `json:"done"`
}

func (r *Repository) langfuseStore() (*events.EventStreamStore[langfuseState], error) {
	return events.NewEventStreamStore(r.table, func() langfuseState { return langfuseState{Captures: map[string]string{}} }, func(ctx context.Context, old langfuseState, changes []events.EventChange) (langfuseState, error) {
		for _, change := range changes {
			var next langfuseState
			if change.Version != 1 || len(change.Data) > 1<<20 || json.Unmarshal(change.Data, &next) != nil || next.Captures == nil || len(next.Captures) > 4096 || (old.Done && !next.Done) {
				return langfuseState{}, ErrCorrupt
			}
			for id, key := range next.Captures {
				if !safeText(id, 256) || !r.validBlobKey(key) {
					return langfuseState{}, ErrCorrupt
				}
			}
			for id, key := range old.Captures {
				if next.Captures[id] != key {
					return langfuseState{}, ErrCorrupt
				}
			}
			old = next
		}
		return old, nil
	})
}
func (r *Repository) langfuseStream(id RequestID) string {
	return r.namespace + "/langfuse/" + string(id)
}
func (r *Repository) updateLangfuse(ctx context.Context, scope Scope, id RequestID, fn func(*langfuseState) error) error {
	if _, err := r.Read(ctx, scope, id); err != nil {
		return err
	}
	store, err := r.langfuseStore()
	if err != nil {
		return err
	}
	for tries := 0; tries < 8; tries++ {
		current, err := store.ReadState(ctx, r.langfuseStream(id))
		if err != nil {
			return err
		}
		next := current.Value
		if next.Captures == nil {
			next.Captures = map[string]string{}
		}
		if err := fn(&next); err != nil {
			return err
		}
		data, err := json.Marshal(next)
		if err != nil {
			return ErrInvalid
		}
		_, err = store.TryAppend(ctx, r.langfuseStream(id), current.Revision, events.EventAppend{AppendToken: r.digest("langfuse-update", data), ApplicationVersion: 1, Changes: []json.RawMessage{data}})
		if errors.Is(err, contracts.ErrConflict) {
			continue
		}
		return err
	}
	return contracts.ErrConflict
}

// SaveLangfuseCapture commits immutable encrypted input outside operation
// progress, which finalization replaces. It never authorizes a model call.
func (r *Repository) SaveLangfuseCapture(ctx context.Context, scope Scope, id RequestID, attempt string, data json.RawMessage) error {
	if _, err := r.Read(ctx, scope, id); err != nil {
		return err
	}
	if !safeText(attempt, 256) || !json.Valid(data) {
		return ErrInvalid
	}
	key, err := r.writeBlob(ctx, r.langfuseStream(id), data)
	if err != nil {
		return err
	}
	return r.updateLangfuse(ctx, scope, id, func(s *langfuseState) error {
		if old := s.Captures[attempt]; old != "" && old != key {
			return contracts.ErrConflict
		}
		if s.Done {
			return contracts.ErrConflict
		}
		s.Captures[attempt] = key
		return nil
	})
}
func (r *Repository) LoadLangfuseCaptures(ctx context.Context, scope Scope, id RequestID) (map[string]json.RawMessage, error) {
	if _, err := r.Read(ctx, scope, id); err != nil {
		return nil, err
	}
	store, err := r.langfuseStore()
	if err != nil {
		return nil, err
	}
	current, err := store.ReadState(ctx, r.langfuseStream(id))
	if err != nil {
		return nil, err
	}
	result := map[string]json.RawMessage{}
	for attempt, key := range current.Value.Captures {
		data, err := r.readReferencedBlob(ctx, r.langfuseStream(id), key)
		if err != nil {
			return nil, err
		}
		result[attempt] = data
	}
	return result, nil
}

// Acknowledgments suppress known successful exports. A lost HTTP response can
// still duplicate observations: Langfuse v4 does not deduplicate span IDs.
func (r *Repository) ClaimLangfuseExport(ctx context.Context, scope Scope, id RequestID, now time.Time) (string, error) {
	if !validTime(now) {
		return "", ErrInvalid
	}
	owner := uuid.NewString()
	done := false
	err := r.updateLangfuse(ctx, scope, id, func(s *langfuseState) error {
		if s.Done {
			done = true
			return nil
		}
		if s.Owner != "" && now.Before(s.Until) {
			return contracts.ErrConflict
		}
		s.Owner = owner
		s.Until = now.Add(10 * time.Second)
		return nil
	})
	if err != nil || done {
		return "", err
	}
	return owner, nil
}
func (r *Repository) AcknowledgeLangfuseExport(ctx context.Context, scope Scope, id RequestID, owner string) error {
	return r.updateLangfuse(ctx, scope, id, func(s *langfuseState) error {
		if s.Owner != owner {
			return contracts.ErrConflict
		}
		s.Done = true
		return nil
	})
}
func (r *Repository) ReleaseLangfuseExport(ctx context.Context, scope Scope, id RequestID, owner string) error {
	return r.updateLangfuse(ctx, scope, id, func(s *langfuseState) error {
		if s.Owner != owner || s.Done {
			return contracts.ErrConflict
		}
		s.Owner = ""
		s.Until = time.Time{}
		return nil
	})
}

// LangfuseID provides a scope-bound opaque identifier without exposing signed
// checkpoint handles or caller operation keys to Langfuse.
func (r *Repository) LangfuseID(scope Scope, domain, value string) string {
	return r.digest("langfuse-id/"+domain, []byte(r.scopeTag(scope)+"/"+value))
}

// LangfusePaidAttempts enumerates the root's deterministic attempt IDs without
// relying on the recovery index (completed attempts have left that index).
func (r *Repository) LangfusePaidAttempts(ctx context.Context, scope Scope, id RequestID) (map[RequestID]SavedProviderExecution, RequestID, error) {
	root, err := r.Read(ctx, scope, id)
	if err != nil {
		return nil, "", err
	}
	out := map[RequestID]SavedProviderExecution{}
	var last RequestID
	for n := uint64(1); n <= 4096; n++ {
		child := r.requestAttemptID(root, n)
		if _, err := r.Read(ctx, scope, child); errors.Is(err, contracts.ErrNotFound) {
			return out, last, nil
		} else if err != nil {
			return nil, "", err
		}
		last = child
		saved, err := r.LoadProviderExecution(ctx, scope, child)
		if errors.Is(err, ErrProviderExecutionMissing) || errors.Is(err, ErrBudgetPlanMissing) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		out[child] = saved
	}
	return nil, "", ErrInvalid
}
