package cloudstate

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
)

// encodeProgress marshals request progress for an update, rejecting a value
// over the payload limit as invalid before any write is attempted.
func encodeProgress(progress map[string]json.RawMessage) (json.RawMessage, error) {
	data, err := json.Marshal(progress)
	if err != nil || len(data) > maxPayloadBytes {
		return nil, ErrInvalid
	}
	return data, nil
}

// tryProgressUpdate replaces id's progress at record's read revision, never
// moving UpdatedAt backwards from record. retry reports a lost revision race,
// which callers resolve by reading the record again; any other result,
// including success, is final.
func (r *Repository) tryProgressUpdate(ctx context.Context, scope Scope, id RequestID, record Record, token string, status Status, progress json.RawMessage, now time.Time) (retry bool, err error) {
	if now.Before(record.UpdatedAt) {
		now = record.UpdatedAt
	}
	_, err = r.TryUpdate(ctx, scope, id, Update{ExpectedRevision: record.Revision, Token: token, Status: status, Progress: progress, UpdatedAt: now})
	return errors.Is(err, contracts.ErrConflict), err
}
