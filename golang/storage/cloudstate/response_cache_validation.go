package cloudstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func validCacheKey(key cache.ResponseKey) bool {
	route := key.Route
	return safeText(key.ScopeID, 512) && (key.Operation == cache.OperationGenerate || key.Operation == cache.OperationCompact) &&
		key.Fingerprint != (cache.Fingerprint{}) && key.RequestIndex >= 0 &&
		safeText(string(route.Provider), 256) && safeText(string(route.Endpoint), 2048) &&
		safeText(string(route.Model), 512) && safeText(string(route.Revision), 512) && safeText(string(route.Compiler), 512) &&
		(route.Account == "" || safeText(string(route.Account), 512)) && (route.Region == "" || safeText(string(route.Region), 256))
}

func normalizeCacheEntry(entry cache.ResponseEntry) (cache.ResponseEntry, error) {
	if !validCacheKey(entry.Key) || !safeText(string(entry.ID), 256) || !safeText(string(entry.OriginOperationID), 4096) || !safeText(string(entry.OriginCheckpointID), 256) || !validTime(entry.CompletedAt) {
		return cache.ResponseEntry{}, ErrInvalid
	}
	response, err := objectJSON(entry.Response)
	if err != nil {
		return cache.ResponseEntry{}, err
	}
	var operation string
	var disposition string
	switch entry.Key.Operation {
	case cache.OperationGenerate:
		var value llm.GenerateResponseV1
		if json.Unmarshal(response, &value) != nil || (value.Status != llm.ResponseStatusCompleted && value.Status != llm.ResponseStatusToolCalls) || value.Checkpoint.Kind != "generation" {
			return cache.ResponseEntry{}, ErrInvalid
		}
		operation, disposition = value.OperationID, value.Cache.Disposition
	case cache.OperationCompact:
		var value llm.CompactResponseV1
		if json.Unmarshal(response, &value) != nil {
			return cache.ResponseEntry{}, ErrInvalid
		}
		operation, disposition = value.OperationID, value.Cache.Disposition
	}
	if operation != string(entry.OriginOperationID) || disposition == "hit" {
		return cache.ResponseEntry{}, ErrInvalid
	}
	entry.Response, entry.CompletedAt = response, entry.CompletedAt.UTC()
	return entry, nil
}

func validCacheUse(use cache.ResponseUse) bool {
	return safeText(use.ScopeID, 512) && safeText(string(use.OperationID), 4096) && safeText(string(use.EntryID), 256) && safeText(string(use.CheckpointID), 256) && validTime(use.CompletedAt)
}

func (s *responseCache) validateOrigin(ctx context.Context, entry cache.ResponseEntry) error {
	checkpoint, err := s.repository.Checkpoints().Get(ctx, entry.Key.ScopeID, entry.OriginCheckpointID)
	if err != nil {
		return err
	}
	kind := state.CheckpointGeneration
	if entry.Key.Operation == cache.OperationCompact {
		kind = state.CheckpointCompaction
	}
	if checkpoint.OriginOperationID != entry.OriginOperationID || checkpoint.Kind != kind || checkpoint.OriginCacheEntryID != nil || entry.CompletedAt.Before(checkpoint.CreatedAt) {
		return ErrInvalid
	}
	var metadata llm.CheckpointMetadata
	if entry.Key.Operation == cache.OperationGenerate {
		var response llm.GenerateResponseV1
		if json.Unmarshal(entry.Response, &response) != nil {
			return ErrInvalid
		}
		metadata = response.Checkpoint
		codec := state.CheckpointBlobCodec{MaxBytes: maxPayloadBytes}
		data, err := codec.EncodeResponse(response.Output)
		if err != nil || sha256.Sum256(data) != checkpoint.ResponseBlob.Digest {
			return ErrInvalid
		}
	} else {
		var response llm.CompactResponseV1
		if json.Unmarshal(entry.Response, &response) != nil {
			return ErrInvalid
		}
		metadata = response.Checkpoint
	}
	if metadata.Depth != checkpoint.Depth || (metadata.Parent == nil) != (checkpoint.ParentID == nil) {
		return ErrInvalid
	}
	return nil
}

func (s *responseCache) validateUse(ctx context.Context, use cache.ResponseUse, entry cache.ResponseEntry) error {
	checkpoint, err := s.repository.Checkpoints().Get(ctx, use.ScopeID, use.CheckpointID)
	if err != nil {
		return err
	}
	// Compaction replays retain the compaction kind in the public contract.
	kind := state.CheckpointCacheReplay
	if entry.Key.Operation == cache.OperationCompact {
		kind = state.CheckpointCompaction
	}
	if checkpoint.Kind != kind || checkpoint.OriginOperationID != use.OperationID || checkpoint.OriginCacheEntryID == nil || *checkpoint.OriginCacheEntryID != use.EntryID || use.CompletedAt.Before(checkpoint.CreatedAt) {
		return ErrInvalid
	}
	origin, err := s.repository.Checkpoints().Get(ctx, use.ScopeID, entry.OriginCheckpointID)
	if err != nil {
		return err
	}
	if checkpoint.ResponseBlob.Digest != origin.ResponseBlob.Digest {
		return ErrInvalid
	}
	return nil
}
