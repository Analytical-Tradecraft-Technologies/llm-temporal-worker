package cloudstate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

const checkpointBlobPrefix = "llmtw_cpb_"

func (s *checkpointStore) blobStream(scope, mediaType string) string {
	data, _ := json.Marshal([]string{scope, mediaType})
	return "checkpoint-blob/" + s.repository.digest("checkpoint-blob-scope", data)
}

func (s *checkpointStore) Write(ctx context.Context, scope string, data []byte, mediaType string) (state.CheckpointBlobReference, error) {
	if err := validContext(ctx); err != nil {
		return state.CheckpointBlobReference{}, err
	}
	if !safeText(scope, 512) || !safeText(mediaType, 256) || len(data) > maxPayloadBytes {
		return state.CheckpointBlobReference{}, ErrInvalid
	}
	key, err := s.repository.writeBlob(ctx, s.blobStream(scope, mediaType), data)
	if err != nil {
		return state.CheckpointBlobReference{}, err
	}
	reference := state.CheckpointBlobReference{Digest: sha256.Sum256(data), ByteLength: int64(len(data)), MediaType: mediaType}
	tag := strings.TrimPrefix(key, s.repository.namespace+"/payload/")
	reference.ID = state.BlobID(checkpointBlobPrefix + tag + "." + s.blobProof(scope, tag, reference))
	return reference, nil
}

func (s *checkpointStore) blobProof(scope, tag string, reference state.CheckpointBlobReference) string {
	data, _ := json.Marshal(struct {
		Scope, Tag, Digest, MediaType string
		Length                        int64
	}{scope, tag, hex.EncodeToString(reference.Digest[:]), reference.MediaType, reference.ByteLength})
	return s.repository.digest("checkpoint-blob-reference", data)
}

func (s *checkpointStore) Read(ctx context.Context, scope string, reference state.CheckpointBlobReference) ([]byte, error) {
	if err := validContext(ctx); err != nil {
		return nil, err
	}
	if !safeText(scope, 512) || !safeText(reference.MediaType, 256) || reference.ByteLength < 0 || reference.ByteLength > maxPayloadBytes || reference.Digest == ([32]byte{}) {
		return nil, ErrInvalid
	}
	id := string(reference.ID)
	tag, proof, ok := strings.Cut(strings.TrimPrefix(id, checkpointBlobPrefix), ".")
	if !strings.HasPrefix(id, checkpointBlobPrefix) || !ok || !hexDigest(tag) || !hexDigest(proof) {
		return nil, ErrInvalid
	}
	// Reject a copied cross-scope reference or changed metadata before opening
	// any object. The ID is a scoped locator; it grants no authorization itself.
	if !hmac.Equal([]byte(proof), []byte(s.blobProof(scope, tag, reference))) {
		return nil, contracts.ErrNotFound
	}
	key := s.repository.namespace + "/payload/" + tag
	data, err := s.repository.readBlob(ctx, s.blobStream(scope, reference.MediaType), key)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != reference.ByteLength || sha256.Sum256(data) != reference.Digest {
		return nil, ErrCorrupt
	}
	return data, nil
}
