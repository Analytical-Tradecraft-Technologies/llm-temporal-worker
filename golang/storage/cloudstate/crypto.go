package cloudstate

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
)

func derive(key []byte, domain string, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("llmtw/cloudstate/v1/" + domain + "\x00"))
	m.Write(data)
	return m.Sum(nil)
}

func newCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(derive(key, "encryption", nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (r *Repository) digest(domain string, data []byte) string {
	return hex.EncodeToString(derive(r.secret, domain, data))
}

func (r *Repository) blobKey(stream string, data []byte) string {
	return r.namespace + "/payload/" + r.digest("payload/"+stream, data)
}

func (r *Repository) writeBlob(ctx context.Context, stream string, data []byte) (string, error) {
	if len(data) > maxPayloadBytes {
		return "", ErrInvalid
	}
	key := r.blobKey(stream, data)
	nonce := make([]byte, r.cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := r.cipher.Seal(nonce, nonce, data, []byte(key))
	err := r.blobs.Create(ctx, blob.BlobKey(key), bytes.NewReader(sealed), int64(len(sealed)))
	if err == nil {
		return key, nil
	}
	// Never reinterpret an uncertain write as a definite conflict. A later
	// retry may safely reconcile an existing immutable object by reading it.
	if errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, contracts.ErrAlreadyExists) {
		return "", err
	}
	stored, err := r.readBlob(ctx, stream, key)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(stored, data) {
		return "", ErrCorrupt
	}
	return key, nil
}

func (r *Repository) readBlob(ctx context.Context, stream, key string) ([]byte, error) {
	if !r.validBlobKey(key) {
		return nil, ErrCorrupt
	}
	opened, err := r.blobs.Open(ctx, blob.BlobKey(key))
	if err != nil {
		return nil, err
	}
	if opened.Body == nil {
		return nil, ErrCorrupt
	}
	defer opened.Body.Close()
	limit := int64(maxPayloadBytes + r.cipher.NonceSize() + r.cipher.Overhead())
	if opened.Size < int64(r.cipher.NonceSize()+r.cipher.Overhead()) || opened.Size > limit {
		return nil, ErrCorrupt
	}
	data, err := io.ReadAll(io.LimitReader(opened.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != opened.Size {
		return nil, ErrCorrupt
	}
	n := r.cipher.NonceSize()
	plain, err := r.cipher.Open(nil, data[:n], data[n:], []byte(key))
	if err != nil {
		return nil, ErrCorrupt
	}
	if r.blobKey(stream, plain) != key {
		return nil, ErrCorrupt
	}
	return plain, nil
}

func (r *Repository) validBlobKey(key string) bool {
	prefix := r.namespace + "/payload/"
	if len(key) != len(prefix)+64 || key[:len(prefix)] != prefix {
		return false
	}
	decoded, err := hex.DecodeString(key[len(prefix):])
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == key[len(prefix):]
}
