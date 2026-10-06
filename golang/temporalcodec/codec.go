// Package temporalcodec provides the Temporal Payload Codec the worker uses
// to encrypt payloads before they reach Temporal history.
//
// The codec is exported so a Go caller, or a Temporal codec server built with
// converter.NewPayloadCodecHTTPHandler, can encode and decode the same
// payloads as the worker when given the same keys.
package temporalcodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	// EncodingEncrypted is the metadata encoding of an encrypted payload.
	EncodingEncrypted = "binary/encrypted"
	// MetadataCipher names the cipher and payload layout version.
	MetadataCipher = "encryption-cipher"
	// MetadataKeyID names the configured key that encrypted the payload.
	MetadataKeyID = "encryption-key-id"
	// CipherAES256GCMV1 is AES-256-GCM with a key derived by HKDF-SHA256 from
	// the configured secret. Data is a 12-byte random nonce followed by the
	// sealed, protobuf-encoded original payload.
	CipherAES256GCMV1 = "llmtw-aes256-gcm-v1"

	// MinSecretBytes is the minimum configured secret length.
	MinSecretBytes = 32

	hkdfInfo = "llm-temporal-worker temporal payload codec v1"
)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Key is one configured secret. Exactly one key is primary; it encrypts every
// new payload, while every key can decrypt.
type Key struct {
	ID      string
	Secret  []byte
	Primary bool
}

type aesGCMCodec struct {
	primary string
	aeads   map[string]cipher.AEAD
}

// NewAESGCM returns an AES-256-GCM PayloadCodec over keys. It rejects an
// empty key set, a malformed or duplicate ID, a secret shorter than
// MinSecretBytes, and anything other than exactly one primary key.
func NewAESGCM(keys []Key) (converter.PayloadCodec, error) {
	if len(keys) == 0 {
		return nil, errors.New("payload codec requires at least one key")
	}
	codec := &aesGCMCodec{aeads: make(map[string]cipher.AEAD, len(keys))}
	for _, key := range keys {
		if !keyIDPattern.MatchString(key.ID) {
			return nil, errors.New("payload codec key ID is invalid")
		}
		if _, exists := codec.aeads[key.ID]; exists {
			return nil, fmt.Errorf("duplicate payload codec key %q", key.ID)
		}
		if len(key.Secret) < MinSecretBytes {
			return nil, fmt.Errorf("payload codec key %q must be at least %d bytes", key.ID, MinSecretBytes)
		}
		if key.Primary {
			if codec.primary != "" {
				return nil, errors.New("payload codec requires exactly one primary key")
			}
			codec.primary = key.ID
		}
		derived, err := hkdf.Key(sha256.New, key.Secret, nil, hkdfInfo, 32)
		if err != nil {
			return nil, fmt.Errorf("derive payload codec key %q", key.ID)
		}
		block, err := aes.NewCipher(derived)
		if err != nil {
			return nil, fmt.Errorf("construct payload codec key %q", key.ID)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("construct payload codec key %q", key.ID)
		}
		codec.aeads[key.ID] = aead
	}
	if codec.primary == "" {
		return nil, errors.New("payload codec requires exactly one primary key")
	}
	return codec, nil
}

// additionalData binds the cipher and key ID metadata to the ciphertext, so
// relabelling a payload with another configured key fails authentication.
func additionalData(keyID string) []byte {
	return []byte(CipherAES256GCMV1 + "\x00" + keyID)
}

// Encode encrypts every payload with the primary key.
func (codec *aesGCMCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	aead := codec.aeads[codec.primary]
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		if payload == nil {
			continue
		}
		plaintext, err := payload.Marshal()
		if err != nil {
			return payloads, errors.New("payload codec could not serialize a payload")
		}
		nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(plaintext)+aead.Overhead())
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return payloads, errors.New("payload codec could not generate a nonce")
		}
		result[index] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(EncodingEncrypted),
				MetadataCipher:             []byte(CipherAES256GCMV1),
				MetadataKeyID:              []byte(codec.primary),
			},
			Data: aead.Seal(nonce, nonce, plaintext, additionalData(codec.primary)),
		}
	}
	return result, nil
}

// Decode decrypts payloads this codec encrypted with any configured key.
// Payloads with another encoding pass through unchanged, so histories written
// before the codec was enabled stay readable. An encrypted payload with an
// unknown cipher or key, or one that fails authentication, is an error.
func (codec *aesGCMCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	result := make([]*commonpb.Payload, len(payloads))
	for index, payload := range payloads {
		if payload == nil || string(payload.GetMetadata()[converter.MetadataEncoding]) != EncodingEncrypted {
			result[index] = payload
			continue
		}
		if string(payload.Metadata[MetadataCipher]) != CipherAES256GCMV1 {
			return payloads, errors.New("encrypted payload uses an unsupported cipher")
		}
		keyID := string(payload.Metadata[MetadataKeyID])
		aead, ok := codec.aeads[keyID]
		if !ok {
			if keyIDPattern.MatchString(keyID) {
				return payloads, fmt.Errorf("encrypted payload key %q is not configured", keyID)
			}
			return payloads, errors.New("encrypted payload key is not configured")
		}
		data := payload.Data
		if len(data) < aead.NonceSize()+aead.Overhead() {
			return payloads, errors.New("encrypted payload is truncated")
		}
		plaintext, err := aead.Open(nil, data[:aead.NonceSize()], data[aead.NonceSize():], additionalData(keyID))
		if err != nil {
			return payloads, errors.New("encrypted payload failed authentication")
		}
		decoded := &commonpb.Payload{}
		if err := decoded.Unmarshal(plaintext); err != nil {
			return payloads, errors.New("encrypted payload is malformed")
		}
		result[index] = decoded
	}
	return result, nil
}
