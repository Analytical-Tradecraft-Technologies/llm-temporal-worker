package cloudstate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
)

type openedBlobs struct {
	blob.BlobStore
	result blob.BlobReadResult
}

func (b openedBlobs) Open(context.Context, blob.BlobKey) (blob.BlobReadResult, error) {
	return b.result, nil
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

type brokenReader struct{ err error }

func (b brokenReader) Read([]byte) (int, error) { return 0, b.err }

func TestPayloadReadLimitsAndBodyOwnership(t *testing.T) {
	for _, kind := range []string{"nil", "negative", "oversized-declared", "oversized-body", "truncated", "trailing", "reader-failure"} {
		t.Run(kind, func(t *testing.T) {
			r, _, blobs, request := fixture(t)
			key, err := r.writeBlob(context.Background(), r.stream(request.ID), []byte("payload"))
			if err != nil {
				t.Fatal(err)
			}
			data := blobs.values[blob.BlobKey(key)]
			body := &trackedBody{Reader: bytes.NewReader(data)}
			opened := blob.BlobReadResult{Body: body, Size: int64(len(data))}
			failure := errors.New("read failed")
			switch kind {
			case "nil":
				opened.Body = nil
			case "negative":
				opened.Size = -1
			case "oversized-declared":
				opened.Size = maxPayloadBytes + 100
			case "oversized-body":
				body.Reader = strings.NewReader(strings.Repeat("x", maxPayloadBytes+100))
			case "truncated":
				body.Reader = bytes.NewReader(data[:len(data)-1])
			case "trailing":
				body.Reader = bytes.NewReader(append(append([]byte(nil), data...), 0))
			case "reader-failure":
				body.Reader = brokenReader{err: failure}
			}
			r.blobs = openedBlobs{BlobStore: blobs, result: opened}
			_, err = r.readBlob(context.Background(), r.stream(request.ID), key)
			want := ErrCorrupt
			if kind == "reader-failure" {
				want = failure
			}
			if !errors.Is(err, want) {
				t.Fatalf("read accepted %s: %v", kind, err)
			}
			if kind != "nil" && !body.closed {
				t.Fatal("leaked blob body")
			}
		})
	}
}

func TestPayloadBindingRejectsCrossRequestAndNamespaceReads(t *testing.T) {
	r, _, blobs, request := fixture(t)
	ctx := context.Background()
	key, err := r.writeBlob(ctx, r.stream(request.ID), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.readBlob(ctx, r.stream(otherID), key); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cross-request blob: %v", err)
	}
	otherKey := strings.Replace(key, r.namespace, "different", 1)
	blobs.values[blob.BlobKey(otherKey)] = append([]byte(nil), blobs.values[blob.BlobKey(key)]...)
	r.namespace = "different"
	if _, err := r.readBlob(ctx, r.stream(request.ID), otherKey); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cross-namespace blob: %v", err)
	}
}

func TestOversizedRecordRejectedBeforeIndex(t *testing.T) {
	r, table, blobs, request := fixture(t)
	request.Manifest = []byte(`{"text":"` + strings.Repeat("x", maxPayloadBytes-20) + `"}`)
	if _, err := r.Create(context.Background(), request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized envelope: %v", err)
	}
	if len(table.rows) != 0 || len(blobs.values) != 0 {
		t.Fatal("oversized record wrote recovery state")
	}
}
