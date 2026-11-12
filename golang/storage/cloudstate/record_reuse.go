package cloudstate

import (
	"bytes"
	"context"
	"sync"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// WithRecordReuse starts a fresh invocation-local store of validated immutable
// request records. Repeated reads still resolve the authoritative event stream
// and enforce scope before reusing an exactly matching pointer. It must be
// called once per Activity, not on a worker's long-lived context (#1112).
// Nothing is persisted, and returned records never share their mutable bytes.
func WithRecordReuse(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, recordReuseKey{}, &recordReuse{records: make(map[recordReuseID]reusedRecord), checkpoints: make(map[checkpointReuseID]reusedCheckpoint), blobs: make(map[blobReuseID][]byte)})
}

type recordReuseKey struct{}
type recordReuseID struct {
	repository *Repository
	id         RequestID
}
type reusedRecord struct {
	pointer recordPointer
	record  Record
}

// Keep at most one revision per request and bound retained payload bytes. A full
// store only loses the optimization; it never changes storage behavior.
// Checkpoints and payloads have their own entry bounds and share the byte bound.
const recordReuseMaxEntries = 8
const checkpointReuseMaxEntries = 8
const blobReuseMaxEntries = 32
const recordReuseMaxBytes = 16 << 20

type recordReuse struct {
	mu      sync.Mutex
	records map[recordReuseID]reusedRecord
	// checkpoints holds committed checkpoints read in this invocation. Their
	// ID and operation rows are created once and never replaced or deleted,
	// so a committed checkpoint cannot change while an Activity runs.
	checkpoints map[checkpointReuseID]reusedCheckpoint
	// blobs holds payloads read and authenticated in this invocation. A
	// payload key is a keyed digest of its stream and plaintext, and nothing
	// deletes a payload, so a key always names the same bytes.
	blobs map[blobReuseID][]byte
	bytes int
}

type blobReuseID struct {
	repository  *Repository
	stream, key string
}

type checkpointReuseID struct {
	repository *Repository
	scope      string
	id         state.CheckpointID
}
type reusedCheckpoint struct {
	pointer checkpointPointer
	data    []byte
}

func recordReuseFrom(ctx context.Context) *recordReuse {
	if ctx == nil {
		return nil
	}
	reuse, _ := ctx.Value(recordReuseKey{}).(*recordReuse)
	return reuse
}

func cloneRecord(record Record) Record {
	record.Request.Manifest = bytes.Clone(record.Request.Manifest)
	record.Progress = bytes.Clone(record.Progress)
	return record
}

func recordBytes(record Record) int {
	return len(record.Request.Manifest) + len(record.Progress)
}

func (reuse *recordReuse) get(r *Repository, pointer recordPointer) (Record, bool) {
	if reuse == nil {
		return Record{}, false
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	entry, ok := reuse.records[recordReuseID{r, pointer.ID}]
	if !ok || entry.pointer != pointer {
		return Record{}, false
	}
	return cloneRecord(entry.record), true
}

func (reuse *recordReuse) remember(r *Repository, pointer recordPointer, record Record) {
	if reuse == nil {
		return
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	id := recordReuseID{r, pointer.ID}
	previous, exists := reuse.records[id]
	n := reuse.bytes - recordBytes(previous.record) + recordBytes(record)
	if (!exists && len(reuse.records) >= recordReuseMaxEntries) || n > recordReuseMaxBytes {
		return
	}
	reuse.records[id] = reusedRecord{pointer: pointer, record: cloneRecord(record)}
	reuse.bytes = n
}

// checkpoint returns the plaintext envelope and pointer of a committed
// checkpoint this invocation already read and validated. The caller decodes
// and validates it again, so no decoded value is ever shared.
func (reuse *recordReuse) checkpoint(r *Repository, scope string, id state.CheckpointID) (checkpointPointer, []byte, bool) {
	if reuse == nil {
		return checkpointPointer{}, nil, false
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	entry, ok := reuse.checkpoints[checkpointReuseID{r, scope, id}]
	if !ok {
		return checkpointPointer{}, nil, false
	}
	return entry.pointer, bytes.Clone(entry.data), true
}

// rememberCheckpoint keeps a committed checkpoint read from storage. Like
// records, only a decrypted, fully validated read may enter, never a write.
func (reuse *recordReuse) rememberCheckpoint(r *Repository, scope string, id state.CheckpointID, pointer checkpointPointer, data []byte) {
	if reuse == nil {
		return
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	key := checkpointReuseID{r, scope, id}
	if _, exists := reuse.checkpoints[key]; exists || len(reuse.checkpoints) >= checkpointReuseMaxEntries || reuse.bytes+len(data) > recordReuseMaxBytes {
		return
	}
	reuse.checkpoints[key] = reusedCheckpoint{pointer: pointer, data: bytes.Clone(data)}
	reuse.bytes += len(data)
}

// blob returns a payload this invocation already read and authenticated.
func (reuse *recordReuse) blob(r *Repository, stream, key string) ([]byte, bool) {
	if reuse == nil {
		return nil, false
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	data, ok := reuse.blobs[blobReuseID{r, stream, key}]
	if !ok {
		return nil, false
	}
	return bytes.Clone(data), true
}

// rememberBlob keeps an authenticated payload read from storage. A payload
// this invocation wrote enters only once it has been read back.
func (reuse *recordReuse) rememberBlob(r *Repository, stream, key string, data []byte) {
	if reuse == nil {
		return
	}
	reuse.mu.Lock()
	defer reuse.mu.Unlock()
	id := blobReuseID{r, stream, key}
	if _, exists := reuse.blobs[id]; exists || len(reuse.blobs) >= blobReuseMaxEntries || reuse.bytes+len(data) > recordReuseMaxBytes {
		return
	}
	reuse.blobs[id] = bytes.Clone(data)
	reuse.bytes += len(data)
}
