package cloudstate

import (
	"bytes"
	"context"
	"sync"
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
	return context.WithValue(ctx, recordReuseKey{}, &recordReuse{records: make(map[recordReuseID]reusedRecord)})
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
const recordReuseMaxEntries = 8
const recordReuseMaxBytes = 16 << 20

type recordReuse struct {
	mu      sync.Mutex
	records map[recordReuseID]reusedRecord
	bytes   int
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
