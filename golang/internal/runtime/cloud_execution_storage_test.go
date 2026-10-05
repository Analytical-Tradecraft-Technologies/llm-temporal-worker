package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

// These linearizable test doubles deliberately share no repository caches.
// Hooks inject failures before or after the backend has committed a mutation.
type executionMemoryTable struct {
	mu      sync.Mutex
	rows    map[kv.KeyValueKey]kv.KeyValueRecord
	version uint64
	hook    func(string, kv.KeyValueItem) (before, after error)
	trace   func(string)
}

func cloneExecutionItem(item kv.KeyValueItem) kv.KeyValueItem {
	encoded, err := item.Fields.MarshalBinary()
	if err != nil {
		panic(err)
	}
	var fields kv.KeyValueDocument
	if err := fields.UnmarshalBinary(encoded); err != nil {
		panic(err)
	}
	item.Fields = fields
	return item
}

func (s *executionMemoryTable) Get(ctx context.Context, key kv.KeyValueKey) (kv.KeyValueRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return kv.KeyValueRecord{}, err
	}
	r, ok := s.rows[key]
	if !ok {
		return kv.KeyValueRecord{}, contracts.ErrNotFound
	}
	r.Item = cloneExecutionItem(r.Item)
	return r, nil
}

func (s *executionMemoryTable) Create(ctx context.Context, item kv.KeyValueItem) (kv.KeyValueVersion, error) {
	return s.write(ctx, "create", item, "")
}

func (s *executionMemoryTable) Replace(ctx context.Context, item kv.KeyValueItem, version kv.KeyValueVersion) (kv.KeyValueVersion, error) {
	return s.write(ctx, "replace", item, version)
}

func (s *executionMemoryTable) write(ctx context.Context, op string, item kv.KeyValueItem, version kv.KeyValueVersion) (kv.KeyValueVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.trace != nil {
		s.trace(op + ":" + item.PartitionKey)
	}
	var after error
	if s.hook != nil {
		before, delayed := s.hook(op, item)
		if before != nil {
			return "", before
		}
		after = delayed
	}
	previous, exists := s.rows[item.Key()]
	if op == "create" && exists {
		return "", contracts.ErrAlreadyExists
	}
	if op == "replace" && (!exists || previous.Version != version || version == "") {
		return "", contracts.ErrConflict
	}
	s.version++
	next := kv.KeyValueVersion(fmt.Sprint(s.version))
	s.rows[item.Key()] = kv.KeyValueRecord{Item: cloneExecutionItem(item), Version: next, LastModifiedAt: time.Now().UTC()}
	if after != nil {
		return "", after
	}
	return next, nil
}

func (s *executionMemoryTable) Delete(ctx context.Context, key kv.KeyValueKey, version kv.KeyValueVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	r, exists := s.rows[key]
	if !exists || r.Version != version || version == "" {
		return contracts.ErrConflict
	}
	delete(s.rows, key)
	return nil
}

type executionTestCursor struct {
	Partition, Prefix, Last string
	Size                    int
}

func (s *executionMemoryTable) QueryPartition(ctx context.Context, query kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return kv.KeyValueQueryPage{}, err
	}
	if query.PageSize == 0 {
		query.PageSize = 100
	}
	cursor := executionTestCursor{Partition: query.PartitionKey, Prefix: query.SortKeyPrefix, Size: query.PageSize}
	if query.PageToken != "" {
		data, err := base64.RawURLEncoding.DecodeString(query.PageToken)
		var parsed executionTestCursor
		if err != nil || json.Unmarshal(data, &parsed) != nil || parsed.Partition != cursor.Partition || parsed.Prefix != cursor.Prefix || parsed.Size != cursor.Size {
			return kv.KeyValueQueryPage{}, contracts.ErrInvalidArgument
		}
		cursor = parsed
	}
	var keys []kv.KeyValueKey
	for key := range s.rows {
		if key.PartitionKey == query.PartitionKey && strings.HasPrefix(key.SortKey, query.SortKeyPrefix) && key.SortKey > cursor.Last {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].SortKey < keys[j].SortKey })
	var page kv.KeyValueQueryPage
	for _, key := range keys[:min(len(keys), query.PageSize)] {
		row := s.rows[key]
		row.Item = cloneExecutionItem(row.Item)
		page.Records = append(page.Records, row)
		cursor.Last = key.SortKey
	}
	if len(keys) > query.PageSize {
		data, _ := json.Marshal(cursor)
		page.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
	}
	return page, nil
}

type executionMemoryBlobs struct {
	mu     sync.Mutex
	values map[blob.BlobKey][]byte
	hook   func(blob.BlobKey) (before, after error)
	open   func(blob.BlobKey) error
	trace  func(string)
}

func (s *executionMemoryBlobs) Create(ctx context.Context, key blob.BlobKey, body io.Reader, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.trace != nil {
		s.trace("blob")
	}
	var after error
	if s.hook != nil {
		before, delayed := s.hook(key)
		if before != nil {
			return before
		}
		after = delayed
	}
	if _, exists := s.values[key]; exists {
		return contracts.ErrAlreadyExists
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return contracts.ErrInvalidArgument
	}
	s.values[key] = append([]byte(nil), data...)
	return after
}
func (s *executionMemoryBlobs) Open(ctx context.Context, key blob.BlobKey) (blob.BlobReadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return blob.BlobReadResult{}, err
	}
	if s.open != nil {
		if err := s.open(key); err != nil {
			return blob.BlobReadResult{}, err
		}
	}
	data, ok := s.values[key]
	if !ok {
		return blob.BlobReadResult{}, contracts.ErrNotFound
	}
	return blob.BlobReadResult{Body: io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), Size: int64(len(data))}, nil
}
func (s *executionMemoryBlobs) Delete(ctx context.Context, key blob.BlobKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(s.values, key)
	return nil
}
