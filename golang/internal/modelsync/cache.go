package modelsync

import "sync"

// Response cache bounds. One entry per model's /endpoints lookup plus the
// three list responses; the byte bound keeps a pathological catalog from
// holding more than a few Documents' worth of memory.
const (
	maxCachedResponses     = MaxModels + 8
	maxCachedResponseBytes = 64 << 20
)

// ResponseCache keeps OpenRouter responses that carried an ETag, keyed by
// URL, so a later fetch can revalidate them with If-None-Match (#1555). The
// zero value is ready to use and safe for concurrent use. It holds only
// OpenRouter's public list responses.
type ResponseCache struct {
	mu      sync.Mutex
	entries map[string]cachedResponse
	bytes   int
}

type cachedResponse struct {
	etag string
	body []byte
}

func (cache *ResponseCache) lookup(url string) (cachedResponse, bool) {
	if cache == nil {
		return cachedResponse{}, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, found := cache.entries[url]
	return entry, found
}

// store records a response, evicting arbitrary entries to stay within the
// bounds. A response larger than the byte bound is not cached.
func (cache *ResponseCache) store(url, etag string, body []byte) {
	if cache == nil || len(body) > maxCachedResponseBytes {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.entries == nil {
		cache.entries = make(map[string]cachedResponse)
	}
	if previous, found := cache.entries[url]; found {
		cache.bytes -= len(previous.body)
		delete(cache.entries, url)
	}
	for victim, entry := range cache.entries {
		if len(cache.entries) < maxCachedResponses && cache.bytes+len(body) <= maxCachedResponseBytes {
			break
		}
		cache.bytes -= len(entry.body)
		delete(cache.entries, victim)
	}
	cache.entries[url] = cachedResponse{etag: etag, body: append([]byte(nil), body...)}
	cache.bytes += len(body)
}
