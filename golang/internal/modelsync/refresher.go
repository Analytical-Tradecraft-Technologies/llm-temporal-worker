package modelsync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"
)

const (
	defaultPollInterval = time.Minute
	defaultLeaseTTL     = 5 * time.Minute
	defaultFetchTimeout = 4 * time.Minute
)

// Outcome classifies one Refresher step for logs and metrics.
type Outcome string

const (
	OutcomeInstalled Outcome = "installed"
	OutcomePublished Outcome = "published"
	OutcomeSkipped   Outcome = "skipped"
	OutcomeFailed    Outcome = "failed"
)

// Event reports one Refresher step. Cause is a bounded class, never an error
// message, so it is safe to log.
type Event struct {
	Step    string
	Outcome Outcome
	Cause   string
	Digest  string
}

// Refresher keeps one worker's installed catalog current. Every worker polls
// the Store for a newly published Document; independently, each worker's
// refresh timer fires after a uniformly random interval in [MinInterval,
// MaxInterval], and the worker that wins the Store's lease fetches from
// OpenRouter unless another worker already published within the interval.
type Refresher struct {
	Store Store
	// Fetch returns a new Document; Fetcher.Fetch in production.
	Fetch func(context.Context) (Document, error)
	// Install compiles and publishes a Document into the worker's snapshot.
	Install      func(Document) error
	MinInterval  time.Duration
	MaxInterval  time.Duration
	PollInterval time.Duration
	LeaseTTL     time.Duration
	FetchTimeout time.Duration
	Clock        func() time.Time
	// Jitter returns a uniform duration in [0, span]; nil uses crypto/rand.
	Jitter  func(span time.Duration) time.Duration
	Observe func(Event)

	installed string
}

// Sync installs the published Document when it differs from the installed
// one. It reports whether any Document is installed afterwards.
func (refresher *Refresher) Sync(ctx context.Context) (bool, error) {
	digest, err := refresher.Store.LatestDigest(ctx)
	if err != nil {
		refresher.observe(Event{Step: "sync", Outcome: OutcomeFailed, Cause: "store"})
		return refresher.installed != "", err
	}
	if digest == "" || digest == refresher.installed {
		return refresher.installed != "", nil
	}
	latest, _, body, found, err := refresher.Store.Latest(ctx)
	if err != nil {
		refresher.observe(Event{Step: "sync", Outcome: OutcomeFailed, Cause: "store"})
		return refresher.installed != "", err
	}
	if !found || latest == refresher.installed {
		return refresher.installed != "", nil
	}
	if Digest(body) != latest {
		refresher.observe(Event{Step: "sync", Outcome: OutcomeFailed, Cause: "digest"})
		return refresher.installed != "", fmt.Errorf("published model catalog does not match its digest")
	}
	document, err := DecodeDocument(body)
	if err != nil {
		refresher.observe(Event{Step: "sync", Outcome: OutcomeFailed, Cause: "decode"})
		return refresher.installed != "", err
	}
	if err := refresher.Install(document); err != nil {
		refresher.observe(Event{Step: "sync", Outcome: OutcomeFailed, Cause: "install"})
		return refresher.installed != "", err
	}
	refresher.installed = latest
	refresher.observe(Event{Step: "sync", Outcome: OutcomeInstalled, Digest: latest})
	return true, nil
}

// Refresh fetches and publishes a new Document when this worker wins the
// lease and no other worker published one recently, then installs the
// latest published Document.
func (refresher *Refresher) Refresh(ctx context.Context) error {
	token, err := leaseToken()
	if err != nil {
		return err
	}
	acquired, err := refresher.Store.AcquireRefresh(ctx, token, refresher.leaseTTL())
	if err != nil {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "store"})
		return err
	}
	if !acquired {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeSkipped, Cause: "lease_held"})
		_, err := refresher.Sync(ctx)
		return err
	}
	defer func() {
		// Release with a fresh context so a canceled refresh still frees the
		// lease instead of holding it until it expires.
		releaseContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = refresher.Store.ReleaseRefresh(releaseContext, token)
	}()
	_, fetchedAt, _, found, err := refresher.Store.Latest(ctx)
	if err != nil {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "store"})
		return err
	}
	// Another worker's timer fired first this period: install its Document
	// instead of fetching again. The 90% margin keeps a worker whose timer
	// fires at MinInterval from skipping a Document published exactly then.
	if found && refresher.now().Sub(fetchedAt) < refresher.MinInterval*9/10 {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeSkipped, Cause: "fresh"})
		_, err := refresher.Sync(ctx)
		return err
	}
	fetchContext, cancel := context.WithTimeout(ctx, refresher.fetchTimeout())
	document, err := refresher.Fetch(fetchContext)
	cancel()
	if err != nil {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "fetch"})
		return err
	}
	encoded, err := document.Encode()
	if err != nil {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "encode"})
		return err
	}
	digest := Digest(encoded)
	published, err := refresher.Store.Publish(ctx, digest, document.FetchedAt, encoded)
	if err != nil {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomeFailed, Cause: "store"})
		return err
	}
	if published {
		refresher.observe(Event{Step: "refresh", Outcome: OutcomePublished, Digest: digest})
	}
	_, err = refresher.Sync(ctx)
	return err
}

// Run polls and refreshes until ctx ends. The first refresh is immediate when
// nothing has been published yet; afterwards each refresh is scheduled after a
// fresh random interval.
func (refresher *Refresher) Run(ctx context.Context) {
	installed, _ := refresher.Sync(ctx)
	next := refresher.now()
	if installed {
		next = next.Add(refresher.interval())
	}
	poll := time.NewTicker(refresher.pollInterval())
	defer poll.Stop()
	for {
		if !refresher.now().Before(next) {
			// A failed refresh keeps the installed Document and retries at the
			// next scheduled time; the published Document is never cleared.
			_ = refresher.Refresh(ctx)
			next = refresher.now().Add(refresher.interval())
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			_, _ = refresher.Sync(ctx)
		}
	}
}

func (refresher *Refresher) interval() time.Duration {
	minimum, maximum := refresher.MinInterval, refresher.MaxInterval
	if maximum <= minimum {
		return minimum
	}
	jitter := refresher.Jitter
	if jitter == nil {
		jitter = cryptoJitter
	}
	return minimum + jitter(maximum-minimum)
}

func cryptoJitter(span time.Duration) time.Duration {
	value, err := rand.Int(rand.Reader, big.NewInt(int64(span)+1))
	if err != nil {
		return span / 2
	}
	return time.Duration(value.Int64())
}

func leaseToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", errors.New("model catalog lease token unavailable")
	}
	return hex.EncodeToString(token[:]), nil
}

func (refresher *Refresher) now() time.Time {
	if refresher.Clock != nil {
		return refresher.Clock()
	}
	return time.Now()
}

func (refresher *Refresher) pollInterval() time.Duration {
	if refresher.PollInterval > 0 {
		return refresher.PollInterval
	}
	return defaultPollInterval
}

func (refresher *Refresher) leaseTTL() time.Duration {
	if refresher.LeaseTTL > 0 {
		return refresher.LeaseTTL
	}
	return defaultLeaseTTL
}

func (refresher *Refresher) fetchTimeout() time.Duration {
	if refresher.FetchTimeout > 0 {
		return refresher.FetchTimeout
	}
	return defaultFetchTimeout
}

func (refresher *Refresher) observe(event Event) {
	if refresher.Observe != nil {
		refresher.Observe(event)
	}
}
