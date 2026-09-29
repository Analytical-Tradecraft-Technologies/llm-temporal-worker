package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/state"
)

type prepareResponses struct {
	ResponseRepository
	lookup func(ResponseLookup) (*ResponseEntry, error)
}

func (s prepareResponses) Lookup(_ context.Context, l ResponseLookup) (*ResponseEntry, error) {
	return s.lookup(l)
}

type prepareFills struct {
	FillRepository
	acquire func(FillLease) (FillDecision, error)
	release func(FillLease, time.Time) error
}

func (s prepareFills) Acquire(_ context.Context, l FillLease) (FillDecision, error) {
	return s.acquire(l)
}
func (s prepareFills) Release(_ context.Context, l FillLease, at time.Time) error {
	return s.release(l, at)
}

func TestPrepareDecisionsAndPublicationRace(t *testing.T) {
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	key := ResponseKey{ScopeID: "scope", Fingerprint: Fingerprint{1}}
	lease := FillLease{Key: key, OperationID: state.OperationID("operation"), Attempt: "attempt", AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
	age := time.Hour
	lookup := ResponseLookup{Key: key, Now: now, MaxAge: &age}
	errStorage := errors.New("storage unavailable")
	for _, test := range []struct {
		name                string
		firstHit, secondHit bool
		decision            FillDisposition
		fail                string
		wantEvents          []string
	}{
		{"hit", true, false, "", "", []string{"lookup"}},
		{"owner", false, false, FillOwned, "", []string{"lookup", "acquire", "lookup"}},
		{"wait", false, false, FillWait, "", []string{"lookup", "acquire", "lookup"}},
		{"recover", false, true, FillRecoveryNeeded, "", []string{"lookup", "acquire"}},
		{"finished", false, true, FillAttemptFinished, "", []string{"lookup", "acquire"}},
		{"raced publication owner", false, true, FillOwned, "", []string{"lookup", "acquire", "lookup", "release"}},
		{"raced publication waiter", false, true, FillWait, "", []string{"lookup", "acquire", "lookup"}},
		{"initial lookup failed", false, false, "", "lookup1", []string{"lookup"}},
		{"acquisition failed", false, false, "", "acquire", []string{"lookup", "acquire"}},
		{"second lookup failed", false, false, FillOwned, "lookup2", []string{"lookup", "acquire", "lookup"}},
		{"release unknown", false, true, FillOwned, "release", []string{"lookup", "acquire", "lookup", "release"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var events []string
			reads := 0
			responses := prepareResponses{lookup: func(got ResponseLookup) (*ResponseEntry, error) {
				events = append(events, "lookup")
				reads++
				if !reflect.DeepEqual(got, lookup) {
					t.Fatal("lost freshness policy")
				}
				if (reads == 1 && test.fail == "lookup1") || (reads == 2 && test.fail == "lookup2") {
					return nil, errStorage
				}
				if (reads == 1 && test.firstHit) || (reads == 2 && test.secondHit) {
					return &ResponseEntry{Key: key, ID: "entry"}, nil
				}
				return nil, nil
			}}
			fills := prepareFills{
				acquire: func(got FillLease) (FillDecision, error) {
					events = append(events, "acquire")
					if got != lease {
						t.Fatal("changed attempt")
					}
					if test.fail == "acquire" {
						return FillDecision{}, errStorage
					}
					return FillDecision{Disposition: test.decision, Record: FillRecord{Lease: lease}}, nil
				},
				release: func(got FillLease, at time.Time) error {
					events = append(events, "release")
					if got != lease || !at.Equal(now) {
						t.Fatal("wrong release")
					}
					if test.fail == "release" {
						return errStorage
					}
					return nil
				},
			}
			result, err := Prepare(context.Background(), responses, fills, lookup, lease)
			if (err != nil) != (test.fail != "") || (err != nil && !errors.Is(err, errStorage)) {
				t.Fatal(err)
			}
			if err == nil {
				wantHit := test.firstHit || (test.secondHit && (test.decision == FillOwned || test.decision == FillWait))
				if (result.Entry != nil) != wantHit {
					t.Fatalf("hit = %v", result)
				}
				if !wantHit && result.Fill.Disposition != test.decision {
					t.Fatal(result)
				}
			}
			if !reflect.DeepEqual(events, test.wantEvents) {
				t.Fatalf("events: %v", events)
			}
		})
	}
}

func TestPrepareRejectsInvalidOrCancelledBeforeIO(t *testing.T) {
	now := time.Now()
	lease := FillLease{Key: ResponseKey{ScopeID: "scope"}, AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
	lookup := ResponseLookup{Key: lease.Key, Now: now}
	responses := prepareResponses{lookup: func(ResponseLookup) (*ResponseEntry, error) { t.Fatal("unexpected IO"); return nil, nil }}
	fills := prepareFills{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Prepare(ctx, responses, fills, lookup, lease); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Prepare(nil, responses, fills, lookup, lease); err == nil {
		t.Fatal("nil context")
	}
	if _, err := Prepare(context.Background(), nil, fills, lookup, lease); err == nil {
		t.Fatal("nil repository")
	}
	if _, err := Prepare(context.Background(), responses, nil, lookup, lease); err == nil {
		t.Fatal("nil fills")
	}
	bad := lease
	bad.Key.ScopeID = "other"
	if _, err := Prepare(context.Background(), responses, fills, lookup, bad); err == nil {
		t.Fatal("wrong scope")
	}
	lookup.Now = lease.ExpiresAt
	if _, err := Prepare(context.Background(), responses, fills, lookup, lease); err == nil {
		t.Fatal("expired attempt")
	}
}
