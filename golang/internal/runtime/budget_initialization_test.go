package runtime

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	redisstore "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/redis"
)

type testInitializedRequests struct {
	*recordingCloudRequests
	budget.InitializationStore
}

type initializationTestStore struct {
	value                   budget.Initialization
	trace                   *[]string
	prepareErr, completeErr error
}

func (s *initializationTestStore) record(step string) {
	if s.trace != nil {
		*s.trace = append(*s.trace, step)
	}
}
func (s *initializationTestStore) ReadBudgetInitialization(context.Context, string) (budget.Initialization, error) {
	s.record("read")
	if s.value.Schema == "" {
		return budget.Initialization{}, contracts.ErrNotFound
	}
	return s.value, nil
}
func (s *initializationTestStore) PrepareBudgetInitialization(_ context.Context, identity budget.InitializationIdentity, now time.Time) (budget.InitializationPreparation, error) {
	s.record("prepare")
	created := s.value.Schema == ""
	if created {
		s.value = testInitialization(identity, now)
	}
	if s.prepareErr != nil {
		return budget.InitializationPreparation{}, s.prepareErr
	}
	return budget.InitializationPreparation{Receipt: s.value, Created: created}, nil
}
func (s *initializationTestStore) CompleteBudgetInitialization(_ context.Context, value budget.Initialization) error {
	s.record("complete")
	if value.Marker() != s.value.Marker() {
		return contracts.ErrConflict
	}
	s.value.Ready = true
	return s.completeErr
}

func testInitialization(identity budget.InitializationIdentity, now time.Time) budget.Initialization {
	return budget.Initialization{Schema: budget.InitializationSchema, Identity: identity, Epoch: "00000000-0000-4000-8000-000000000001", CreatedAt: now.UTC()}
}

func testBudgetIdentity(t *testing.T, value config.Config) budget.InitializationIdentity {
	t.Helper()
	keys, err := redisKeyOptions(value, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	space, err := redisstore.NewBudgetKeySpace(keys)
	if err != nil {
		t.Fatal(err)
	}
	return space.InitializationIdentity()
}

type initializationTestRedis struct {
	identity                budget.InitializationIdentity
	trace                   *[]string
	marker                  string
	checkErr, initializeErr error
	createCount             int
}

func (r *initializationTestRedis) Identity() budget.InitializationIdentity { return r.identity }
func (r *initializationTestRedis) Check(_ context.Context, value budget.Initialization) error {
	if r.trace != nil {
		*r.trace = append(*r.trace, "check")
	}
	if r.checkErr != nil {
		return r.checkErr
	}
	if r.marker != value.Marker() {
		return redisstore.ErrBudgetAuthorityUnavailable
	}
	return nil
}
func (r *initializationTestRedis) Initialize(_ context.Context, preparation budget.InitializationPreparation) error {
	if r.trace != nil {
		*r.trace = append(*r.trace, "initialize")
	}
	if r.marker == "" {
		if !preparation.Created {
			return redisstore.ErrBudgetAuthorityUnavailable
		}
		r.createCount++
		r.marker = preparation.Receipt.Marker()
	}
	return r.initializeErr
}

func TestBudgetInitializationOrderAndReadOnlyReplay(t *testing.T) {
	identity := testBudgetIdentity(t, trustedTemporalTestConfig(t))
	var trace []string
	store := &initializationTestStore{trace: &trace}
	redis := &initializationTestRedis{identity: identity, trace: &trace}
	ctx := context.Background()
	report, err := initializeBudgetAuthority(ctx, store, redis, false, time.Now())
	if err != nil || report.Status != "initialization_required" || !reflect.DeepEqual(trace, []string{"read"}) {
		t.Fatalf("read-only plan = %#v, %v, %v", report, trace, err)
	}
	trace = nil
	report, err = initializeBudgetAuthority(ctx, store, redis, true, time.Now())
	if err != nil || !report.Applied || report.Status != "ready" || !reflect.DeepEqual(trace, []string{"read", "prepare", "initialize", "complete", "read", "check"}) {
		t.Fatalf("initialization = %#v, %v, %v", report, trace, err)
	}
	for _, apply := range []bool{false, true} {
		trace = nil
		report, err = initializeBudgetAuthority(ctx, store, redis, apply, time.Now())
		if err != nil || report.Applied || report.Status != "ready" || !reflect.DeepEqual(trace, []string{"read", "check"}) {
			t.Fatalf("ready replay = %#v, %v, %v", report, trace, err)
		}
	}
	if redis.createCount != 1 {
		t.Fatal("replay recreated budget authority")
	}
}

func TestBudgetInitializationUnknownOutcomesNeverRegrant(t *testing.T) {
	for _, stage := range []string{"prepare", "initialize", "complete"} {
		t.Run(stage, func(t *testing.T) {
			identity := testBudgetIdentity(t, trustedTemporalTestConfig(t))
			store := &initializationTestStore{}
			redis := &initializationTestRedis{identity: identity}
			switch stage {
			case "prepare":
				store.prepareErr = contracts.ErrOutcomeUnknown
			case "initialize":
				redis.initializeErr = contracts.ErrOutcomeUnknown
			case "complete":
				store.completeErr = contracts.ErrOutcomeUnknown
			}
			if _, err := initializeBudgetAuthority(context.Background(), store, redis, true, time.Now()); !errors.Is(err, contracts.ErrOutcomeUnknown) {
				t.Fatalf("lost %s reply = %v", stage, err)
			}
			store.prepareErr, store.completeErr, redis.initializeErr = nil, nil, nil
			_, err := initializeBudgetAuthority(context.Background(), store, redis, true, time.Now())
			if stage == "prepare" {
				if !errors.Is(err, redisstore.ErrBudgetAuthorityUnavailable) || redis.createCount != 0 {
					t.Fatalf("lost create permission was reissued: %v", err)
				}
				return
			}
			if err != nil || redis.createCount != 1 {
				t.Fatalf("lost reply did not resume safely: %v", err)
			}
			redis.marker = ""
			if _, err := initializeBudgetAuthority(context.Background(), store, redis, true, time.Now()); !errors.Is(err, redisstore.ErrBudgetAuthorityUnavailable) || redis.createCount != 1 {
				t.Fatalf("lost authority was reset: %v", err)
			}
		})
	}
}

func TestBudgetAuthorityReadinessIsReadOnlyAndFailsClosed(t *testing.T) {
	identity := testBudgetIdentity(t, trustedTemporalTestConfig(t))
	value := testInitialization(identity, time.Now())
	value.Ready = true
	ready := ProbeResult{Dependency: DependencyRedis, Status: ProbeStatusReady, Reason: ProbeReasonReady}
	for _, test := range []struct {
		err    error
		status ProbeStatus
	}{
		{nil, ProbeStatusReady}, {redisstore.ErrBudgetAuthorityUnavailable, ProbeStatusPolicy},
		{context.DeadlineExceeded, ProbeStatusTimeout}, {errors.New("sensitive endpoint failure"), ProbeStatusUnavailable},
	} {
		var trace []string
		redis := &initializationTestRedis{identity: identity, marker: value.Marker(), checkErr: test.err, trace: &trace}
		probe := withBudgetAuthorityProbe(DependencyProbeFunc(func(context.Context) ProbeResult { return ready }), redis, value)
		if result := probe.Probe(context.Background()); result.Status != test.status {
			t.Fatalf("readiness = %#v", result)
		}
		if !reflect.DeepEqual(trace, []string{"check"}) {
			t.Fatalf("readiness wrote state: %v", trace)
		}
	}
}
