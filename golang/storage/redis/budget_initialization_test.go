package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	redisclient "github.com/redis/go-redis/v9"
)

func TestBudgetInitializerDisablesRetriesForOneTimeCreation(t *testing.T) {
	for _, source := range []redisclient.UniversalClient{
		redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0", MaxRetries: 8}),
		redisclient.NewClusterClient(&redisclient.ClusterOptions{Addrs: []string{"127.0.0.1:0"}, MaxRetries: 8, MaxRedirects: 8}),
	} {
		t.Cleanup(func() { _ = source.Close() })
		client, err := singleAttemptBudgetClient(source)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		switch concrete := client.(type) {
		case *redisclient.Client:
			if concrete.Options().MaxRetries != 0 {
				t.Fatal("single-use creation can be retransmitted")
			}
		case *redisclient.ClusterClient:
			if concrete.Options().MaxRetries != -1 || concrete.Options().MaxRedirects != 0 {
				t.Fatalf("cluster single-use retries were not disabled: retries=%d redirects=%d", concrete.Options().MaxRetries, concrete.Options().MaxRedirects)
			}
		}
	}
}

func TestBudgetMaterializerRequiresReadyReceiptForItsOwnKeys(t *testing.T) {
	keys := KeyOptions{Prefix: "test", HashTag: "budget", KeySecret: []byte(strings.Repeat("s", 32))}
	space, err := NewBudgetKeySpace(keys)
	if err != nil {
		t.Fatal(err)
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	for _, kind := range []string{"pending", "namespace", "fingerprint", "invalid-epoch", "valid"} {
		value := budget.Initialization{Schema: budget.InitializationSchema, Identity: space.InitializationIdentity(), Epoch: "00000000-0000-4000-8000-000000000001", CreatedAt: time.Now().UTC(), Ready: true}
		switch kind {
		case "pending":
			value.Ready = false
		case "namespace":
			value.Identity.Namespace = "other:{budget}:"
		case "fingerprint":
			value.Identity.KeyFingerprint = strings.Repeat("0", 64)
		case "invalid-epoch":
			value.Epoch = "not-a-uuid"
		}
		_, err := NewRedisBudgetMaterializer(RedisBudgetMaterializerOptions{Client: client, Keys: keys, Mode: AdmissionModeFunction, GenerationID: "redis-budget-v1", IncarnationID: "redis-budget-v1", Initialization: &value})
		if kind == "valid" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrBudgetAuthorityUnavailable) {
			t.Fatalf("%s receipt error = %v", kind, err)
		}
	}
}

type initializationScanner struct {
	calls int
	pages [][]string
}

func (s *initializationScanner) Scan(ctx context.Context, cursor uint64, match string, count int64) *redisclient.ScanCmd {
	s.calls++
	cmd := redisclient.NewScanCmd(ctx, nil)
	if match != "*" || count != 256 {
		cmd.SetErr(errors.New("scan changed its bounded page size or glob"))
		return cmd
	}
	page := s.pages[cursor]
	next := cursor + 1
	if next == uint64(len(s.pages)) {
		next = 0
	}
	cmd.SetVal(page, next)
	return cmd
}

func TestBudgetInitializationScansEveryPageAndMatchesLiteralNamespace(t *testing.T) {
	space, err := NewBudgetKeySpace(KeyOptions{Prefix: "test", HashTag: "budget*", KeySecret: []byte(strings.Repeat("s", 32))})
	if err != nil {
		t.Fatal(err)
	}
	scanner := &initializationScanner{pages: [][]string{{space.AuthorityKey(), "other:{budget*}:unrelated", "test:{budgetABC}:other"}, {}, {"test:{budget*}:occupied"}}}
	if err := scanBudgetNamespace(context.Background(), scanner, space); !errors.Is(err, ErrBudgetAuthorityUnavailable) || scanner.calls != 3 {
		t.Fatalf("occupied final page was missed: calls=%d err=%v", scanner.calls, err)
	}
	scanner.pages = scanner.pages[:2]
	if err := scanBudgetNamespace(context.Background(), scanner, space); err != nil {
		t.Fatalf("literal namespace matched unrelated keys: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scanner.calls = 0
	if err := scanBudgetNamespace(ctx, scanner, space); !errors.Is(err, context.Canceled) || scanner.calls != 0 {
		t.Fatalf("canceled scan reached Redis: %v", err)
	}
}
