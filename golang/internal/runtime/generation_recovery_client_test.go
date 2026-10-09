package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

type generationRecoveryClientFixture struct {
	t        *testing.T
	iterator client.HistoryEventIterator
	called   bool
}

func (f *generationRecoveryClientFixture) GetWorkflowHistory(ctx context.Context, workflowID, runID string, wait bool, filter enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	f.t.Helper()
	if ctx.Err() != nil || workflowID != "private-workflow" || runID != "private-run" || wait || filter != enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT {
		f.t.Fatal("history identity, filter or context changed")
	}
	f.called = true
	return f.iterator
}

// Unimplemented mutation methods panic if this dry-run boundary ever calls one.
type generationRecoveryDryStore struct {
	generationRecoveryStore
	verified bool
}

func (s *generationRecoveryDryStore) VerifyOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error) {
	s.verified = true
	return cloudstate.Record{}, nil
}

func (*generationRecoveryDryStore) LoadGenerationPlan(context.Context, cloudstate.Operation) (cloudstate.GenerationPlan, error) {
	return cloudstate.GenerationPlan{}, contracts.ErrNotFound
}

func TestGenerationRecoveryClientUsesExplicitRunAndConfiguredCodec(t *testing.T) {
	for _, wrongCodec := range []bool{false, true} {
		events, dc, _ := generationRecoveryHistoryFixture(t)
		if wrongCodec {
			dc = converter.GetDefaultDataConverter()
		}
		c := &generationRecoveryClientFixture{t: t, iterator: &recoveryHistoryIterator{events: events}}
		store := &generationRecoveryDryStore{}
		var output bytes.Buffer
		err := recoverGenerationFromClient(context.Background(), c, dc, "test", store, boundedCloud(t, false).options.ResolveScope, "private-workflow", "private-run", false, time.Now(), &output)
		if !c.called {
			t.Fatal("configured client not used")
		}
		if wrongCodec {
			if !errors.Is(err, errGenerationRecoveryHistory) || store.verified || output.Len() != 0 {
				t.Fatalf("wrong codec reached storage or exposed output: %v", err)
			}
		} else if err != nil || !store.verified || output.String() != "{\"status\":\"recovery_required\",\"applied\":false}\n" || strings.Contains(output.String(), "private-") {
			t.Fatalf("dry-run boundary: %q %v", output.String(), err)
		}
	}
}
