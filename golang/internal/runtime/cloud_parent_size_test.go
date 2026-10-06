package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// A turn that would take the transcript past MaxExtendableParentBytes is
// refused before anything is paid, and planning asks for compaction instead.
// The parent itself stays preparable, so compaction rescues the lineage (#980).
func TestCloudGenerateRefusesToExtendAParentPastTheExtendableBound(t *testing.T) {
	t.Parallel()
	f := compactionTriggerFixture(t, `{"recent_turns":1}`)
	chunk := strings.Repeat("history ", 100<<10) // 800 KiB per turn
	for i := 0; i < 3; i++ {
		f.turn(t, fmt.Sprintf("turn-%d", i), chunk)
	}
	// The next turn's transcript exceeds the extendable bound.
	if !f.plan(t, "too-large", chunk) {
		t.Fatal("planning did not request compaction for a parent past the extendable bound")
	}
	before := f.submits.Load()
	_, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Generate: &f.request})
	var failure *provider.Error
	if !errors.As(err, &failure) || failure.Code != provider.CodeInvalidArgument || failure.Retry != provider.RetryNever || f.submits.Load() != before {
		t.Fatalf("prepare = %v, submits %d -> %d; want an unpaid invalid_argument", err, before, f.submits.Load())
	}
	// Compaction of the same parent proceeds, and the compacted lineage can
	// be extended again.
	f.compact(t, "rescue")
	f.turn(t, "after-compaction", "small follow-up")
	if cloudstate.MaxExtendableParentBytes >= cloudstate.MaxPreparedParentBytes {
		t.Fatal("no output headroom between the extendable and prepared bounds")
	}
}
