package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/secrets"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

type generationRecoveryStore interface {
	VerifyOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error)
	LoadGenerationPlan(context.Context, cloudstate.Operation) (cloudstate.GenerationPlan, error)
	SaveGenerationPlan(context.Context, cloudstate.Operation, cloudstate.GenerationPlan) (cloudstate.GenerationPlan, error)
	LoadGenerationBinding(context.Context, cloudstate.Operation) (json.RawMessage, error)
	SaveGenerationBinding(context.Context, cloudstate.Operation, json.RawMessage) error
}

var _ generationRecoveryStore = (*cloudstate.Repository)(nil)

type generationRecoveryReport struct {
	Status  string `json:"status"`
	Applied bool   `json:"applied"`
}

func restoreGenerationRecovery(ctx context.Context, store generationRecoveryStore, authorize CheckpointScopeResolver, proof generationRecoveryProof, apply bool, now time.Time) (generationRecoveryReport, error) {
	if ctx == nil || store == nil || authorize == nil {
		return generationRecoveryReport{}, errGenerationRecoveryHistory
	}
	if _, err := authorize(ctx, proof.original.Context); err != nil {
		return generationRecoveryReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return generationRecoveryReport{}, err
	}
	if proof.original.Parent == nil || proof.effective.Parent == nil || !samePublicGeneration(proof.original, proof.effective) {
		return generationRecoveryReport{}, errGenerationRecoveryHistory
	}
	if _, err := proof.original.MarshalJSON(); err != nil {
		return generationRecoveryReport{}, errGenerationRecoveryHistory
	}
	manifest, err := proof.effective.MarshalJSON()
	if err != nil {
		return generationRecoveryReport{}, errGenerationRecoveryHistory
	}
	// This is a read-only verification, including for Pending operations. It
	// cannot create a request, advance an event, or repair a discovery index.
	if _, err := store.VerifyOperation(ctx, publicGenerationOperation(proof.effective, now)); err != nil {
		return generationRecoveryReport{}, err
	}
	op := publicGenerationOperation(proof.original, now)
	plan, err := store.LoadGenerationPlan(ctx, op)
	if err == nil && !plan.CompactBeforeGenerate {
		return generationRecoveryReport{}, contracts.ErrConflict
	}
	if err != nil && (!errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown)) {
		return generationRecoveryReport{}, err
	}
	if err == nil {
		bound, err := store.LoadGenerationBinding(ctx, op)
		if err == nil {
			if !bytes.Equal(bound, manifest) {
				return generationRecoveryReport{}, contracts.ErrConflict
			}
			return generationRecoveryReport{Status: "ready"}, nil
		}
		if !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, contracts.ErrOutcomeUnknown) {
			return generationRecoveryReport{}, err
		}
	}
	if !apply {
		return generationRecoveryReport{Status: "recovery_required"}, nil
	}
	saved, err := store.SaveGenerationPlan(ctx, op, cloudstate.GenerationPlan{CompactBeforeGenerate: true})
	if err != nil {
		return generationRecoveryReport{}, err
	}
	if !saved.CompactBeforeGenerate {
		return generationRecoveryReport{}, contracts.ErrConflict
	}
	if err := store.SaveGenerationBinding(ctx, op, manifest); err != nil {
		return generationRecoveryReport{}, err
	}
	bound, err := store.LoadGenerationBinding(ctx, op)
	if err != nil {
		return generationRecoveryReport{}, err
	}
	if !bytes.Equal(bound, manifest) {
		return generationRecoveryReport{}, contracts.ErrConflict
	}
	return generationRecoveryReport{Status: "ready", Applied: true}, nil
}

func validGenerationRecoveryIDs(workflowID, runID string) bool {
	return workflowID != "" && runID != "" && len(workflowID) <= 1024 && len(runID) <= 128 && utf8.ValidString(workflowID) && utf8.ValidString(runID) && strings.TrimSpace(workflowID) == workflowID && strings.TrimSpace(runID) == runID
}

// RunGenerationRecover restores only proven original public metadata. It
// constructs cloud storage and the configured Temporal client, never a worker,
// provider catalog, routing client, or budget authority. Without --apply every
// storage operation is a read. The explicit run avoids latest-run substitution.
func RunGenerationRecover(ctx context.Context, data []byte, workflowID, runID string, apply bool, output io.Writer) error {
	if ctx == nil || output == nil || !validGenerationRecoveryIDs(workflowID, runID) {
		return errors.New("generation recovery requires explicit workflow and run IDs")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	snapshot, err := config.Compile(ctx, data, nil)
	if err != nil {
		return errors.New("generation recovery configuration is invalid")
	}
	value := snapshot.Config()
	if value.State.Kind != config.StateKindDurable || value.State.Requests == nil {
		return errors.New("generation recovery requires durable cloud storage")
	}
	options, err := trustedTemporalCloudOptions(value)
	if err != nil {
		return errors.New("generation recovery authorization configuration is invalid")
	}
	resolver := secrets.New(secrets.Options{})
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{Resolver: resolver}}
	repository, err := factory.buildCloudRequests(ctx, value.State.Requests)
	if err != nil {
		return errors.New("generation recovery storage could not be opened")
	}
	store, ok := repository.(generationRecoveryStore)
	if !ok || isNilCapability(store) {
		return errors.New("generation recovery storage is unsupported")
	}
	var dc converter.DataConverter
	temporalFactory := DefaultTemporalClientFactory{SecretResolver: resolver, DialContext: func(ctx context.Context, options client.Options) (client.Client, error) {
		dc = options.DataConverter
		return client.DialContext(ctx, options)
	}}
	temporalClient, err := temporalFactory.New(ctx, value)
	if err != nil {
		return errors.New("generation recovery Temporal connection failed")
	}
	defer temporalClient.Close()
	return recoverGenerationFromClient(ctx, temporalClient, dc, value.Temporal.Namespace, store, options.ResolveScope, workflowID, runID, apply, time.Now().UTC(), output)
}

type generationRecoveryHistoryClient interface {
	GetWorkflowHistory(context.Context, string, string, bool, enumspb.HistoryEventFilterType) client.HistoryEventIterator
}

func recoverGenerationFromClient(ctx context.Context, temporalClient generationRecoveryHistoryClient, dc converter.DataConverter, namespace string, store generationRecoveryStore, authorize CheckpointScopeResolver, workflowID, runID string, apply bool, now time.Time, output io.Writer) error {
	if ctx == nil || temporalClient == nil || dc == nil || output == nil || !validGenerationRecoveryIDs(workflowID, runID) {
		return errGenerationRecoveryHistory
	}
	proof, err := readGenerationRecoveryHistory(ctx, temporalClient.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT), dc, namespace)
	if err != nil {
		return err
	}
	report, err := restoreGenerationRecovery(ctx, store, authorize, proof, apply, now)
	if err != nil {
		return errors.New("generation recovery could not verify storage and authorization; retry only with the same workflow and run")
	}
	if err := json.NewEncoder(output).Encode(report); err != nil {
		return errors.New("write generation recovery result failed")
	}
	return nil
}
