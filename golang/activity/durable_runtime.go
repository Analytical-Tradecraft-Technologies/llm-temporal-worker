package activity

import (
	"context"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// QueryV1Func is the narrow control-plane callback used by DurableV1Runtime.
// Query remains separate from the Generate/Compact phase runners so a caller
// cannot accidentally perform inference while serving a control query.
type QueryV1Func func(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error)

// DurableV1Runtime adapts the storage-neutral durable phase runners to the
// Activity boundary. It owns no clients and performs no composition itself;
// callers must supply all snapshot-scoped ports and a separately authorized
// Query callback. This keeps registration and Temporal lifecycle code reusable
// while leaving deployment-specific PostgreSQL/Redis/provider wiring explicit.
//
// A zero value is deliberately fail-closed: Generate and Compact return the
// runner's invalid-port error, while Query returns a configuration error when
// no query callback is supplied.
type DurableV1Runtime struct {
	Generate durable.GeneratePorts
	Compact  durable.CompactPorts
	Query    QueryV1Func
}

var _ V1Runtime = (*DurableV1Runtime)(nil)

// NewDurableV1Runtime validates and constructs the Activity-facing durable
// runtime. Generate and Compact are required because a configured v1 worker
// must not advertise a partially implemented one-shot boundary. Query is
// intentionally optional: deployments may bind it through Activities.QueryService
// while composing the Generate/Compact runtime.
func NewDurableV1Runtime(generate durable.GeneratePorts, compact durable.CompactPorts, query QueryV1Func) (*DurableV1Runtime, error) {
	if err := generate.Validate(); err != nil {
		return nil, fmt.Errorf("generate runtime ports: %w", err)
	}
	if err := compact.Validate(); err != nil {
		return nil, fmt.Errorf("compact runtime ports: %w", err)
	}
	return &DurableV1Runtime{Generate: generate, Compact: compact, Query: query}, nil
}

// Validate checks the complete Generate/Compact callback set carried by this
// runtime. It is safe to call during snapshot composition and never performs
// provider or storage work.
func (runtime *DurableV1Runtime) Validate() error {
	if runtime == nil {
		return fmt.Errorf("durable v1 runtime is nil")
	}
	if err := runtime.Generate.Validate(); err != nil {
		return fmt.Errorf("generate runtime ports: %w", err)
	}
	if err := runtime.Compact.Validate(); err != nil {
		return fmt.Errorf("compact runtime ports: %w", err)
	}
	return nil
}

func (runtime *DurableV1Runtime) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	if runtime == nil {
		return llm.GenerateResponseV1{}, durable.ErrV1PortsInvalid
	}
	return durable.GenerateV1(ctx, request, runtime.Generate)
}

func (runtime *DurableV1Runtime) CompactV1(ctx context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	if runtime == nil {
		return llm.CompactResponseV1{}, durable.ErrV1PortsInvalid
	}
	return durable.CompactV1(ctx, request, runtime.Compact)
}

func (runtime *DurableV1Runtime) QueryV1(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	if runtime == nil || runtime.Query == nil {
		return llm.QueryResponseV1{}, provider.NewError(provider.CodeConfiguration, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetryNever, "v1 query runtime is not configured")
	}
	response, err := runtime.Query(ctx, request)
	if err != nil {
		return llm.QueryResponseV1{}, err
	}
	if response.Kind != request.Kind || response.OperationKey != request.OperationKey {
		return llm.QueryResponseV1{}, fmt.Errorf("v1 query response identity does not match request")
	}
	return response, nil
}
