package app

import (
	"context"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

// SnapshotBuilder is the only app boundary that turns external bytes into an
// immutable configuration snapshot. Client construction is deliberately
// injected: this package can validate/reload configuration without owning
// provider SDK clients or credentials.
type SnapshotBuilder struct {
	References config.ReferenceResolver
}

func (builder SnapshotBuilder) Build(ctx context.Context, data []byte) (*config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot, err := config.Compile(ctx, data, builder.References)
	if err != nil {
		return nil, fmt.Errorf("build configuration snapshot: %w", err)
	}
	return snapshot, nil
}

type SnapshotBuilderFunc func(context.Context, []byte) (*config.Snapshot, error)

func (function SnapshotBuilderFunc) Build(ctx context.Context, data []byte) (*config.Snapshot, error) {
	return function(ctx, data)
}
