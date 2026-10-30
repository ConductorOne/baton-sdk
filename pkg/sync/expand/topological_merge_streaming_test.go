package expand

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// RunTopologicalMergeStreaming is the memory-bounded pull evaluator. It keeps
// only one principal group per active input stream, instead of materializing all
// source entitlement grant groups into maps for each destination.
func (e *Expander) RunTopologicalMergeStreaming(ctx context.Context) error {
	entitlements, layers, err := e.prepareTopological(ctx)
	if err != nil {
		return err
	}
	if err := e.driveTopological(ctx, entitlements, layers, topologicalRun{
		reduce: func(ctx context.Context, dest *v2.Entitlement, incoming []topoIncomingEdge, ents map[string]*v2.Entitlement, sink *destinationSink) error {
			return e.mergeDestinationStreams(ctx, dest, incoming, ents, nil, nil, sink)
		},
	}); err != nil {
		return err
	}
	e.markExpansionComplete()
	return nil
}
