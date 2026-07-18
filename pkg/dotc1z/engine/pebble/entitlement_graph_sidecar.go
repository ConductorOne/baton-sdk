package pebble

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Entitlement-graph sidecar: an opaque blob (owned by pkg/sync/expand)
// holding the sync's expansion graph, so it rides the c1z instead of
// bloating the sync token. Same single-fixed-key shape as the stats
// sidecar; absent on files written by a pre-sidecar SDK.

// encodeEntitlementGraphKey returns the engine-meta key for the single
// sync's graph blob. One sync per file, so no sync_id in the key.
func encodeEntitlementGraphKey() []byte {
	buf := make([]byte, 0, 6+len("entitlement-graph"))
	buf = append(buf, versionV3, typeEngineMeta)
	buf = codec.AppendTupleString(buf, "entitlement-graph")
	buf = codec.AppendTupleSeparator(buf)
	return buf
}

// EntitlementGraphSidecarLowerBound / UpperBound expose the sidecar's
// single-key range for cleanup and compaction.
func EntitlementGraphSidecarLowerBound() []byte {
	return encodeEntitlementGraphKey()
}

func EntitlementGraphSidecarUpperBound() []byte {
	return upperBoundOf(EntitlementGraphSidecarLowerBound())
}

// PutEntitlementGraphSidecar stores the opaque graph blob. Same write
// barrier as the stats sidecar: callers span EndSync's sealed window.
func (e *Engine) PutEntitlementGraphSidecar(ctx context.Context, data []byte) error {
	return e.withWriteAllowSealed(func() error {
		return e.db.MetaSet(encodeEntitlementGraphKey(), data, pebble.Sync)
	})
}

// GetEntitlementGraphSidecar returns the stored blob, or (nil, nil) if
// none exists.
func (e *Engine) GetEntitlementGraphSidecar(ctx context.Context) ([]byte, error) {
	val, closer, err := e.db.Get(encodeEntitlementGraphKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// DeleteEntitlementGraphSidecar removes the blob (no-op when absent).
func (e *Engine) DeleteEntitlementGraphSidecar(ctx context.Context) error {
	return e.withWriteAllowSealed(func() error {
		return e.db.MetaDelete(encodeEntitlementGraphKey(), pebble.Sync)
	})
}
