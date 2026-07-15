package pebble

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// BuildManifest assembles the v3 envelope manifest for a Pebble-engine c1z
// using the given payload encoding. Used by CloneSync and by pkg/dotc1z's
// Pebble store when saving the envelope at Close.
func BuildManifest(encoding c1zstore.PayloadEncoding) (*c1zv3.C1ZManifestV3, error) {
	descriptors, err := formatv3.BuildDescriptorClosure()
	if err != nil {
		return nil, err
	}
	return c1zv3.C1ZManifestV3_builder{
		// PebbleManifestEngine ("pebble3"), not EnginePebble: readers
		// dispatch on this name, and SDKs that predate the structural-
		// identity keyspace must reject the file loudly instead of
		// scanning retired index families as empty. See its doc comment.
		Engine:              c1zstore.PebbleManifestEngine,
		EngineSchemaVersion: uint32(SDKPebbleFormat),
		PayloadEncoding:     payloadEncodingToProto(encoding),
		Descriptors:         descriptors,
	}.Build(), nil
}

// BuildManifestWithSyncRuns assembles the manifest including the
// sync-run projection from e. Used by pkg/dotc1z's Pebble store when
// saving the envelope at Close, so readers can enumerate syncs and
// read row counts from the envelope header without unpacking the
// Pebble payload (consumed by compaction source selection via
// formatv3.ReadManifestHeader).
func BuildManifestWithSyncRuns(ctx context.Context, e *Engine, encoding c1zstore.PayloadEncoding) (*c1zv3.C1ZManifestV3, error) {
	m, err := BuildManifest(encoding)
	if err != nil {
		return nil, err
	}
	syncRuns, err := syncRunSummaries(ctx, e)
	if err != nil {
		return nil, err
	}
	m.SetSyncRuns(syncRuns)
	m.SetPebbleIdIndexFormat(e.manifestIDIndexFormat())
	if root, ok, err := e.GetGrantDigestGlobalRoot(ctx); err != nil {
		return nil, fmt.Errorf("pebble: manifest grant digest root: %w", err)
	} else if ok {
		m.SetGrantDigestRoot(c1zv3.GrantDigestRoot_builder{
			XorDigest:  root.Hash,
			Count:      root.Count,
			AbiVersion: GrantDigestABIVersion,
		}.Build())
	}
	return m, nil
}

// syncRunSummaries projects every sync_run record (plus its stats
// sidecar, when present) into manifest summaries.
//
// A stats sidecar read failure downgrades to a summary without stats
// (the consumer falls back to opening the payload) rather than failing
// the save — stats are advisory; the payload remains authoritative.
func syncRunSummaries(ctx context.Context, e *Engine) ([]*c1zv3.SyncRunSummary, error) {
	var out []*c1zv3.SyncRunSummary
	l := ctxzap.Extract(ctx)
	err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		stats, serr := e.readSyncStats(ctx, r.GetSyncId())
		if serr != nil {
			l.Warn("pebble: manifest sync-run projection: stats sidecar read failed; writing summary without stats",
				zap.String("sync_id", r.GetSyncId()),
				zap.Error(serr),
			)
			stats = nil
		}
		out = append(out, c1zv3.SyncRunSummary_builder{
			SyncId:       r.GetSyncId(),
			Type:         r.GetType(),
			StartedAt:    r.GetStartedAt(),
			EndedAt:      r.GetEndedAt(),
			ParentSyncId: r.GetParentSyncId(),
			Stats:        stats,
		}.Build())
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("pebble: manifest sync-run projection: %w", err)
	}
	return out, nil
}

// payloadEncodingToProto maps the public c1zstore.PayloadEncoding to
// the c1zv3 enum. PayloadEncodingUnspecified means "engine default"
// for our purposes: INDEXED_ZSTD.
func payloadEncodingToProto(enc c1zstore.PayloadEncoding) c1zv3.PayloadEncoding {
	switch enc {
	case c1zstore.PayloadEncodingTar:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR
	case c1zstore.PayloadEncodingTarZstd:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	case c1zstore.PayloadEncodingIndexedZstd, c1zstore.PayloadEncodingUnspecified:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD
	default:
		// Any non-enumerated value falls back to the default.
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD
	}
}
