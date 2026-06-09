package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	mergepkg "github.com/conductorone/baton-sdk/pkg/synccompactor/pebble"
)

// WithEngine selects the storage engine for the compacted output.
// The default (unset) is sqlite, which is byte-identical to the
// historical compactor. EnginePebble produces a v3 Pebble c1z via a
// native record merge.
//
// This is the only supported way to choose the engine; an engine
// passed through WithC1ZOptions does not select the compaction
// strategy and is overridden.
func WithEngine(engine dotc1z.Engine) Option {
	return func(c *Compactor) {
		c.engine = engine
	}
}

// compactableV3SyncType reports whether a v3 sync type is a compactable
// snapshot type. Diff syncs (partial_upserts / partial_deletions) are
// excluded — compaction folds full / resources-only / partial snapshots
// only, matching the sqlite source selection.
func compactableV3SyncType(t v3.SyncType) bool {
	switch t {
	case v3.SyncType_SYNC_TYPE_FULL,
		v3.SyncType_SYNC_TYPE_RESOURCES_ONLY,
		v3.SyncType_SYNC_TYPE_PARTIAL:
		return true
	default:
		return false
	}
}

// unionV3SyncType folds two sync types to the wider one: full beats
// resources-only beats partial. Mirrors the sqlite union rule.
func unionV3SyncType(a, b v3.SyncType) v3.SyncType {
	switch {
	case a == v3.SyncType_SYNC_TYPE_FULL || b == v3.SyncType_SYNC_TYPE_FULL:
		return v3.SyncType_SYNC_TYPE_FULL
	case a == v3.SyncType_SYNC_TYPE_RESOURCES_ONLY || b == v3.SyncType_SYNC_TYPE_RESOURCES_ONLY:
		return v3.SyncType_SYNC_TYPE_RESOURCES_ONLY
	default:
		return v3.SyncType_SYNC_TYPE_PARTIAL
	}
}

// compactPebble folds every input into the empty newSyncId on the
// Pebble output via a native record merge: each input is opened, its
// latest finished compactable sync is selected (diff syncs excluded),
// and all are merged keeping the newest record per key. The output
// sync_run's type and ended_at are then set to the union / max across
// the inputs (mirroring the sqlite UpdateSync), and its stats are
// recomputed. Inputs are merged in reverse entry order so the tie
// winner matches the sqlite fold.
func (c *Compactor) compactPebble(ctx context.Context, newSyncId string) error {
	l := ctxzap.Extract(ctx)

	destEng, ok := enginepkg.AsEngine(c.compactedC1z)
	if !ok {
		return errors.New("compactPebble: compacted store is not a pebble engine")
	}

	sources := make([]mergepkg.SourceSync, 0, len(c.entries))
	unionType := v3.SyncType_SYNC_TYPE_PARTIAL
	var maxEnded time.Time

	for i := len(c.entries) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		cs := c.entries[i]
		w, err := dotc1z.NewStore(ctx, cs.FilePath, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(c.tmpDir))
		if err != nil {
			return fmt.Errorf("compactPebble: open input %s: %w", cs.FilePath, err)
		}
		// Close the input store when the merge completes; the SST data
		// is read eagerly into the destination during MergeInto.
		defer func() {
			if cerr := w.Close(ctx); cerr != nil {
				l.Error("compactPebble: error closing input", zap.Error(cerr), zap.String("file", cs.FilePath))
			}
		}()

		srcEng, ok := enginepkg.AsEngine(w)
		if !ok {
			return fmt.Errorf("compactPebble: input %s is not a pebble c1z", cs.FilePath)
		}
		rec, err := srcEng.LatestFinishedSyncRecord(ctx, compactableV3SyncType)
		if err != nil {
			return fmt.Errorf("compactPebble: select source sync for %s: %w", cs.FilePath, err)
		}
		if rec == nil {
			return fmt.Errorf("compactPebble: input %s has no finished compactable sync (diff syncs are not compactable)", cs.FilePath)
		}

		sources = append(sources, mergepkg.SourceSync{Engine: srcEng, SyncID: rec.GetSyncId()})
		unionType = unionV3SyncType(unionType, rec.GetType())
		if ts := rec.GetEndedAt(); ts != nil && ts.AsTime().After(maxEnded) {
			maxEnded = ts.AsTime()
		}
	}

	if err := mergepkg.MergeInto(ctx, destEng, sources, newSyncId); err != nil {
		return fmt.Errorf("compactPebble: merge: %w", err)
	}

	// Set the compacted sync_run's type + ended_at to the union / max
	// across the inputs so downstream gating (e.g. grant expansion)
	// behaves identically to the sqlite path, then recompute stats.
	rec, err := destEng.GetSyncRunRecord(ctx, newSyncId)
	if err != nil {
		return fmt.Errorf("compactPebble: load dest sync_run: %w", err)
	}
	rec.SetType(unionType)
	if !maxEnded.IsZero() {
		rec.SetEndedAt(timestamppb.New(maxEnded))
	}
	if err := destEng.PutSyncRunRecord(ctx, rec); err != nil {
		return fmt.Errorf("compactPebble: persist dest sync_run: %w", err)
	}
	if err := destEng.PersistSyncStats(ctx, newSyncId); err != nil {
		return fmt.Errorf("compactPebble: persist stats: %w", err)
	}
	return nil
}
