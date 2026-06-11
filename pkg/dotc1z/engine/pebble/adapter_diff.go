package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// generateSyncDiff is the shared GenerateSyncDiff implementation
// reached by pebbleFileOps.GenerateSyncDiff. Mirrors the SQLite
// contract: emits a NEW SyncTypePartial sync whose records are
// the rows present in `applied` that aren't present in `base`
// (set difference keyed by record identity). Modifications and
// deletions are NOT captured — that matches the SQLite behavior
// in pkg/dotc1z/diff.go (additions-only diff).
//
// Strategy: for each record-type-bearing keyspace under
// appliedSyncID, iterate the source keys, recompute the same
// record's primary key under baseSyncID, Get from base; if base
// returns ErrNotFound, write the value under diffSyncID's
// keyspace in the same record type. Index entries are
// recomputed on write so the diff sync has its own (fresh)
// indexes that match the records that landed.
//
// Returns the diff sync's ID.
func generateSyncDiff(ctx context.Context, a *Adapter, baseSyncID, appliedSyncID string) (string, error) {
	if baseSyncID == "" || appliedSyncID == "" {
		return "", errors.New("generate-diff: baseSyncID and appliedSyncID must both be non-empty")
	}
	if baseSyncID == appliedSyncID {
		return "", errors.New("generate-diff: base and applied are the same sync")
	}

	baseRun, err := a.engine.GetSyncRunRecord(ctx, baseSyncID)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", status.Errorf(codes.NotFound, "generate-diff: base sync %q not found", baseSyncID)
		}
		return "", err
	}
	if baseRun.GetEndedAt() == nil {
		return "", status.Errorf(codes.FailedPrecondition, "generate-diff: base sync %q is not ended", baseSyncID)
	}
	appliedRun, err := a.engine.GetSyncRunRecord(ctx, appliedSyncID)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", status.Errorf(codes.NotFound, "generate-diff: applied sync %q not found", appliedSyncID)
		}
		return "", err
	}
	if appliedRun.GetEndedAt() == nil {
		return "", status.Errorf(codes.FailedPrecondition, "generate-diff: applied sync %q is not ended", appliedSyncID)
	}

	diffSyncID := ksuid.New().String()
	baseBytes, err := codec.EncodeSyncID(baseSyncID)
	if err != nil {
		return "", fmt.Errorf("generate-diff: encode base: %w", err)
	}
	appliedBytes, err := codec.EncodeSyncID(appliedSyncID)
	if err != nil {
		return "", fmt.Errorf("generate-diff: encode applied: %w", err)
	}

	// Insert the diff sync's SyncRunRecord first so reads can find
	// it before/while we write the per-record-type deltas.
	now := timestamppb.Now()
	diffRun := v3.SyncRunRecord_builder{
		SyncId:       diffSyncID,
		Type:         v3.SyncType_SYNC_TYPE_PARTIAL,
		ParentSyncId: baseSyncID,
		StartedAt:    now,
	}.Build()
	if err := a.engine.PutSyncRunRecord(ctx, diffRun); err != nil {
		return "", fmt.Errorf("generate-diff: put diff sync run: %w", err)
	}
	// Bind through the ADAPTER so its tracked sync stays in lockstep
	// with the engine — the diff records below are written via the
	// engine's current-sync key context (v3 values carry no sync_id).
	// The binding is left on the diff sync when we return: the diff is
	// now the latest finished sync, so this matches where SQLite's
	// latest-finished read resolution would land anyway.
	if err := a.SetCurrentSync(ctx, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: set current diff sync: %w", err)
	}

	// Per-record-type set difference. Each helper iterates the
	// applied sync's primary keyspace, checks base for the same
	// identity, and writes the diff sync's keys (primary + any
	// secondary indexes) for records absent from base.
	if err := diffResourceTypes(ctx, a, baseBytes, appliedBytes, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: resource_types: %w", err)
	}
	if err := diffResources(ctx, a, baseBytes, appliedBytes, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: resources: %w", err)
	}
	if err := diffEntitlements(ctx, a, baseBytes, appliedBytes, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: entitlements: %w", err)
	}
	if err := diffGrants(ctx, a, baseBytes, appliedBytes, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: grants: %w", err)
	}
	if err := diffAssets(ctx, a, baseBytes, appliedBytes, diffSyncID); err != nil {
		return "", fmt.Errorf("generate-diff: assets: %w", err)
	}

	// Stamp the diff sync run as finished.
	diffRun.SetEndedAt(timestamppb.Now())
	if err := a.engine.PutSyncRunRecord(ctx, diffRun); err != nil {
		return "", fmt.Errorf("generate-diff: finish diff sync: %w", err)
	}
	return diffSyncID, nil
}

// existsAt returns true when `key` is present in the engine's DB.
// Wraps the Get+closer dance in a single boolean check.
func existsAt(db *pebble.DB, key []byte) (bool, error) {
	val, closer, err := db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = val
	_ = closer.Close()
	return true, nil
}

// diffResourceTypes copies resource_types that exist under
// appliedBytes but not under baseBytes. The destination sync is
// supplied by the engine's current-sync key context.
func diffResourceTypes(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
	srcPrefix := encodeResourceTypePrefix(appliedBytes)
	return iterDiff(ctx, a.engine.DB(), srcPrefix, upperBoundOf(srcPrefix), func(_ []byte, val []byte) error {
		var rec v3.ResourceTypeRecord
		if err := unmarshalRecord(val, &rec); err != nil {
			return fmt.Errorf("unmarshal resource_type: %w", err)
		}
		baseKey := encodeResourceTypeKey(baseBytes, rec.GetExternalId())
		if exists, err := existsAt(a.engine.DB(), baseKey); err != nil {
			return err
		} else if exists {
			return nil
		}
		return a.engine.PutResourceTypeRecord(ctx, &rec)
	})
}

func diffResources(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
	srcPrefix := encodeResourcePrefix(appliedBytes)
	return iterDiff(ctx, a.engine.DB(), srcPrefix, upperBoundOf(srcPrefix), func(_ []byte, val []byte) error {
		var rec v3.ResourceRecord
		if err := unmarshalRecord(val, &rec); err != nil {
			return fmt.Errorf("unmarshal resource: %w", err)
		}
		baseKey := encodeResourceKey(baseBytes, rec.GetResourceTypeId(), rec.GetResourceId())
		if exists, err := existsAt(a.engine.DB(), baseKey); err != nil {
			return err
		} else if exists {
			return nil
		}
		return a.engine.PutResourceRecord(ctx, &rec)
	})
}

func diffEntitlements(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
	srcPrefix := encodeEntitlementPrefix(appliedBytes)
	return iterDiff(ctx, a.engine.DB(), srcPrefix, upperBoundOf(srcPrefix), func(_ []byte, val []byte) error {
		var rec v3.EntitlementRecord
		if err := unmarshalRecord(val, &rec); err != nil {
			return fmt.Errorf("unmarshal entitlement: %w", err)
		}
		baseKey := encodeEntitlementKey(baseBytes, rec.GetExternalId())
		if exists, err := existsAt(a.engine.DB(), baseKey); err != nil {
			return err
		} else if exists {
			return nil
		}
		return a.engine.PutEntitlementRecord(ctx, &rec)
	})
}

func diffGrants(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
	srcPrefix := encodeGrantPrefix(appliedBytes)
	return iterDiff(ctx, a.engine.DB(), srcPrefix, upperBoundOf(srcPrefix), func(_ []byte, val []byte) error {
		var rec v3.GrantRecord
		if err := unmarshalRecord(val, &rec); err != nil {
			return fmt.Errorf("unmarshal grant: %w", err)
		}
		baseKey := encodeGrantKey(baseBytes, rec.GetExternalId())
		if exists, err := existsAt(a.engine.DB(), baseKey); err != nil {
			return err
		} else if exists {
			return nil
		}
		return a.engine.PutGrantRecord(ctx, &rec)
	})
}

func diffAssets(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
	srcPrefix := encodeAssetPrefix(appliedBytes)
	return iterDiff(ctx, a.engine.DB(), srcPrefix, upperBoundOf(srcPrefix), func(_ []byte, val []byte) error {
		var rec v3.AssetRecord
		if err := unmarshalRecord(val, &rec); err != nil {
			return fmt.Errorf("unmarshal asset: %w", err)
		}
		baseKey := encodeAssetKey(baseBytes, rec.GetExternalId())
		if exists, err := existsAt(a.engine.DB(), baseKey); err != nil {
			return err
		} else if exists {
			return nil
		}
		rec.SetSyncId(diffSyncID)
		return a.engine.PutAssetRecord(ctx, &rec)
	})
}

// iterDiff iterates [lower, upper) on db, invoking fn(key, value)
// for each row. Cheap wrapper to keep the per-record-type helpers
// readable.
func iterDiff(ctx context.Context, db *pebble.DB, lower, upper []byte, fn func(key, value []byte) error) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
