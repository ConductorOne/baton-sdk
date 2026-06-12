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
// Strategy: for the small record types (resource_types, resources,
// entitlements, assets), iterate the source keys under appliedSyncID,
// recompute the same record's primary key under baseSyncID, Get from
// base; if base returns ErrNotFound, write the value under
// diffSyncID's keyspace in the same record type. GRANTS — the type
// that dominates every real file — are diffed via the per-entitlement
// grant digests instead (see diffGrants): unchanged entitlements are
// skipped with a single root comparison, and only the principal-hash
// buckets that actually differ are scanned. Index entries are
// recomputed on write so the diff sync has its own (fresh) indexes
// that match the records that landed.
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
	if err := diffGrants(ctx, a, baseBytes, appliedBytes, baseSyncID, appliedSyncID, diffSyncID); err != nil {
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

// diffGrants computes the grants set difference using the
// per-entitlement grant digests instead of scanning every applied
// grant.
//
// Walk: enumerate the distinct entitlement_ids in the applied sync's
// hash index (one seek each), compare each entitlement's digest between
// base and applied — one root read per side when nothing changed, the
// overwhelmingly common case — and materialize only the principal-hash
// buckets the comparison flags as dirty. Grants in dirty buckets are
// probed against base's PRIMARY keyspace by external_id, which
// preserves the additions-only contract exactly: a grant whose
// external_id exists in base under a different entitlement/principal
// is still "present in base" (not emitted), which is why the probe
// targets the primary key rather than comparing index keys.
//
// Why pruning is sound: equal (count, digest) node pairs mean the two
// sides hold identical grant content-hash sets, and the content hash
// folds external_id — so a clean entitlement/bucket cannot contain an
// applied external_id that base lacks. Dirtiness over-approximates
// additions (it also fires for removals and source-set changes, which
// the base probe then filters out), never under-approximates them.
//
// NOTE: grants without an entitlement or principal ref have no
// hash-index entry and are invisible to the digest. They are silently
// skipped. A future O(1) coverage check (e.g. a stored grant count in
// the sync run record) will restore detection of this case.
func diffGrants(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, baseSyncID, appliedSyncID, diffSyncID string) error {
	eng := a.engine

	ents, err := eng.distinctDigestPartitions(ctx, grantDigestSpec, appliedBytes)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Entitlements only present in base (fully removed) are not in
		// `ents` and are correctly skipped: they cannot contain
		// additions. Digests missing on either side (e.g. a ghost
		// entitlement with grants but no entitlement record) degrade
		// to an on-demand index fold inside DirtyEntitlementBuckets.
		dirty, err := eng.DirtyEntitlementBuckets(ctx, baseSyncID, eng, appliedSyncID, ent)
		if err != nil {
			return err
		}
		for _, bucket := range dirty {
			var innerErr error
			err := eng.IterateGrantsByEntitlementBucket(ctx, appliedSyncID, ent, bucket, func(rec *v3.GrantRecord) bool {
				exists, probeErr := existsAt(eng.DB(), encodeGrantKey(baseBytes, rec.GetExternalId()))
				if probeErr != nil {
					innerErr = probeErr
					return false
				}
				if exists {
					return true
				}
				if putErr := eng.PutGrantRecord(ctx, rec); putErr != nil {
					innerErr = putErr
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
			if innerErr != nil {
				return innerErr
			}
		}
	}
	return nil
}

// diffGrantsFullScan is the O(applied grants) path: iterate every
// applied grant, probe base by external_id, emit on miss. Used
// directly by benchmarks and available as a correctness reference.
func diffGrantsFullScan(ctx context.Context, a *Adapter, baseBytes, appliedBytes []byte, diffSyncID string) error {
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
