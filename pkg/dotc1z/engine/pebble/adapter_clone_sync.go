package pebble

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// cloneSync is the shared CloneSync implementation reached by
// pebbleFileOps.CloneSync. Materializes the named sync's data into
// a freshly-created Pebble-backed c1z at outPath.
//
// Strategy: open a fresh Pebble engine under a temp dir, range-copy
// every key prefix that's scoped to (sync_id) — both primary
// records and index entries — from the source engine into the
// destination. Then Checkpoint the destination and emit a c1z v3
// envelope at outPath. The copy is byte-level (proto values aren't
// re-encoded; the schema_version is preserved).
//
// Defaults syncID to the latest finished SyncTypeFull when empty.
// Errors if the sync isn't ended (mirrors the SQLite contract in
// clone_sync.go).
func cloneSync(
	ctx context.Context,
	a *Adapter,
	encoding c1zstore.PayloadEncoding,
	outPath, syncID string,
	_ ...c1zstore.CloneSyncOption,
) error {
	//TODO: Support options in pebble.
	if _, err := os.Stat(outPath); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clone-sync: output path (%s) must not exist for cloning to proceed", outPath)
	}

	resolved := syncID
	if resolved == "" {
		latest, err := a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeFull)
		if err != nil {
			return err
		}
		if latest == "" {
			return fmt.Errorf("clone-sync: no finished full sync available")
		}
		resolved = latest
	}

	srcRun, err := a.engine.GetSyncRunRecord(ctx, resolved)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return fmt.Errorf("clone-sync: sync %q not found", resolved)
		}
		return err
	}
	if srcRun.GetEndedAt() == nil {
		return fmt.Errorf("clone-sync: sync %q is not ended", resolved)
	}

	syncIDBytes, err := codec.EncodeSyncID(resolved)
	if err != nil {
		return fmt.Errorf("clone-sync: encode sync_id: %w", err)
	}

	cloneTmp, err := os.MkdirTemp("", "c1z-clone")
	if err != nil {
		return err
	}
	defer os.RemoveAll(cloneTmp)
	destDBDir := filepath.Join(cloneTmp, "db")

	dest, err := Open(ctx, destDBDir)
	if err != nil {
		return fmt.Errorf("clone-sync: open dest engine: %w", err)
	}
	defer func() { _ = dest.Close() }()

	// Every keyspace that's scoped by sync_id — primary keys plus
	// the four secondary indexes. Listed in adjacent pairs so the
	// destination always lands a record's primary alongside its
	// index entries.
	ranges := [][2][]byte{
		{encodeSyncRunKey(syncIDBytes), upperBoundOf(encodeSyncRunKey(syncIDBytes))},
		{encodeResourceTypePrefix(syncIDBytes), upperBoundOf(encodeResourceTypePrefix(syncIDBytes))},
		{encodeResourcePrefix(syncIDBytes), upperBoundOf(encodeResourcePrefix(syncIDBytes))},
		{ResourceByParentSyncLowerBound(syncIDBytes), ResourceByParentSyncUpperBound(syncIDBytes)},
		{encodeEntitlementPrefix(syncIDBytes), upperBoundOf(encodeEntitlementPrefix(syncIDBytes))},
		{EntitlementByResourceSyncLowerBound(syncIDBytes), EntitlementByResourceSyncUpperBound(syncIDBytes)},
		{encodeGrantPrefix(syncIDBytes), upperBoundOf(encodeGrantPrefix(syncIDBytes))},
		{GrantByEntitlementSyncLowerBound(syncIDBytes), GrantByEntitlementSyncUpperBound(syncIDBytes)},
		{GrantByEntitlementResourceSyncLowerBound(syncIDBytes), GrantByEntitlementResourceSyncUpperBound(syncIDBytes)},
		{GrantByPrincipalSyncLowerBound(syncIDBytes), GrantByPrincipalSyncUpperBound(syncIDBytes)},
		{GrantByPrincipalResourceTypeSyncLowerBound(syncIDBytes), GrantByPrincipalResourceTypeSyncUpperBound(syncIDBytes)},
		{GrantByNeedsExpansionSyncLowerBound(syncIDBytes), GrantByNeedsExpansionSyncUpperBound(syncIDBytes)},
		{encodeAssetPrefix(syncIDBytes), upperBoundOf(encodeAssetPrefix(syncIDBytes))},
		// Stats sidecar — single key per sync; copyRange's [lo, hi)
		// shape requires a half-open range, so we synthesize one
		// that contains exactly this sync's stats key.
		{encodeSyncStatsKey(syncIDBytes), upperBoundOf(encodeSyncStatsKey(syncIDBytes))},
	}
	for _, r := range ranges {
		if err := copyRange(ctx, a.engine.DB(), dest.DB(), r[0], r[1]); err != nil {
			return fmt.Errorf("clone-sync: copy range: %w", err)
		}
	}

	// Checkpoint the destination and emit the envelope at outPath.
	checkpointDir := filepath.Join(cloneTmp, "checkpoint")
	if err := dest.CheckpointTo(ctx, checkpointDir); err != nil {
		return fmt.Errorf("clone-sync: checkpoint: %w", err)
	}

	tmpPath := outPath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if out != nil {
			_ = out.Close()
		}
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	manifest, err := BuildManifest(encoding)
	if err != nil {
		return err
	}
	if err := formatv3.WriteEnvelope(out, manifest, checkpointDir); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}
	success = true
	return nil
}

// copyRange iterates [lower, upper) on src and Sets each key/value
// onto dst in fixed-size batches, committing each batch with
// pebble.Sync. Chunking caps the per-batch memory at
// copyRangeBatchKeys keys regardless of total range size, so a
// clone of a 1M-grant sync uses ~80 batches instead of one
// gigabyte-scale batch.
func copyRange(ctx context.Context, src, dst *pebble.DB, lower, upper []byte) error {
	iter, err := src.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	batch := dst.NewBatch()
	defer func() { _ = batch.Close() }()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := batch.Set(iter.Key(), iter.Value(), nil); err != nil {
			return err
		}
		count++
		if count >= copyRangeBatchKeys {
			if err := batch.Commit(pebble.Sync); err != nil {
				return err
			}
			_ = batch.Close()
			batch = dst.NewBatch()
			count = 0
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if batch.Empty() {
		return nil
	}
	return batch.Commit(pebble.Sync)
}

// copyRangeBatchKeys caps copyRange's per-batch memory footprint.
// Chosen at 10k keys to roughly match the engine's DefaultPageSize;
// larger batches don't improve commit throughput once Pebble's
// memtable already absorbs the writes.
const copyRangeBatchKeys = 10_000
