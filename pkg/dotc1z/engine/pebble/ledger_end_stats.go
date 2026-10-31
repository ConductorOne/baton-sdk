package pebble

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (e *Engine) restoreEndSyncStats(ctx context.Context, syncID string) (bool, error) {
	facts, err := e.ledger.Facts(ctx)
	if err != nil {
		return false, err
	}
	counters, err := e.ledger.Counters(ctx)
	if err != nil {
		return false, err
	}
	lo, hi := rawdb.LedgerBounds()
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return false, err
	}
	present := it.First()
	if present && bytes.Equal(it.Key(), encodeLedgerFactKey(c1zstore.LedgerFactDiscardOnSeal)) {
		present = it.Next()
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return false, err
	}
	if !present {
		archive, err := e.readLedgerArchive(ctx)
		if err != nil {
			return false, err
		}
		if archive != nil && archive.SyncID == syncID {
			facts, counters = archive.Facts, archive.Counters
		}
	}
	active, err := e.ledger.active()
	if err != nil {
		return false, err
	}
	if active || len(facts) != 0 {
		e.setSyncStatsOverlay(syncID, syncStatsOverlay(c1zstore.LedgerSyncStats(facts, counters)))
	}
	return c1zstore.SampledFromCounters(counters.Counters), nil
}
