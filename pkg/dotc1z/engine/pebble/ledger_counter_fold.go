package pebble

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

func foldedLedgerCounterKey() []byte {
	return encodeLedgerCounterKey("", c1zstore.TakeoverBucketWorker)
}

func (l *Ledger) FoldCounters(ctx context.Context, currentRunID string) error {
	if currentRunID == "" {
		return errors.New("counter fold requires the current attempt ID")
	}
	return l.e.withWrite(func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := l.e.requireCurrentSync(); err != nil {
			return err
		}
		prefix := codec.AppendTupleSeparator(codec.AppendTupleStrings(rawdb.LedgerCounterPrefix(), currentRunID))
		batch := l.e.db.NewRecordBatch()
		defer batch.Close()
		counters, needed, err := l.countersExcept(ctx, prefix, batch.StageLedgerCounterDelete)
		if err != nil || !needed {
			return err
		}
		value, err := marshalRecord(ledgerCountersToProto(counters))
		if err != nil {
			return err
		}
		if err := batch.StageLedgerCounterBucket(foldedLedgerCounterKey(), value); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return batch.Commit(pebble.Sync)
	})
}
