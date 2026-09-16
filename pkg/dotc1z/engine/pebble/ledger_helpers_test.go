package pebble

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// encodeLedgerPrefixOp bounds every row of one op (by-value prefix:
// trailing separator distinguishes tuple boundaries, see keys.go).
func encodeLedgerPrefixOp(op string) []byte {
	buf := append([]byte{}, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, op)
	return codec.AppendTupleSeparator(buf)
}

// encodeLedgerPrefixResource bounds every row of one op on one
// resource (all pages, all parents).
func encodeLedgerPrefixResource(op, resourceTypeID, resourceID string) []byte {
	buf := append([]byte{}, rawdb.LedgerKeyPrefix()...)
	buf = codec.AppendTupleStrings(buf, op, resourceTypeID, resourceID)
	return codec.AppendTupleSeparator(buf)
}

// mismatchCount reports how many GetRow calls found a row
// echoing a different identity since Open. Nonzero is a key-function
// bug to investigate, not a data-loss event (the page re-ran).
func (l *Ledger) mismatchCount() uint64 { return l.mismatches.Load() }

// iterate yields every page row in key order (op, then resource).
// Rows only: facts, counter buckets and the frontier are sibling
// sub-families (rawdb keyspace.go).
func (l *Ledger) iterate(ctx context.Context, yield func(*v3.LedgerRow) bool) error {
	lo, hi := rawdb.LedgerRowBounds()
	return l.iterateRange(lo, hi, yield)
}

// counterBucketCount reports how many buckets exist (tests).
func (l *Ledger) counterBucketCount(ctx context.Context) (int, error) {
	lo, hi := rawdb.LedgerCounterBounds()
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n, iter.Error()
}

// iterateByOp yields every row of one op.
func (l *Ledger) iterateByOp(ctx context.Context, op string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixOp(op)
	return l.iterateRange(lo, upperBoundOf(lo), yield)
}

// iterateByResource yields every row of one op on one resource.
func (l *Ledger) iterateByResource(ctx context.Context, op, resourceTypeID, resourceID string, yield func(*v3.LedgerRow) bool) error {
	lo := encodeLedgerPrefixResource(op, resourceTypeID, resourceID)
	return l.iterateRange(lo, upperBoundOf(lo), yield)
}

func (l *Ledger) iterateRange(lo, hi []byte, yield func(*v3.LedgerRow) bool) error {
	iter, err := l.e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		row := &v3.LedgerRow{}
		if err := unmarshalRecord(iter.Value(), row); err != nil {
			return fmt.Errorf("iterate ledger: %w", err)
		}
		if !yield(row) {
			return nil
		}
	}
	return iter.Error()
}

func (l *Ledger) rowCount(ctx context.Context) (uint64, error) {
	var n uint64
	err := l.iterate(ctx, func(*v3.LedgerRow) bool { n++; return true })
	return n, err
}

// retainTokensFlag reports the flag.
func (l *Ledger) retainTokensFlag() bool { return l.retainTokens.Load() }
