package pebble

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

type ledgerReferenceExample struct {
	Kind           string `json:"kind"`
	Op             string `json:"operation"`
	ResourceTypeID string `json:"resource_type_id,omitempty"`
	ResourceID     string `json:"resource_id,omitempty"`
}
type ledgerReferenceStats struct {
	RowsChecked          uint64                   `json:"rows_checked"`
	Lookups              uint64                   `json:"lookups"`
	MissingChildren      uint64                   `json:"missing_child_references"`
	MissingContinuations uint64                   `json:"missing_continuation_references"`
	IdentityMismatches   uint64                   `json:"identity_mismatches"`
	Uncheckable          uint64                   `json:"uncheckable_references"`
	Examples             []ledgerReferenceExample `json:"warning_examples"`
}

func (s *ledgerReferenceStats) example(kind string, id *v3.LedgerActionIdentity) {
	if len(s.Examples) < 16 {
		s.Examples = append(s.Examples, ledgerReferenceExample{kind, id.GetOp(), id.GetResourceTypeId(), id.GetResourceId()})
	}
}
func ledgerReferenceKey(id *v3.LedgerActionIdentity, hash []byte, scrubbed bool) ([]byte, bool) {
	if id == nil {
		return nil, false
	}
	if !scrubbed {
		hash = ledgerTokenHash(id.GetPageToken())
	}
	if len(hash) != ledgerTokenHashLen {
		return nil, false
	}
	return encodeLedgerKeyWithHash(ledgerIdentityFromProto(id), hash), true
}

type ledgerReferenceLookup interface {
	SeekGE([]byte) bool
	Key() []byte
	Error() error
}

func scanLedgerReferences(ctx context.Context, iterator ledgerReportIterator, references ledgerReferenceLookup) (*ledgerReferenceStats, error) {
	stats := &ledgerReferenceStats{}
	check := func(id *v3.LedgerActionIdentity, hash []byte, scrubbed, child bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, ok := ledgerReferenceKey(id, hash, scrubbed)
		if !ok {
			stats.Uncheckable++
			stats.example("uncheckable", id)
			return nil
		}
		stats.Lookups++
		found := references.SeekGE(key)
		if err := references.Error(); err != nil {
			return err
		}
		if !found || !bytes.Equal(key, references.Key()) {
			if child {
				stats.MissingChildren++
				stats.example("missing_child", id)
			} else {
				stats.MissingContinuations++
				stats.example("missing_continuation", id)
			}
		}
		return nil
	}
	scan := func() error {
		for iterator.First(); iterator.Valid(); iterator.Next() {
			stats.RowsChecked++
			if err := ctx.Err(); err != nil {
				return err
			}
			row := &v3.LedgerRow{}
			if err := unmarshalRecord(iterator.Value(), row); err != nil {
				return err
			}
			id := row.GetIdentity()
			key, ok := ledgerReferenceKey(id, id.GetPageTokenHash(), row.GetScrubbed())
			if !ok || !bytes.Equal(key, iterator.Key()) {
				stats.IdentityMismatches++
				stats.example("identity_mismatch", id)
			}
			hash := row.GetNextPageTokenHash()
			if len(hash) == 0 && !row.GetScrubbed() {
				hash = ledgerTokenHash(row.GetNextPageToken())
			}
			if !bytes.Equal(hash, ledgerTokenHash("")) {
				if err := check(id, hash, true, false); err != nil {
					return err
				}
			}
			for _, child := range row.GetChildren() {
				id := child.GetIdentity()
				// Local processing phases have frontier entries, not collection page rows.
				if id.GetOp() == "grant-expansion" || id.GetOp() == "list-external-resources" {
					continue
				}
				if err := check(id, id.GetPageTokenHash(), row.GetScrubbed(), true); err != nil {
					return err
				}
			}
		}
		return iterator.Error()
	}
	return stats, scan()
}

func (e *Engine) validateLedgerReferences(ctx context.Context) (*ledgerReferenceStats, error) {
	lo := rawdb.LedgerKeyPrefix()
	bounds := &pebble.IterOptions{LowerBound: lo, UpperBound: upperBoundOf(lo)}
	iterator, err := e.db.NewIter(bounds)
	if err != nil {
		return nil, err
	}
	references, err := e.db.NewIter(bounds)
	if err != nil {
		return nil, errors.Join(err, iterator.Close())
	}
	stats, err := scanLedgerReferences(ctx, iterator, references)
	return stats, errors.Join(err, iterator.Close(), references.Close())
}
