package pebble

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type ledgerReferenceExample struct {
	Kind           string `json:"kind"`
	Op             string `json:"operation"`
	ResourceTypeID string `json:"resource_type_id,omitempty"`
	ResourceID     string `json:"resource_id,omitempty"`
}
type ledgerReferenceStats struct {
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
func ledgerReferenceTargetIdentity(value []byte) (*v3.LedgerActionIdentity, bool, error) {
	var id *v3.LedgerActionIdentity
	var scrubbed bool
	err := ledgerReportFields(value, func(number protowire.Number, kind protowire.Type, value []byte) error {
		switch int(number) {
		case 1:
			if kind != protowire.BytesType {
				return errors.New("invalid reference identity")
			}
			encoded, _ := protowire.ConsumeBytes(value)
			id = &v3.LedgerActionIdentity{}
			return proto.Unmarshal(encoded, id)
		case 13:
			if kind != protowire.VarintType {
				return errors.New("invalid reference scrub flag")
			}
			flag, _ := protowire.ConsumeVarint(value)
			scrubbed = flag != 0
		}
		return nil
	})
	return id, scrubbed, err
}
func (e *Engine) validateLedgerReferences(ctx context.Context) (*ledgerReferenceStats, error) {
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
		value, closer, err := e.db.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			if child {
				stats.MissingChildren++
				stats.example("missing_child", id)
			} else {
				stats.MissingContinuations++
				stats.example("missing_continuation", id)
			}
			return nil
		}
		if err != nil {
			return err
		}
		got, gotScrubbed, err := ledgerReferenceTargetIdentity(value)
		closeErr := closer.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
		gotKey, ok := ledgerReferenceKey(got, got.GetPageTokenHash(), gotScrubbed)
		if !ok || !bytes.Equal(key, gotKey) {
			stats.IdentityMismatches++
			stats.example("identity_mismatch", id)
		}
		return nil
	}
	lo := rawdb.LedgerKeyPrefix()
	iterator, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: upperBoundOf(lo)})
	if err != nil {
		return nil, err
	}
	scan := func() error {
		for valid := iterator.First(); valid; valid = iterator.Next() {
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
				if err := check(id, id.GetPageTokenHash(), row.GetScrubbed(), true); err != nil {
					return err
				}
			}
		}
		return iterator.Error()
	}
	err = errors.Join(scan(), iterator.Close())
	return stats, err
}
