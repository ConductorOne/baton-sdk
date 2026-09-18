package pebble

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (u *pageUnit) stageExpandedGrants(records []*v3.GrantRecord) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	if len(records) == 0 {
		return nil
	}
	if u.expandedGrants == nil {
		u.expandedGrants = make(map[*v3.GrantRecord]struct{}, len(records))
	}
	for _, record := range records {
		u.expandedGrants[record] = struct{}{}
		u.grants = append(u.grants, record)
	}
	return nil
}

type pageGrantWrite struct {
	last     int
	prior    *v3.GrantRecord
	expanded bool
}

func (u *pageUnit) stagePageGrantRecords(batch *rawdb.RecordBatch) (uint64, error) {
	e := u.l.e
	if len(u.expandedGrants) == 0 {
		return e.stageGrantRecords(batch, u.grants)
	}
	latest := make(map[grantIdentity]pageGrantWrite, len(u.grants))
	for i, rec := range u.grants {
		id, err := grantIdentityFromRecord(rec)
		if err != nil {
			return 0, err
		}
		state := latest[id]
		state.last = i
		_, state.expanded = u.expandedGrants[rec]
		if !state.expanded {
			state.prior = rec
		}
		latest[id] = state
	}
	normal := make([]*v3.GrantRecord, 0, len(latest))
	for _, state := range latest {
		if !state.expanded {
			normal = append(normal, u.grants[state.last])
		}
	}
	count, err := e.stageGrantRecords(batch, normal)
	if err != nil {
		return 0, err
	}
	_ = e.takeFreshGrantsEmpty()
	e.expandedWriteCalls.Add(1)
	e.expandedWriteRows.Add(int64(len(u.expandedGrants)))
	now := timestamppb.Now()
	for id, state := range latest {
		if !state.expanded {
			continue
		}
		if err := u.stageExpandedGrant(batch, id, state, now); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func (u *pageUnit) stageExpandedGrant(batch *rawdb.RecordBatch, id grantIdentity, state pageGrantWrite, now *timestamppb.Timestamp) error {
	key := encodeGrantIdentityKey(id)
	oldVal, closer, err := u.l.e.db.Get(key)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if err == nil {
		defer closer.Close()
	} else {
		oldVal = nil
	}
	prior := state.prior
	if prior == nil && oldVal != nil {
		prior = &v3.GrantRecord{}
		if err := unmarshalRecord(oldVal, prior); err != nil {
			return err
		}
	}
	record := proto.CloneOf(u.grants[state.last])
	if prior != nil {
		record.SetExpansion(prior.GetExpansion())
		record.SetNeedsExpansion(prior.GetNeedsExpansion())
		record.SetDiscoveredAt(prior.GetDiscoveredAt())
		record.SetSourceScopeKey(prior.GetSourceScopeKey())
	}
	if record.GetDiscoveredAt() == nil {
		record.SetDiscoveredAt(now)
	}
	value, err := marshalRecord(record)
	if err != nil {
		return err
	}
	return batch.StageGrantPutDeferred(key, value, oldVal, record.GetNeedsExpansion())
}
