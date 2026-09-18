package pebble

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

func (u *pageUnit) stageResourceDeletes(ids []resourceBufKey) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.resourceDeletes = append(u.resourceDeletes, ids...)
	return nil
}

func (u *pageUnit) stageEntitlementDeletes(ids []entitlementIdentity) error {
	if u.done {
		return ErrPageUnitCommitted
	}
	u.entitlementDeletes = append(u.entitlementDeletes, ids...)
	return nil
}

func (u *pageUnit) stageRecordDeletes(batch *rawdb.RecordBatch) error {
	seenResources := make(map[resourceBufKey]struct{}, len(u.resourceDeletes))
	for _, id := range u.resourceDeletes {
		if _, seen := seenResources[id]; seen {
			continue
		}
		seenResources[id] = struct{}{}
		var staged []byte
		if i, found := u.resourceIdx[id]; found {
			var err error
			staged, err = marshalRecord(u.resources[i])
			if err != nil {
				return err
			}
		}
		key := encodeResourceKey(id.rt, id.id)
		if err := u.deleteRecordValue(key, staged, func(value []byte) error {
			return batch.StageResourceDelete(key, value, id.rt, id.id)
		}); err != nil {
			return err
		}
	}
	if len(u.entitlementDeletes) == 0 {
		return nil
	}
	latest := make(map[entitlementIdentity]int, len(u.entitlements))
	for i, rec := range u.entitlements {
		id, err := entitlementIdentityFromRecord(rec)
		if err != nil {
			return err
		}
		latest[id] = i
	}
	seenEntitlements := make(map[entitlementIdentity]struct{}, len(u.entitlementDeletes))
	for _, id := range u.entitlementDeletes {
		if _, seen := seenEntitlements[id]; seen {
			continue
		}
		seenEntitlements[id] = struct{}{}
		var staged []byte
		if i, found := latest[id]; found {
			var err error
			staged, err = marshalRecord(u.entitlements[i])
			if err != nil {
				return err
			}
		}
		key := encodeEntitlementIdentityKey(id)
		if err := u.deleteRecordValue(key, staged, func(value []byte) error {
			return batch.StageEntitlementDelete(key, value)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (u *pageUnit) deleteRecordValue(key, staged []byte, stageDelete func([]byte) error) error {
	if staged != nil {
		return stageDelete(staged)
	}
	value, closer, err := u.l.e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	return stageDelete(value)
}
