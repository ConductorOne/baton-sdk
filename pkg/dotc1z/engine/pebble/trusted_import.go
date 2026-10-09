package pebble

import (
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

func stageTrustedResourceRecords(batch *rawdb.RecordBatch, records []*v3.ResourceRecord) (uint64, error) {
	last := make(map[resourceBufKey]int, len(records))
	for i, record := range records {
		if record != nil {
			last[resourceBufKey{record.GetResourceTypeId(), record.GetResourceId()}] = i
		}
	}
	var staged uint64
	for i, record := range records {
		if record == nil || last[resourceBufKey{record.GetResourceTypeId(), record.GetResourceId()}] != i {
			continue
		}
		value, err := marshalRecord(record)
		if err != nil {
			return 0, err
		}
		if err := batch.StageResourcePut(
			encodeResourceKey(record.GetResourceTypeId(), record.GetResourceId()),
			value,
			nil,
			record.GetResourceTypeId(),
			record.GetResourceId(),
		); err != nil {
			return 0, err
		}
		staged++
	}
	return staged, nil
}

func stageTrustedEntitlementRecords(batch *rawdb.RecordBatch, records []*v3.EntitlementRecord) (uint64, error) {
	last := make(map[entitlementIdentity]int, len(records))
	for i, record := range records {
		if record == nil {
			continue
		}
		identity, err := entitlementIdentityFromRecord(record)
		if err != nil {
			return 0, err
		}
		last[identity] = i
	}
	var staged uint64
	for i, record := range records {
		if record == nil {
			continue
		}
		identity, err := entitlementIdentityFromRecord(record)
		if err != nil {
			return 0, err
		}
		if last[identity] != i {
			continue
		}
		value, err := marshalRecord(record)
		if err != nil {
			return 0, err
		}
		if err := batch.StageEntitlementPut(encodeEntitlementIdentityKey(identity), value, nil); err != nil {
			return 0, err
		}
		staged++
	}
	return staged, nil
}

func stageTrustedGrantRecords(batch *rawdb.RecordBatch, records []*v3.GrantRecord) (uint64, error) {
	last := make(map[grantIdentity]int, len(records))
	for i, record := range records {
		if record == nil {
			continue
		}
		identity, err := grantIdentityFromRecord(record)
		if err != nil {
			return 0, err
		}
		last[identity] = i
	}
	var staged uint64
	for i, record := range records {
		if record == nil {
			continue
		}
		identity, err := grantIdentityFromRecord(record)
		if err != nil {
			return 0, err
		}
		if last[identity] != i {
			continue
		}
		value, err := marshalRecord(record)
		if err != nil {
			return 0, err
		}
		if err := batch.StageGrantPutTrusted(encodeGrantIdentityKey(identity), value, record.GetNeedsExpansion()); err != nil {
			return 0, err
		}
		staged++
	}
	return staged, nil
}
