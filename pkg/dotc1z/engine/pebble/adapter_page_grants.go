package pebble

import (
	"context"
	"iter"
	"slices"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func (w *pageWriter) ListGrantsWithAnnotations(ctx context.Context) iter.Seq2[c1zstore.GrantAnnotation, error] {
	return func(yield func(c1zstore.GrantAnnotation, error) bool) {
		fail := func(err error) { yield(c1zstore.GrantAnnotation{}, err) }
		if err := w.requireSync(); err != nil {
			fail(err)
			return
		}
		if w.unit.done {
			fail(ErrPageUnitCommitted)
			return
		}
		staged := make(map[string]*v3.GrantRecord, len(w.unit.grants))
		deleted := make(map[string]bool, len(w.unit.grantDeletes))
		for _, id := range w.unit.grantDeletes {
			deleted[string(encodeGrantIdentityKey(id))] = true
		}
		for _, record := range w.unit.grants {
			id, err := grantIdentityFromRecord(record)
			if err != nil {
				fail(err)
				return
			}
			key := string(encodeGrantIdentityKey(id))
			if !deleted[key] {
				staged[key] = record
			}
		}
		keys := make([]string, 0, len(staged))
		for key := range staged {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		next := 0
		emit := func(record *v3.GrantRecord) bool {
			if err := ctx.Err(); err != nil {
				fail(err)
				return false
			}
			ent, principal := record.GetEntitlement(), record.GetPrincipal()
			return yield(c1zstore.GrantAnnotation{
				Grant: V3GrantToV2(record), Annotation: expansionRecordToV2(record.GetExpansion()),
				GrantExternalID: record.GetExternalId(), TargetEntitlementID: ent.GetEntitlementId(),
				PrincipalResourceTypeID: principal.GetResourceTypeId(), PrincipalResourceID: principal.GetResourceId(), NeedsExpansion: record.GetNeedsExpansion(),
			}, nil)
		}
		token := ""
		for {
			records, continuation, err := w.e.PaginateGrants(ctx, token, DefaultPageSize)
			if err != nil {
				fail(err)
				return
			}
			for _, record := range records {
				id, err := grantIdentityFromRecord(record)
				if err != nil {
					fail(err)
					return
				}
				key := string(encodeGrantIdentityKey(id))
				for next < len(keys) && keys[next] < key {
					if !emit(staged[keys[next]]) {
						return
					}
					next++
				}
				if next < len(keys) && keys[next] == key {
					if !emit(staged[keys[next]]) {
						return
					}
					next++
				} else if !deleted[key] {
					if !emit(record) {
						return
					}
				}
			}
			if continuation == "" {
				break
			}
			token = continuation
		}
		for next < len(keys) {
			if !emit(staged[keys[next]]) {
				return
			}
			next++
		}
	}
}
