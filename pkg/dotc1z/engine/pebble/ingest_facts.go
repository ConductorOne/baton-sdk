package pebble

// Ingestion-invariant scan primitives: the engine mechanics behind the
// syncer's referential invariants (I3/I7/I8 in
// pkg/sync/ingest_invariants.go). All read-only, all O(distinct
// referents) by prefix-skip over the ordered keyspaces — never
// O(rows). Value reads happen only for DANGLING referents (rare to
// zero on healthy syncs), never on the bulk path.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/anypb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Annotation type names the invariant probes match against (the tail
// of the anypb type URL). Matched by name to avoid unmarshaling the
// annotation payloads — existence is the fact.
const (
	anyTypeInsertResourceGrants     = "c1.connector.v2.InsertResourceGrants"
	anyTypeExternalResourceMatch    = "c1.connector.v2.ExternalResourceMatch"
	anyTypeExternalResourceMatchAll = "c1.connector.v2.ExternalResourceMatchAll"
	anyTypeExternalResourceMatchID  = "c1.connector.v2.ExternalResourceMatchID"
)

// annsContainType reports whether any annotation's type URL names typeName.
func annsContainType(anns []*anypb.Any, typeName string) bool {
	for _, a := range anns {
		url := a.GetTypeUrl()
		name := url
		if i := strings.LastIndexByte(url, '/'); i >= 0 {
			name = url[i+1:]
		}
		if name == typeName {
			return true
		}
	}
	return false
}

// grantRecordHasExternalMatch reports whether a GrantRecord's
// annotations carry any ExternalResourceMatch* type — I9's unprocessed
// match-carrier exemption.
func grantRecordHasExternalMatch(anns []*anypb.Any) bool {
	return annsContainType(anns, anyTypeExternalResourceMatch) ||
		annsContainType(anns, anyTypeExternalResourceMatchAll) ||
		annsContainType(anns, anyTypeExternalResourceMatchID)
}

// grantValueCarriesInsertFact reports whether a raw grant row value's
// annotations carry InsertResourceGrants. Unmarshals the record —
// acceptable because every caller probes DANGLING referents only.
func grantValueCarriesInsertFact(val []byte) (bool, error) {
	rec := &v3.GrantRecord{}
	if err := unmarshalRecord(val, rec); err != nil {
		return false, fmt.Errorf("insert-fact probe: unmarshal grant record: %w", err)
	}
	return annsContainType(rec.GetAnnotations(), anyTypeInsertResourceGrants), nil
}

// ForEachDistinctGrantEntitlementResource visits each distinct
// (entitlement resource_type_id, resource_id) pair present in the grant
// primary keyspace, in key order. The keyspace leads with
// ent_rt | ent_rid, so distinctness is a prefix-skip: one seek per
// distinct resource — O(distinct) seeks, never O(grants). Backs the
// syncer's grant→resource referential invariant (I3).
func (e *Engine) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	prefix := encodeGrantPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	const headerLen = 3 // versionV3, typeGrant, separator
	for valid := iter.First(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if len(key) <= headerLen {
			return fmt.Errorf("distinct ent-resource scan: malformed grant key %x", key)
		}
		tail := key[headerLen:]
		rtBytes, next, ok := codec.DecodeTupleStringAlias(tail, 0)
		if !ok || next >= len(tail) {
			return fmt.Errorf("distinct ent-resource scan: malformed grant key tail %x", key)
		}
		ridBytes, _, ok := codec.DecodeTupleStringAlias(tail, next+1)
		if !ok {
			return fmt.Errorf("distinct ent-resource scan: malformed grant key tail %x", key)
		}
		rt, rid := string(rtBytes), string(ridBytes)
		if err := visit(rt, rid); err != nil {
			return err
		}
		// Skip every remaining grant of this entitlement resource.
		valid = iter.SeekGE(upperBoundOf(encodeGrantPrimaryEntitlementResourcePrefix(rt, rid)))
	}
	return iter.Error()
}

// ForEachDistinctEntitlementResource visits each distinct
// (resource_type_id, resource_id) pair present in the entitlement
// primary keyspace, in key order. Same prefix-skip shape as the grant
// scan: one seek per distinct resource, never O(entitlements). Backs
// the syncer's entitlement→resource referential invariant (I7).
func (e *Engine) ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	prefix := encodeEntitlementPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	const headerLen = 3 // versionV3, typeEntitlement, separator
	for valid := iter.First(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if len(key) <= headerLen {
			return fmt.Errorf("distinct entitlement-resource scan: malformed entitlement key %x", key)
		}
		tail := key[headerLen:]
		rtBytes, next, ok := codec.DecodeTupleStringAlias(tail, 0)
		if !ok || next >= len(tail) {
			return fmt.Errorf("distinct entitlement-resource scan: malformed entitlement key tail %x", key)
		}
		ridBytes, _, ok := codec.DecodeTupleStringAlias(tail, next+1)
		if !ok {
			return fmt.Errorf("distinct entitlement-resource scan: malformed entitlement key tail %x", key)
		}
		rt, rid := string(rtBytes), string(ridBytes)
		if err := visit(rt, rid); err != nil {
			return err
		}
		valid = iter.SeekGE(upperBoundOf(encodeEntitlementPrimaryResourcePrefix(rt, rid)))
	}
	return iter.Error()
}

// ForEachDanglingGrantEntitlement visits each distinct entitlement
// identity referenced by grants for which NO entitlement row exists.
// The grant primary key leads with the full entitlement identity tuple
// (which is byte-identical to the entitlement primary key's tuple), so
// the scan is one seek per distinct entitlement plus one point-Get
// probe each — O(distinct entitlements), never O(grants), and no value
// reads: the identity tuple reconstructs the external id byte-exactly
// (see identity.go). Backs the syncer's grant→entitlement referential
// invariant (I8).
func (e *Engine) ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error {
	if e.db == nil {
		return ErrEngineClosing
	}
	prefix := encodeGrantPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	const headerLen = 3 // versionV3, typeGrant, separator
	for valid := iter.First(); valid; {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := iter.Key()
		if len(key) <= headerLen {
			return fmt.Errorf("dangling grant-entitlement scan: malformed grant key %x", key)
		}
		tail := key[headerLen:]
		var comps [4]string
		off := 0
		for i := range comps {
			b, next, ok := codec.DecodeTupleStringAlias(tail, off)
			if !ok || (i < len(comps)-1 && next >= len(tail)) {
				return fmt.Errorf("dangling grant-entitlement scan: malformed grant key tail %x", key)
			}
			comps[i] = string(b)
			off = next + 1
		}
		id := entitlementIdentity{
			resourceTypeID: comps[0],
			resourceID:     comps[1],
			stripped:       comps[2] == idFlagStripped,
			tail:           comps[3],
		}
		exists, err := e.hasEntitlementIdentity(id)
		if err != nil {
			return err
		}
		if !exists {
			if err := visit(id.externalID(), id.resourceTypeID, id.resourceID); err != nil {
				return err
			}
		}
		// Skip every remaining grant of this entitlement.
		valid = iter.SeekGE(upperBoundOf(encodeGrantPrimaryEntitlementPrefix(id)))
	}
	return iter.Error()
}

func (e *Engine) hasEntitlementIdentity(id entitlementIdentity) (bool, error) {
	_, closer, err := e.db.Get(encodeEntitlementIdentityKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	closer.Close()
	return true, nil
}

// GrantsForEntitlementAllCarryInsertFact reports whether EVERY grant
// under the given entitlement identity carries the InsertResourceGrants
// annotation — the probe for I8's per-grant exemption: a dangling
// entitlement whose grants all carry the fact is the machinery-owned
// shape (exempt); one non-carrying grant makes it a connector-owned
// dangling reference. Vacuously true with no grants (never the case for
// a dangling entitlement, which is only visited because grants
// reference it). Reads row values, so it is reserved for DANGLING
// referential probes — rare to zero on healthy syncs.
func (e *Engine) GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error) {
	if e.db == nil {
		return false, ErrEngineClosing
	}
	entID := entitlementIdentityFromParts(entResourceTypeID, entResourceID, entitlementID)
	prefix := encodeGrantPrimaryEntitlementPrefix(entID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		carries, err := grantValueCarriesInsertFact(iter.Value())
		if err != nil {
			return false, err
		}
		if !carries {
			return false, nil
		}
	}
	return true, iter.Error()
}

// GrantsForEntResourceCarryInsertFact reports whether any grant under the
// given entitlement resource carries the InsertResourceGrants annotation.
// Reads row values, so it is reserved for DANGLING referential probes —
// rare to zero on healthy syncs — never the bulk path.
func (e *Engine) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	if e.db == nil {
		return false, ErrEngineClosing
	}
	prefix := encodeGrantPrimaryEntitlementResourcePrefix(resourceTypeID, resourceID)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		carries, err := grantValueCarriesInsertFact(iter.Value())
		if err != nil {
			return false, err
		}
		if carries {
			return true, nil
		}
	}
	return false, iter.Error()
}

// HasResourceRecord reports whether a resource row exists — the probe
// side of the referential invariants.
func (e *Engine) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	if e.db == nil {
		return false, ErrEngineClosing
	}
	_, closer, err := e.db.Get(encodeResourceKey(resourceTypeID, resourceID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	closer.Close()
	return true, nil
}
