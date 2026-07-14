package pebble

// Ingestion facts: engine-maintained evidence about rows, consumed by the
// syncer's ingestion invariants (see
// docs/tasks/source-cache-ingestion-invariants.md).
//
// Facts are chosen by density. Sparse facts get partial indexes
// (by_needs_expansion). Dense facts — annotations that the biggest
// connectors stamp on ~100% of grants — get a MONOTONE existence bit
// (this file) or no structure at all (read from row values on demand).
// A dense inline secondary index is deliberately rejected: it is the
// cost class the engine already refuses (the reason by_principal is a
// deferred build).
//
// The existence bit mirrors the deferred-index pending marker exactly:
// an atomic bool for in-process reads, a durable engine-meta key so a
// crash/resume keeps the fact, restored at Open, cleared by
// ResetForNewSync. Monotonicity (set-once, never unset within a sync)
// makes the bit order-independent under parallel workers and mixed
// stream/replay ingestion.

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

// encodeExternalMatchFactKey is the durable engine-meta marker for "this
// sync wrote at least one grant carrying an ExternalResourceMatch*
// annotation". Engine-meta (typeEngineMeta) survives ResetForNewSync's
// record-span excise, so the reset path clears it explicitly.
func encodeExternalMatchFactKey() []byte {
	buf := make([]byte, 0, 2+len("fact_external_match_grants"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "fact_external_match_grants")
}

// markExternalMatchFact records the existence fact, durably. CAS keeps
// the durable write to one per sync, off the per-record hot path: after
// the first hit, callers pay one atomic load.
func (e *Engine) markExternalMatchFact() error {
	if !e.externalMatchFact.CompareAndSwap(false, true) {
		return nil
	}
	if err := e.db.Set(encodeExternalMatchFactKey(), nil, pebble.Sync); err != nil {
		// Roll back so a retried write re-attempts the durable marker;
		// an armed flag without the durable key would diverge from a
		// crash/resume of the same sync (same rationale as
		// markDeferredIdxPending).
		e.externalMatchFact.Store(false)
		return fmt.Errorf("arm external-match fact marker: %w", err)
	}
	return nil
}

// HasExternalMatchGrants reports the existence fact for the open sync.
func (e *Engine) HasExternalMatchGrants() bool {
	return e.externalMatchFact.Load()
}

// restoreIngestFactMarkers reloads durable fact markers at Open (a prior
// process may have set them before crashing; a finished file carries them
// for read-side consumers).
func (e *Engine) restoreIngestFactMarkers() error {
	if _, closer, err := e.db.Get(encodeExternalMatchFactKey()); err == nil {
		closer.Close()
		e.externalMatchFact.Store(true)
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	return nil
}

// clearIngestFactMarkers drops the durable markers and in-memory bits.
// Called by ResetForNewSync, whose record-span excise does not reach the
// engine-meta keyspace.
func (e *Engine) clearIngestFactMarkers() error {
	e.externalMatchFact.Store(false)
	return e.db.Delete(encodeExternalMatchFactKey(), pebble.Sync)
}

// grantRecordHasExternalMatch reports whether a translated GrantRecord's
// annotations carry any ExternalResourceMatch* type. Called on the put
// path only while the fact bit is still unset, so the per-record cost
// vanishes after the first hit.
func grantRecordHasExternalMatch(anns []*anypb.Any) bool {
	for _, a := range anns {
		url := a.GetTypeUrl()
		name := url
		if i := strings.LastIndexByte(url, '/'); i >= 0 {
			name = url[i+1:]
		}
		switch name {
		case anyTypeExternalResourceMatch, anyTypeExternalResourceMatchAll, anyTypeExternalResourceMatchID:
			return true
		}
	}
	return false
}

// noteGrantRecordFacts inspects one to-be-written grant record for dense
// facts. Cheap by construction: one atomic load when the bit is already
// set; the annotation walk only runs until the sync's first hit.
func (e *Engine) noteGrantRecordFacts(r *v3.GrantRecord) error {
	if e.externalMatchFact.Load() {
		return nil
	}
	if grantRecordHasExternalMatch(r.GetAnnotations()) {
		return e.markExternalMatchFact()
	}
	return nil
}

// ForEachDistinctGrantEntitlementResource visits each distinct
// (entitlement resource_type_id, resource_id) pair present in the grant
// primary keyspace, in key order. The keyspace leads with
// ent_rt | ent_rid, so distinctness is a prefix-skip: one seek per
// distinct resource — O(distinct) seeks, never O(grants). Backs the
// syncer's grant→resource referential invariant.
func (e *Engine) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
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

// GrantsForEntResourceCarryInsertFact reports whether any grant under the
// given entitlement resource carries the InsertResourceGrants annotation.
// Reads row values, so it is reserved for DANGLING referential probes —
// rare to zero on healthy syncs — never the bulk path.
func (e *Engine) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
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
		flags, err := scanGrantSourceCacheFlagsRaw(iter.Value())
		if err != nil {
			return false, err
		}
		if flags.insertResourceGrants {
			return true, nil
		}
	}
	return false, iter.Error()
}

// HasResourceRecord reports whether a resource row exists — the probe
// side of the referential invariant.
func (e *Engine) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
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
