package pebble

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

// Bare-id lookups.
//
// Identity is structural; the public id string is a lossy join kept only as
// an external contract. Readers that receive nothing but an id string
// (Get/Delete by id — CLI and reader-API edges, never the sync hot path)
// recover the structural components here. Parsing is a query plan, not
// identity: the resolution rule is "exactly one row whose public id equals
// the query string wins" — zero matches is NotFound, more than one is an
// explicit ambiguity error, and a guess is never written.
//
// SAFETY CONTRACT — who may call what:
//
//   - Sync and grant expansion must never depend on string resolution.
//     Their list/scan calls carry structured refs (the expander fetches
//     the entitlement record first; expanderStoreAdapter enforces this at
//     the boundary), and grant deletes go through DeleteGrantByIdentityRefs.
//     The one string the expansion path resolves is a GrantExpandable
//     source-entitlement id — connectors hand those over as bare strings —
//     and that resolves ONLY via the exact-match record map
//     (resolveEntitlementIdentityByExternalID): byte-equality against
//     stored external ids, same semantics as SQLite's external_id lookup,
//     erroring loudly on the (new-layout-only) ambiguous case.
//   - Combinatorial candidate probing — resolveGrantIdentityByExternalID
//     and the grant-keyspace split fallback inside
//     resolveGrantScanEntitlementIdentity — is reserved for interactive
//     edges: reader-API Get/Delete-grant by id (provisioner --revoke-grant,
//     the baton explorer) and list requests whose caller supplied only an
//     id string (cmd/baton flags). There an ambiguity error is a usable
//     answer for a human; in a sync it would be a correctness hazard.
//
// Entitlements resolve through a lazily built in-memory multimap from raw
// external id → identity, rebuilt when the entitlement keyspace changes
// (generation counter). The map is exact string-match semantics: identity
// reconstruction is bijective, so the reconstructed id in the map IS the
// stored external id. Entitlement cardinality is small relative to grants,
// so the map is memory-bounded and the one-time scan amortizes across
// lookups.
//
// Grants resolve by combinatorial candidate splitting of the concat shape
// `entitlement_id + ":" + principal_rt + ":" + principal_id`, probing each
// candidate identity with a point Get. Connector-custom grant ids with no
// concat shape (or whose splits match nothing) fall back to a full
// primary-keyspace scan matching the STORED external id — SQLite keyed
// rows by that id, so string reads must find them for parity. The scan is
// O(all grants) and acceptable ONLY because this path is a CLI/reader-API
// edge (see the safety contract above); sync and expansion never resolve
// grants by string.

// maxGrantIDCandidates caps the combinatorial split enumeration for one
// grant-id lookup. Ids with enough colons to exceed it are unresolvable by
// string per policy ("if we can't resolve safely, error").
const maxGrantIDCandidates = 4096

// maxBareIDColons caps the colon count a bare-id string may carry before
// the combinatorial machinery refuses outright: the split enumeration is
// O(colons²) (and each entitlement-scan fallback probe opens an iterator),
// so a pathological id — hostile or corrupt — must fail fast instead of
// grinding through billions of loop iterations that no context check can
// interrupt. Real ids carry a handful of colons; 64 is beyond any
// legitimate ARN-ish shape while keeping the worst case trivial.
const maxBareIDColons = 64

// noteEntitlementKeyspaceWrite invalidates the lazy entitlement id lookup
// map. Call after ANY mutation of the entitlement primary keyspace (batch
// writes, deletes, ingests, range replacements, resets).
func (e *Engine) noteEntitlementKeyspaceWrite() {
	e.entIDLookupGen.Add(1)
}

// entitlementIdentitiesForExternalID returns every entitlement identity
// whose raw external id equals externalID, via the lazily built map.
func (e *Engine) entitlementIdentitiesForExternalID(ctx context.Context, externalID string) ([]entitlementIdentity, error) {
	gen := e.entIDLookupGen.Load()
	e.entIDLookupMu.Lock()
	defer e.entIDLookupMu.Unlock()
	if e.entIDLookup == nil || e.entIDLookupBuiltGen != gen {
		m, err := e.buildEntitlementIDLookup(ctx)
		if err != nil {
			return nil, err
		}
		e.entIDLookup = m
		e.entIDLookupBuiltGen = gen
	}
	return e.entIDLookup[externalID], nil
}

// buildEntitlementIDLookup scans the entitlement primary keyspace once and
// groups identities by their reconstructed (== stored) external id. Only
// keys are decoded; values are never touched.
func (e *Engine) buildEntitlementIDLookup(ctx context.Context) (map[string][]entitlementIdentity, error) {
	prefix := encodeEntitlementPrefix()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	m := map[string][]entitlementIdentity{}
	var n int64
	for iter.First(); iter.Valid(); iter.Next() {
		n++
		if n&0x3FF == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		id, ok := decodeEntitlementIdentityKey(iter.Key())
		if !ok {
			continue
		}
		ext := id.externalID()
		m[ext] = append(m[ext], id)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return m, nil
}

// decodeEntitlementIdentityKey decodes an entitlement primary key back into
// its identity. Returns ok=false for keys that do not have the expected
// four-component shape.
func decodeEntitlementIdentityKey(key []byte) (entitlementIdentity, bool) {
	prefix := []byte{versionV3, typeEntitlement, 0}
	components, ok := decodeTupleComponents(key, prefix, 4)
	if !ok {
		return entitlementIdentity{}, false
	}
	return entitlementIdentity{
		resourceTypeID: components[0],
		resourceID:     components[1],
		stripped:       components[2] == idFlagStripped,
		tail:           components[3],
	}, true
}

// resolveEntitlementIdentityByExternalID applies the exactly-one rule to
// entitlementIdentitiesForExternalID: one match wins, zero is
// pebble.ErrNotFound, several is ErrAmbiguousExternalID.
func (e *Engine) resolveEntitlementIdentityByExternalID(ctx context.Context, externalID string) (entitlementIdentity, error) {
	matches, err := e.entitlementIdentitiesForExternalID(ctx, externalID)
	if err != nil {
		return entitlementIdentity{}, err
	}
	switch len(matches) {
	case 0:
		return entitlementIdentity{}, pebble.ErrNotFound
	case 1:
		return matches[0], nil
	default:
		return entitlementIdentity{}, fmt.Errorf("%w: entitlement id %q matches %d records on different resources",
			ErrAmbiguousExternalID, externalID, len(matches))
	}
}

// resolveGrantScanEntitlementIdentity resolves an entitlement id string for
// GRANT-scoped scans (list/iterate grants of an entitlement). It first
// resolves through the entitlement record map; when the id matches no
// record (grants can exist for entitlements the store never saw), it falls
// back to direct byte-split candidates of the prefix shape, keeping a
// candidate only when the grant primary keyspace actually has rows under
// it. Exactly-one rule throughout.
func (e *Engine) resolveGrantScanEntitlementIdentity(ctx context.Context, entitlementID string) (entitlementIdentity, error) {
	matches, err := e.entitlementIdentitiesForExternalID(ctx, entitlementID)
	if err != nil {
		return entitlementIdentity{}, err
	}
	switch {
	case len(matches) == 1:
		return matches[0], nil
	case len(matches) > 1:
		return entitlementIdentity{}, fmt.Errorf("%w: entitlement id %q matches %d records on different resources",
			ErrAmbiguousExternalID, entitlementID, len(matches))
	}
	// No entitlement record: probe direct (rt | rid | tail) splits against
	// the grant primary keyspace. Each probe opens a (bounded, single-seek)
	// iterator; the colon cap bounds the enumeration at C(maxBareIDColons, 2)
	// = ~2k probes worst case, so no maxGrantIDCandidates-style cap is
	// needed here — O(colons²) can never exceed it, unlike the grant
	// path's O(colons⁴) direct splits.
	if n := strings.Count(entitlementID, ":"); n > maxBareIDColons {
		return entitlementIdentity{}, fmt.Errorf("%w: entitlement id has %d colons; too complex to resolve safely by string", ErrAmbiguousExternalID, n)
	}
	var hits []entitlementIdentity
	for k := 0; k < len(entitlementID); k++ {
		if entitlementID[k] != ':' {
			continue
		}
		if err := ctx.Err(); err != nil {
			return entitlementIdentity{}, err
		}
		for l := k + 1; l < len(entitlementID); l++ {
			if entitlementID[l] != ':' {
				continue
			}
			cand := entitlementIdentity{
				resourceTypeID: entitlementID[:k],
				resourceID:     entitlementID[k+1 : l],
				stripped:       true,
				tail:           entitlementID[l+1:],
			}
			nonEmpty, err := e.grantPrimaryPrefixNonEmpty(encodeGrantPrimaryEntitlementPrefix(cand))
			if err != nil {
				return entitlementIdentity{}, err
			}
			if nonEmpty {
				hits = append(hits, cand)
			}
		}
	}
	switch len(hits) {
	case 0:
		return entitlementIdentity{}, pebble.ErrNotFound
	case 1:
		return hits[0], nil
	default:
		return entitlementIdentity{}, fmt.Errorf("%w: entitlement id %q matches grants under %d distinct identities",
			ErrAmbiguousExternalID, entitlementID, len(hits))
	}
}

func (e *Engine) grantPrimaryPrefixNonEmpty(prefix []byte) (bool, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBoundOf(prefix),
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	ok := iter.First()
	if err := iter.Error(); err != nil {
		return false, err
	}
	return ok, nil
}

// resolveGrantIdentityByExternalID resolves a public grant id string to the
// single grant row whose public id equals it, by enumerating candidate
// splits of the concat shape and probing each candidate identity:
//
//   - direct splits: boundaries for (rt, rid, tail, principal_rt,
//     principal_id) — the stripped-entitlement shape, needing no
//     entitlement record;
//   - entitlement-resolved splits: boundaries for (entitlement_id,
//     principal_rt, principal_id), with the entitlement id resolved through
//     the lazy map — this covers opaque entitlement ids.
//
// A probe hit is exact: the candidate's reconstructed public id equals the
// query by construction, and the hit is counted only when the row's stored
// public id also equals the query (a connector-custom stored id addresses
// the row instead of the concat). Exactly one hit wins; zero is
// pebble.ErrNotFound; several is ErrAmbiguousExternalID.
func (e *Engine) resolveGrantIdentityByExternalID(ctx context.Context, grantID string) (grantIdentity, error) {
	var colons []int
	for i := 0; i < len(grantID); i++ {
		if grantID[i] == ':' {
			colons = append(colons, i)
		}
	}
	if len(colons) < 2 {
		// No concat shape to split: connector-custom ids (SQLite keyed rows
		// by these, and provisioner revokes address grants with them) are
		// findable only by their STORED external id.
		return e.scanGrantIdentityByStoredExternalID(ctx, grantID)
	}
	if len(colons) > maxBareIDColons {
		return grantIdentity{}, fmt.Errorf("%w: grant id has %d colons; too complex to resolve safely by string", ErrAmbiguousExternalID, len(colons))
	}

	var candidates []grantIdentity
	addCandidate := func(id grantIdentity) error {
		if len(candidates) >= maxGrantIDCandidates {
			return fmt.Errorf("%w: grant id %q has too many candidate parses", ErrAmbiguousExternalID, grantID)
		}
		candidates = append(candidates, id)
		return nil
	}

	// Entitlement-resolved splits: (entID | prt | pid). The entitlement
	// lookup depends only on the first boundary, so it is hoisted out of
	// the inner loop — one map probe per candidate entitlement id, not one
	// per (i, j) pair.
	for ii := 0; ii < len(colons); ii++ {
		if err := ctx.Err(); err != nil {
			return grantIdentity{}, err
		}
		i := colons[ii]
		entMatches, err := e.entitlementIdentitiesForExternalID(ctx, grantID[:i])
		if err != nil {
			return grantIdentity{}, err
		}
		if len(entMatches) == 0 {
			continue
		}
		for jj := ii + 1; jj < len(colons); jj++ {
			j := colons[jj]
			for _, ent := range entMatches {
				if err := addCandidate(grantIdentity{
					entitlement:     ent,
					principalTypeID: grantID[i+1 : j],
					principalID:     grantID[j+1:],
				}); err != nil {
					return grantIdentity{}, err
				}
			}
		}
	}
	// Direct stripped splits: (rt | rid | tail | prt | pid). These need no
	// entitlement record, covering grants whose entitlement row is absent.
	// Bounded by maxBareIDColons⁴ in the worst case, which the colon cap
	// above keeps trivial; the ctx check makes even that interruptible.
	for kk := 0; kk < len(colons); kk++ {
		if err := ctx.Err(); err != nil {
			return grantIdentity{}, err
		}
		for ll := kk + 1; ll < len(colons); ll++ {
			for ii := ll + 1; ii < len(colons); ii++ {
				for jj := ii + 1; jj < len(colons); jj++ {
					k, l, i, j := colons[kk], colons[ll], colons[ii], colons[jj]
					if err := addCandidate(grantIdentity{
						entitlement: entitlementIdentity{
							resourceTypeID: grantID[:k],
							resourceID:     grantID[k+1 : l],
							stripped:       true,
							tail:           grantID[l+1 : i],
						},
						principalTypeID: grantID[i+1 : j],
						principalID:     grantID[j+1:],
					}); err != nil {
						return grantIdentity{}, err
					}
				}
			}
		}
	}

	seen := map[string]struct{}{}
	var hits []grantIdentity
	for _, cand := range candidates {
		key := encodeGrantIdentityKey(cand)
		if _, dup := seen[string(key)]; dup {
			continue
		}
		seen[string(key)] = struct{}{}
		val, closer, err := e.db.Get(key)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return grantIdentity{}, err
		}
		// Count the hit only when the row's PUBLIC id equals the query: a
		// row with a connector-custom stored external id is addressed by
		// that id, not by its concat reconstruction.
		ext, serr := scanGrantExternalIDRaw(val)
		closer.Close()
		if serr != nil {
			return grantIdentity{}, serr
		}
		if ext != "" && ext != grantID {
			continue
		}
		hits = append(hits, cand)
	}
	switch len(hits) {
	case 0:
		// Every concat split missed: the id may still be a connector-custom
		// STORED external id that merely contains colons.
		return e.scanGrantIdentityByStoredExternalID(ctx, grantID)
	case 1:
		return hits[0], nil
	default:
		return grantIdentity{}, fmt.Errorf("%w: grant id %q matches %d records", ErrAmbiguousExternalID, grantID, len(hits))
	}
}

// scanGrantIdentityByStoredExternalID finds the grant whose STORED
// external id equals grantID by scanning the grant primary keyspace with
// a shallow per-value field read. This is the resolution of last resort
// for connector-custom ids that carry no reconstructible concat shape —
// SQLite keyed grant rows by exactly this id, so string reads must find
// them for reader parity.
//
// COST: O(all grants) per call, hit or miss. Every NotFound outcome ends
// here (absence is unprovable without the scan), and a successful custom-id
// hit scans to EOF anyway — the exactly-one rule keeps looking for a second
// match (early exit only on ambiguity). Only the external_id field is
// decoded per row. This is acceptable ONLY because every caller is an
// interactive one-shot:
//
//   - reachable from the CLI/reader-API edge (provisioner --revoke-grant,
//     explorer detail views via the storecache, which memoizes per id);
//     the combinatorial prober above answers SDK-shaped ids without ever
//     getting here;
//   - sync and expansion never reach it: the syncer's only
//     delete-grants-by-id loop prefers the store's DeleteGrantByRefs
//     (grantByRefsDeleter in pkg/sync), which Pebble implements, so the
//     string path there runs only on SQLite where external_id is indexed.
//
// If a real workload ever resolves many DISTINCT custom ids (each one a
// full scan — quadratic in aggregate), the escape hatch is a skinny
// by-custom-external-id index written at put time only for rows whose
// stored id differs from their concat reconstruction; SDK-shaped ids need
// no index entry, so the added write weight would be near zero.
//
// Exactly-one rule: zero matches is pebble.ErrNotFound, several is
// ErrAmbiguousExternalID.
func (e *Engine) scanGrantIdentityByStoredExternalID(ctx context.Context, grantID string) (grantIdentity, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return grantIdentity{}, err
	}
	defer iter.Close()
	var hits []grantIdentity
	var scanned int64
	for iter.First(); iter.Valid(); iter.Next() {
		scanned++
		if scanned&0x3FFF == 0 {
			if err := ctx.Err(); err != nil {
				return grantIdentity{}, err
			}
		}
		ext, serr := scanGrantExternalIDRaw(iter.Value())
		if serr != nil {
			return grantIdentity{}, serr
		}
		if ext != grantID {
			continue
		}
		id, ok := decodeGrantIdentityKey(iter.Key())
		if !ok {
			continue
		}
		hits = append(hits, id)
		if len(hits) > 1 {
			return grantIdentity{}, fmt.Errorf("%w: grant id %q matches %d records", ErrAmbiguousExternalID, grantID, len(hits))
		}
	}
	if err := iter.Error(); err != nil {
		return grantIdentity{}, err
	}
	if len(hits) == 0 {
		return grantIdentity{}, pebble.ErrNotFound
	}
	return hits[0], nil
}
