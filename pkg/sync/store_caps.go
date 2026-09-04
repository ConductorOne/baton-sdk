package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// storeCaps is the set of optional store capabilities pkg/sync uses,
// resolved once when a store is attached (syncer.setStore) and read as
// plain fields afterwards. A nil field means "this engine does not
// implement it" — SQLite implements almost none of these; Pebble
// implements all of them.
//
// The point of resolving up front is that the store's shape is decided
// once, in one place a reader can enumerate, instead of being rediscovered
// by an `x.(Interface)` at each use site. Duck-typing at the use site
// hides how many capabilities the syncer actually depends on, and makes
// every new dependency a two-line addition nobody reviews as a dependency.
type storeCaps struct {
	// entitlementGraph is the c1z graph sidecar used to persist a
	// preserved entitlement graph for later incremental expansion.
	entitlementGraph EntitlementGraphStore
	// grantDigest reads the exact whole-file grant digest written at seal
	// time. A preserved graph is only reusable when it can be bound to
	// one, so the graph sidecar is useless without this.
	grantDigest c1zstore.GrantGenerationDigestReader
	// ingestFacts is the referential-inspection surface the ingestion
	// invariants query (ingest_invariants.go). Absent on engines without
	// it, which is why the referential invariants degrade rather than
	// fail there.
	ingestFacts dotc1z.IngestInvariantStore
	// ingestVerification writes and clears the ingestion-invariant
	// verification marker. It is a capability of the store's SyncMeta
	// sub-store, not of the store itself.
	ingestVerification c1zstore.IngestInvariantVerificationWriter
	// dbSize reports the store's uncompressed working-set size, which the
	// progress log folds into the periodic expansion line.
	dbSize connectorstore.DBSizeProvider
}

// resolveStoreCaps resolves every capability in storeCaps from a full
// store, including the one that lives on its SyncMeta sub-store.
//
// This and resolveReaderCaps are the only places in pkg/sync that may
// type-assert a store against an optional capability interface.
func resolveStoreCaps(store c1zstore.Store) storeCaps {
	if store == nil {
		return storeCaps{}
	}
	caps := resolveReaderCaps(store)
	if meta := store.SyncMeta(); meta != nil {
		caps.ingestVerification, _ = meta.(c1zstore.IngestInvariantVerificationWriter)
	}
	return caps
}

// resolveReaderCaps resolves the capabilities reachable on a bare reader,
// for the exported entry points that take one (GraphFromStore,
// runIngestInvariants). ingestVerification is always nil here: it hangs off
// SyncMeta, which a reader does not expose.
func resolveReaderCaps(store connectorstore.Reader) storeCaps {
	if store == nil {
		return storeCaps{}
	}
	var caps storeCaps
	caps.entitlementGraph, _ = store.(EntitlementGraphStore)
	caps.grantDigest, _ = store.(c1zstore.GrantGenerationDigestReader)
	caps.ingestFacts, _ = store.(dotc1z.IngestInvariantStore)
	caps.dbSize, _ = store.(connectorstore.DBSizeProvider)
	return caps
}
