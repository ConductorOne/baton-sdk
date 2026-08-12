package dotc1z

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	storage_v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Optional capability interfaces that callers discover with a type assertion
// rather than a declared parameter type.
//
// These are the failure mode that makes un-embedding *pebble.Engine risky.
// A required interface loses a method and the build breaks; an optional one
// loses a method and the assertion just stops matching. The caller then takes
// the fallback path — a slower scan, a skipped optimization, an absent stat —
// with no compile error and, for the pure-performance ones, no failing test
// either. Dropping the embed silently cost the store V3GrantReaderProvider,
// LatestFinishedSyncIDFetcher and Stats before these assertions existed.
//
// Anything asserted against a store anywhere in the tree belongs here, so the
// compiler is what notices next time instead of a production slow path.
var (
	_ connectorstore.DBSizeProvider               = (*pebbleStore)(nil)
	_ connectorstore.EntitlementGrantDigestReader = (*pebbleStore)(nil)
	_ connectorstore.LatestFinishedSyncIDFetcher  = (*pebbleStore)(nil)
	_ connectorstore.StreamingReader              = (*pebbleStore)(nil)
	_ connectorstore.V3GrantReaderProvider        = (*pebbleStore)(nil)
	_ IngestInvariantStore                        = (*pebbleStore)(nil)

	// Asserted on store.SyncMeta(), not on the store itself.
	_ c1zstore.IngestInvariantVerificationWriter = pebbleStoreSyncMeta{}

	// Asserted inline at the call site, so there is no named type to
	// reference; these mirror the assertion shapes verbatim.

	// Asserted in pkg/dotc1z/cross_engine_parity_test.go.
	_ interface {
		Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error)
	} = (*pebbleStore)(nil)

	// Asserted in pkg/dotc1z/pebble_store.go via sanitizeSyncRunMetadataReader.
	_ interface {
		ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
	} = (*pebbleStore)(nil)

	// pkg/sync/syncer.go: expander fast path. Losing this one is invisible
	// at runtime — the expander silently falls back to materializing full
	// grants instead of principal keys.
	_ interface {
		ListGrantPrincipalKeysForEntitlement(context.Context, *v2.Entitlement, string, uint32) ([]string, string, error)
	} = (*pebbleStore)(nil)

	// pkg/sync/syncer.go: gates the expander's topological-merge path.
	// Losing it silently reroutes every Pebble sync onto the source-batched
	// expander — no error, no failing test, just a whale-scale cost
	// regression. This assertion was missing when the un-embedding landed
	// and the regression shipped on the branch until review caught it.
	_ interface {
		GrantsForEntitlementPrincipalSorted() bool
	} = (*pebbleStore)(nil)

	// pkg/synccompactor/compactor_pebble.go: the fold compactor's base-sync
	// read on the shared destination store. Losing it fails the fold loudly
	// ("not a pebble engine"), but the assertion belongs here all the same.
	_ interface {
		LatestFinishedSyncRecord(ctx context.Context, typeOK func(storage_v3.SyncType) bool) (*storage_v3.SyncRunRecord, error)
	} = (*pebbleStore)(nil)
)
