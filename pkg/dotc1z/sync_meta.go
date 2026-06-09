package dotc1z

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

// The sync-metadata contract lives in pkg/dotc1z/c1zstore so storage
// engines can implement it without importing this package. These aliases
// preserve the historical dotc1z names.

// SyncMeta is the sync-run-metadata sub-store of C1ZStore. See
// c1zstore.SyncMeta for the full contract.
type SyncMeta = c1zstore.SyncMeta

// SyncRun is the exported shape of a sync run. See c1zstore.SyncRun.
type SyncRun = c1zstore.SyncRun
