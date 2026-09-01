package oracle

import (
	"context"
	"fmt"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepebble "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// Source-cache artifact observations (Phase 6b oracle OR3): exact manifest
// membership, per-scope stamp counts, and the compat record, read straight
// from a sealed Pebble artifact. Enumeration — not point reads — is the
// point: "unexpected scopes have no entries and no stamps" is only
// observable over the whole keyspace.

// SourceCacheManifestEntry is one scope's manifest entry as stored.
type SourceCacheManifestEntry struct {
	Validator   string
	Invalidated bool
}

// SourceCacheCompat is the stored replay-compatibility record.
type SourceCacheCompat struct {
	ConnectorCacheGeneration     string
	ConnectorConfigFingerprint   string
	SDKMaterializationGeneration string
	SyncSelectionFingerprint     string
}

// SourceCacheSnapshot is the complete source-cache projection of one
// sealed artifact.
type SourceCacheSnapshot struct {
	// Entries holds every manifest entry, keyed by KindScope.
	Entries map[string]SourceCacheManifestEntry
	// StampCounts holds the number of rows stamped per (kind, scope),
	// keyed by KindScope. Unstamped rows are not counted anywhere.
	StampCounts map[string]int
	// Compat is nil when the artifact carries no compat record.
	Compat *SourceCacheCompat
}

// KindScope is the snapshot map key for (rowKind, scopeKey).
func KindScope(kind sourcecache.RowKind, scopeKey string) string {
	return string(kind) + "\x00" + scopeKey
}

// ReadSourceCacheSnapshot reads the source-cache surfaces of a sealed
// Pebble store (a dotc1z store opened over the artifact).
func ReadSourceCacheSnapshot(ctx context.Context, store c1zstore.Store) (SourceCacheSnapshot, error) {
	out := SourceCacheSnapshot{
		Entries:     map[string]SourceCacheManifestEntry{},
		StampCounts: map[string]int{},
	}
	engine, ok := enginepebble.AsEngine(store)
	if !ok {
		return out, fmt.Errorf("chaos oracle: store is not a pebble engine")
	}

	if err := engine.IterateSourceCacheEntries(ctx, func(rec *v3.SourceCacheEntryRecord) bool {
		out.Entries[rec.GetRowKind()+"\x00"+rec.GetScopeKey()] = SourceCacheManifestEntry{
			Validator:   rec.GetCacheValidator(),
			Invalidated: rec.GetInvalidated(),
		}
		return true
	}); err != nil {
		return out, fmt.Errorf("chaos oracle: iterate source cache entries: %w", err)
	}

	stamp := func(kind sourcecache.RowKind, scopeKey string) {
		if scopeKey == "" {
			return
		}
		out.StampCounts[KindScope(kind, scopeKey)]++
	}
	if err := engine.IterateResources(ctx, func(rec *v3.ResourceRecord) bool {
		stamp(sourcecache.RowKindResources, rec.GetSourceScopeKey())
		return true
	}); err != nil {
		return out, fmt.Errorf("chaos oracle: iterate resource stamps: %w", err)
	}
	if err := engine.IterateEntitlements(ctx, func(rec *v3.EntitlementRecord) bool {
		stamp(sourcecache.RowKindEntitlements, rec.GetSourceScopeKey())
		return true
	}); err != nil {
		return out, fmt.Errorf("chaos oracle: iterate entitlement stamps: %w", err)
	}
	if err := engine.IterateGrants(ctx, func(rec *v3.GrantRecord) bool {
		stamp(sourcecache.RowKindGrants, rec.GetSourceScopeKey())
		return true
	}); err != nil {
		return out, fmt.Errorf("chaos oracle: iterate grant stamps: %w", err)
	}

	compatStore, ok := store.(dotc1z.SourceCacheStore)
	if !ok {
		return out, fmt.Errorf("chaos oracle: store exposes no source-cache compat surface")
	}
	compat, found, err := compatStore.GetSourceCacheCompat(ctx)
	if err != nil {
		return out, fmt.Errorf("chaos oracle: read compat record: %w", err)
	}
	if found {
		out.Compat = &SourceCacheCompat{
			ConnectorCacheGeneration:     compat.ConnectorCacheGeneration,
			ConnectorConfigFingerprint:   compat.ConnectorConfigFingerprint,
			SDKMaterializationGeneration: compat.SDKMaterializationGeneration,
			SyncSelectionFingerprint:     compat.SyncSelectionFingerprint,
		}
	}
	return out, nil
}
