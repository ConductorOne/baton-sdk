package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2pb "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

type sourceScopeMutationDriver struct {
	put       func(context.Context, string) error
	readScope func(context.Context) (string, error)
	delete    func(context.Context) error
	indexLo   []byte
	replay    func(context.Context, *Engine, *Engine, string) (SourceCacheReplayResult, error)
}

func newSourceScopeMutationDriver(t *testing.T, a *Adapter, kind sourcecache.RowKind) sourceScopeMutationDriver {
	t.Helper()
	e := a.PebbleEngine()
	switch kind {
	case sourcecache.RowKindResources:
		resource := v2pb.Resource_builder{
			Id: v2pb.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		}.Build()
		return sourceScopeMutationDriver{
			put: func(ctx context.Context, scope string) error {
				return a.PutResources(sourcecache.WithScope(ctx, scope), resource)
			},
			readScope: func(ctx context.Context) (string, error) {
				rec, err := e.GetResourceRecord(ctx, "user", "alice")
				if err != nil {
					return "", err
				}
				return rec.GetSourceScopeKey(), nil
			},
			delete: func(ctx context.Context) error {
				return e.DeleteResourceRecord(ctx, "user", "alice")
			},
			indexLo: ResourceBySourceScopeLowerBound(),
			replay: func(ctx context.Context, dst, src *Engine, scope string) (SourceCacheReplayResult, error) {
				return dst.ReplaySourceCacheResources(ctx, src, scope)
			},
		}
	case sourcecache.RowKindEntitlements:
		resource := v2pb.Resource_builder{
			Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build()
		entitlement := v2pb.Entitlement_builder{
			Id:       "group:g1:member",
			Resource: resource,
		}.Build()
		return sourceScopeMutationDriver{
			put: func(ctx context.Context, scope string) error {
				return a.PutEntitlements(sourcecache.WithScope(ctx, scope), entitlement)
			},
			readScope: func(ctx context.Context) (string, error) {
				rec, err := e.GetEntitlementRecord(ctx, entitlement.GetId())
				if err != nil {
					return "", err
				}
				return rec.GetSourceScopeKey(), nil
			},
			delete: func(ctx context.Context) error {
				return e.DeleteEntitlementRecord(ctx, entitlement.GetId())
			},
			indexLo: EntitlementBySourceScopeLowerBound(),
			replay: func(ctx context.Context, dst, src *Engine, scope string) (SourceCacheReplayResult, error) {
				return dst.ReplaySourceCacheEntitlements(ctx, src, scope)
			},
		}
	case sourcecache.RowKindGrants:
		grant := scGrant("member", "alice", false)
		return sourceScopeMutationDriver{
			put: func(ctx context.Context, scope string) error {
				return a.PutGrants(sourcecache.WithScope(ctx, scope), grant)
			},
			readScope: func(ctx context.Context) (string, error) {
				rec, err := e.GetGrantRecord(ctx, grant.GetId())
				if err != nil {
					return "", err
				}
				return rec.GetSourceScopeKey(), nil
			},
			delete: func(ctx context.Context) error {
				return e.DeleteGrantRecord(ctx, grant.GetId())
			},
			indexLo: GrantBySourceScopeLowerBound(),
			replay: func(ctx context.Context, dst, src *Engine, scope string) (SourceCacheReplayResult, error) {
				return dst.ReplaySourceCacheGrants(ctx, src, scope)
			},
		}
	default:
		t.Fatalf("unsupported row kind %q", kind)
		return sourceScopeMutationDriver{}
	}
}

func assertSourceScopeReplayRows(
	t *testing.T,
	driver sourceScopeMutationDriver,
	source *Engine,
	scope string,
	want int64,
) {
	t.Helper()
	ctx := t.Context()
	dst := newAdapter(t)
	_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res, err := driver.replay(ctx, dst.PebbleEngine(), source, scope)
	require.NoError(t, err)
	require.Equal(t, want, res.Rows)
	require.Zero(t, res.StaleSkipped)
}

// C01/C02 exercise the complete scoped ownership transition symmetry for all
// three primary kinds. The total-index assertion is independent of replay;
// per-scope replay additionally pins that the one surviving index is owned by
// the expected scope.
func TestVerificationSourceScopeMutationTransitions(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			a := newAdapter(t)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			e := a.PebbleEngine()
			driver := newSourceScopeMutationDriver(t, a, kind)

			// absent → A
			require.NoError(t, driver.put(ctx, "scope-a"))
			scope, err := driver.readScope(ctx)
			require.NoError(t, err)
			require.Equal(t, "scope-a", scope)
			require.Equal(t, 1, countKeys(t, e, driver.indexLo))
			assertSourceScopeReplayRows(t, driver, e, "scope-a", 1)

			// A → A stays singular.
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.Equal(t, 1, countKeys(t, e, driver.indexLo))

			// A → B moves ownership, rather than aliasing the row into both.
			require.NoError(t, driver.put(ctx, "scope-b"))
			scope, err = driver.readScope(ctx)
			require.NoError(t, err)
			require.Equal(t, "scope-b", scope)
			require.Equal(t, 1, countKeys(t, e, driver.indexLo))
			assertSourceScopeReplayRows(t, driver, e, "scope-a", 0)
			assertSourceScopeReplayRows(t, driver, e, "scope-b", 1)

			// B → unscoped removes the index.
			require.NoError(t, driver.put(ctx, ""))
			scope, err = driver.readScope(ctx)
			require.NoError(t, err)
			require.Empty(t, scope)
			require.Zero(t, countKeys(t, e, driver.indexLo))

			// unscoped → A → delete removes both primary and source index.
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.Equal(t, 1, countKeys(t, e, driver.indexLo))
			require.NoError(t, driver.delete(ctx))
			require.Zero(t, countKeys(t, e, driver.indexLo))
			_, err = driver.readScope(ctx)
			require.ErrorIs(t, err, pebble.ErrNotFound)
		})
	}
}

// C02/C04: if a corrupt value makes value-derived obligations unknowable, the
// key-derived identity still permits bounded-memory family scans that remove
// every matching index in the same batch as the primary.
func TestVerificationMalformedAllKindDeleteCleansSourceScopeIndex(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			a := newAdapter(t)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			e := a.PebbleEngine()
			driver := newSourceScopeMutationDriver(t, a, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))

			var family sourceScopeAuditFamily
			for _, candidate := range sourceScopeAuditFamilies() {
				if candidate.name == string(kind) {
					family = candidate
					break
				}
			}
			require.NotEmpty(t, family.name)
			primaries, err := e.db.NewIter(&pebble.IterOptions{
				LowerBound: family.primaryLo,
				UpperBound: family.primaryHi,
			})
			require.NoError(t, err)
			require.True(t, primaries.First())
			primaryKey := append([]byte(nil), primaries.Key()...)
			require.NoError(t, primaries.Close())

			var deleteMalformed func() error
			switch kind {
			case sourcecache.RowKindResources:
				parentKey := rawdb.EncodeResourceByParentIndexKey("team", "parent", "user", "alice")
				require.NoError(t, e.db.UnsafeForTesting().Set(parentKey, nil, pebble.Sync))
				deleteMalformed = func() error { return e.DeleteResourceRecord(ctx, "user", "alice") }
			case sourcecache.RowKindEntitlements:
				deleteMalformed = func() error {
					oldVal, closer, err := e.db.Get(primaryKey)
					if err != nil {
						return err
					}
					defer closer.Close()
					batch := e.db.NewRecordBatch()
					defer batch.Close()
					if err := batch.StageEntitlementDelete(primaryKey, oldVal); err != nil {
						return err
					}
					return batch.Commit(pebble.Sync)
				}
			case sourcecache.RowKindGrants:
				rec, err := e.GetGrantRecord(ctx, scGrant("member", "alice", false).GetId())
				require.NoError(t, err)
				deleteMalformed = func() error { return e.DeleteGrantByIdentityRefs(ctx, rec) }
			}
			require.Equal(t, 1, countKeys(t, e, family.indexLo))

			require.NoError(t, e.db.UnsafeForTesting().Set(primaryKey, []byte("\xff not a proto"), pebble.Sync))
			require.NoError(t, deleteMalformed())

			_, closer, err := e.db.Get(primaryKey)
			require.ErrorIs(t, err, pebble.ErrNotFound)
			if closer != nil {
				closer.Close()
			}
			require.Zero(t, countKeys(t, e, family.indexLo),
				"malformed-value delete left a source-scope index whose primary is gone")
			if kind == sourcecache.RowKindResources {
				require.Zero(t, countKeys(t, e, ResourceByParentLowerBound()),
					"malformed resource delete left a by-parent index whose primary is gone")
			}
		})
	}
}

func TestVerificationTruncatedResourceParentIndexDoesNotPanicOnDelete(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	driver := newSourceScopeMutationDriver(t, a, sourcecache.RowKindResources)
	require.NoError(t, driver.put(ctx, "scope-a"))

	primaries, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{versionV3, typeResource},
		UpperBound: upperBoundOf([]byte{versionV3, typeResource}),
	})
	require.NoError(t, err)
	require.True(t, primaries.First())
	primaryKey := append([]byte(nil), primaries.Key()...)
	require.NoError(t, primaries.Close())

	// A valid terminal parent_rt tuple with no following separator used to
	// produce parentIDOffset == len(key)+1 and panic during malformed-row repair.
	malformedParentKey := append(ResourceByParentLowerBound(), 0)
	malformedParentKey = codec.AppendTupleString(malformedParentKey, "team")
	require.NoError(t, e.db.UnsafeForTesting().Set(malformedParentKey, nil, pebble.Sync))
	require.NoError(t, e.db.UnsafeForTesting().Set(primaryKey, []byte("\xff not a proto"), pebble.Sync))

	require.NoError(t, e.DeleteResourceRecord(ctx, "user", "alice"))
	require.Equal(t, 1, countKeys(t, e, ResourceByParentLowerBound()),
		"unidentifiable malformed index should be skipped without panicking")
}

// C08/C14 compare replay against the ordinary typed materialization path,
// including representative pre-existing obligations for every row kind.
func TestVerificationReplayMatchesDirectTypedMaterialization(t *testing.T) {
	ctx := t.Context()
	source := newAdapter(t)
	_, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	parent := v2pb.ResourceId_builder{ResourceType: "team", Resource: "eng"}.Build()
	resource := v2pb.Resource_builder{
		Id:               v2pb.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		ParentResourceId: parent,
	}.Build()
	entResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	entitlement := v2pb.Entitlement_builder{
		Id:       "group:g1:member",
		Resource: entResource,
	}.Build()
	grant := scGrant("member", "alice", true)
	scoped := sourcecache.WithScope(ctx, "scope-a")
	require.NoError(t, source.PutResources(scoped, resource))
	require.NoError(t, source.PutEntitlements(scoped, entitlement))
	require.NoError(t, source.PutGrants(scoped, grant))

	replayed := newAdapter(t)
	_, err = replayed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = replayed.PebbleEngine().ReplaySourceCacheResources(ctx, source.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = replayed.PebbleEngine().ReplaySourceCacheEntitlements(ctx, source.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = replayed.PebbleEngine().ReplaySourceCacheGrants(ctx, source.PebbleEngine(), "scope-a")
	require.NoError(t, err)

	direct := newAdapter(t)
	_, err = direct.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	sourceResource, err := source.PebbleEngine().GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	sourceEntitlement, err := source.PebbleEngine().GetEntitlementRecord(ctx, entitlement.GetId())
	require.NoError(t, err)
	sourceGrant, err := source.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	require.NoError(t, direct.PebbleEngine().PutResourceRecords(ctx, sourceResource))
	require.NoError(t, direct.PebbleEngine().PutEntitlementRecords(ctx, sourceEntitlement))
	require.NoError(t, direct.PebbleEngine().PutGrantRecords(ctx, sourceGrant))

	replayedResource, err := replayed.PebbleEngine().GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	directResource, err := direct.PebbleEngine().GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	require.True(t, proto.Equal(directResource, replayedResource))

	replayedEntitlement, err := replayed.PebbleEngine().GetEntitlementRecord(ctx, entitlement.GetId())
	require.NoError(t, err)
	directEntitlement, err := direct.PebbleEngine().GetEntitlementRecord(ctx, entitlement.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(directEntitlement, replayedEntitlement))

	replayedGrant, err := replayed.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	directGrant, err := direct.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	require.True(t, proto.Equal(directGrant, replayedGrant))

	for _, prefix := range [][]byte{
		ResourceByParentLowerBound(),
		ResourceBySourceScopeLowerBound(),
		EntitlementBySourceScopeLowerBound(),
		GrantByPrincipalLowerBound(),
		GrantByNeedsExpansionLowerBound(),
		GrantBySourceScopeLowerBound(),
	} {
		require.Equal(t, countKeys(t, direct.PebbleEngine(), prefix), countKeys(t, replayed.PebbleEngine(), prefix),
			"replay and direct materialization disagree for index prefix %x", prefix)
	}
}

// C13: entitlement identity is structural. The same public external ID on two
// resources is two legal identities (and intentionally makes bare-ID lookup
// ambiguous), so replay+overlay must match direct materialization by preserving
// both rows.
func TestVerificationEntitlementOverlayPreservesDistinctStructuralIdentities(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	oldResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "old"}.Build(),
	}.Build()
	oldEntitlement := v2pb.Entitlement_builder{
		Id:       "stable-public-id",
		Resource: oldResource,
	}.Build()
	require.NoError(t, prev.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), oldEntitlement))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)

	newResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "new"}.Build(),
	}.Build()
	newEntitlement := v2pb.Entitlement_builder{
		Id:          "stable-public-id",
		Resource:    newResource,
		DisplayName: "replacement",
	}.Build()
	require.NoError(t, cur.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), newEntitlement))

	var rows []*v3.EntitlementRecord
	require.NoError(t, cur.PebbleEngine().IterateEntitlements(ctx, func(rec *v3.EntitlementRecord) bool {
		rows = append(rows, rec)
		return true
	}))
	require.Len(t, rows, 2)
	resourceIDs := []string{
		rows[0].GetResource().GetResourceId(),
		rows[1].GetResource().GetResourceId(),
	}
	require.ElementsMatch(t, []string{"old", "new"}, resourceIDs)
	require.Equal(t, 2, countKeys(t, cur.PebbleEngine(), EntitlementBySourceScopeLowerBound()))
}

// C13: a true structural-identity collision replaces the replayed value exactly
// as ordinary direct materialization does and leaves one source-scope index.
func TestVerificationEntitlementOverlayReplacesCollidingStructuralIdentity(t *testing.T) {
	ctx := t.Context()
	resource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	base := v2pb.Entitlement_builder{
		Id:          "group:g1:member",
		Resource:    resource,
		DisplayName: "base",
	}.Build()
	overlay := v2pb.Entitlement_builder{
		Id:          base.GetId(),
		Resource:    resource,
		DisplayName: "overlay",
		Description: "current value",
	}.Build()
	scoped := sourcecache.WithScope(ctx, "scope-a")

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, prev.PutEntitlements(scoped, base))

	replayed := newAdapter(t)
	_, err = replayed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = replayed.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.NoError(t, replayed.PutEntitlements(scoped, overlay))

	direct := newAdapter(t)
	_, err = direct.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, direct.PutEntitlements(scoped, base))
	require.NoError(t, direct.PutEntitlements(scoped, overlay))

	replayedRecord, err := replayed.PebbleEngine().GetEntitlementRecord(ctx, overlay.GetId())
	require.NoError(t, err)
	directRecord, err := direct.PebbleEngine().GetEntitlementRecord(ctx, overlay.GetId())
	require.NoError(t, err)
	replayedRecord.SetDiscoveredAt(nil)
	directRecord.SetDiscoveredAt(nil)
	require.True(t, proto.Equal(directRecord, replayedRecord))
	require.Equal(t, "overlay", replayedRecord.GetDisplayName())
	require.Equal(t, 1, countKeys(t, replayed.PebbleEngine(), EntitlementBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(replayed.PebbleEngine()))
}

// C18: replacing a completed sync must remove every source-cache-owned
// family, including manifests for scopes that the replacement does not emit.
func TestVerificationResetRemovesSourceCacheFamilies(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	entResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	entitlement := v2pb.Entitlement_builder{
		Id:       "group:g1:member",
		Resource: entResource,
	}.Build()
	require.NoError(t, a.PutResources(sourcecache.WithScope(ctx, "scope-a"), resource))
	require.NoError(t, a.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), entitlement))
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"), scGrant("member", "alice", false)))
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		require.NoError(t, a.PebbleEngine().PutSourceCacheEntry(ctx, string(kind), "scope-a", "validator-a"))
	}
	require.NoError(t, a.EndSync(ctx))

	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	for _, prefix := range [][]byte{
		ResourceBySourceScopeLowerBound(),
		EntitlementBySourceScopeLowerBound(),
		GrantBySourceScopeLowerBound(),
		SourceCacheEntryLowerBound(),
	} {
		require.Zero(t, countKeys(t, a.PebbleEngine(), prefix), "reset leaked family prefix %x", prefix)
	}
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		_, err = a.PebbleEngine().GetSourceCacheEntry(ctx, string(kind), "scope-a")
		require.ErrorIs(t, err, pebble.ErrNotFound)
	}
}

// C22/C32: every primary↔source-index corruption class must make source
// preflight fail before destination mutation. The corrupt source itself must
// remain byte-for-byte untouched by the rejected replay attempt.
func TestVerificationReplayRejectsCorruptSourceMatrix(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			for _, corruption := range []struct {
				name        string
				replayScope string
				apply       func(t *testing.T, e *Engine, primaryKey, indexKey []byte)
			}{
				{
					name:        "missing-index",
					replayScope: "scope-a",
					apply: func(t *testing.T, e *Engine, _, indexKey []byte) {
						require.NoError(t, e.db.UnsafeForTesting().Delete(indexKey, pebble.Sync))
					},
				},
				{
					name:        "orphan-index",
					replayScope: "scope-a",
					apply: func(t *testing.T, e *Engine, primaryKey, _ []byte) {
						require.NoError(t, e.db.UnsafeForTesting().Delete(primaryKey, pebble.Sync))
					},
				},
				{
					name:        "wrong-scope-index",
					replayScope: "scope-b",
					apply: func(t *testing.T, e *Engine, primaryKey, _ []byte) {
						wrongIndex, ok := rawdb.AppendBySourceScopeKeyFromPrimary(nil, primaryKey, "scope-b")
						require.True(t, ok)
						require.NoError(t, e.db.UnsafeForTesting().Set(wrongIndex, nil, pebble.Sync))
					},
				},
				{
					name:        "malformed-primary",
					replayScope: "scope-a",
					apply: func(t *testing.T, e *Engine, primaryKey, _ []byte) {
						require.NoError(t, e.db.UnsafeForTesting().Set(primaryKey, []byte("\xff"), pebble.Sync))
					},
				},
			} {
				t.Run(corruption.name, func(t *testing.T) {
					ctx := t.Context()
					prev := newAdapter(t)
					_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					driver := newSourceScopeMutationDriver(t, prev, kind)
					require.NoError(t, driver.put(ctx, "scope-a"))

					var family sourceScopeAuditFamily
					for _, candidate := range sourceScopeAuditFamilies() {
						if candidate.name == string(kind) {
							family = candidate
							break
						}
					}
					require.NotEmpty(t, family.name)
					primaries, err := prev.PebbleEngine().db.NewIter(&pebble.IterOptions{
						LowerBound: family.primaryLo, UpperBound: family.primaryHi,
					})
					require.NoError(t, err)
					require.True(t, primaries.First())
					primaryKey := append([]byte(nil), primaries.Key()...)
					require.NoError(t, primaries.Close())
					indexes, err := prev.PebbleEngine().db.NewIter(&pebble.IterOptions{
						LowerBound: family.indexLo, UpperBound: family.indexHi,
					})
					require.NoError(t, err)
					require.True(t, indexes.First())
					indexKey := append([]byte(nil), indexes.Key()...)
					require.NoError(t, indexes.Close())
					corruption.apply(t, prev.PebbleEngine(), primaryKey, indexKey)

					sourceBefore := dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil)
					cur := newAdapter(t)
					_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					curDriver := newSourceScopeMutationDriver(t, cur, kind)
					require.NoError(t, curDriver.put(ctx, corruption.replayScope),
						"destination premise must occupy the exact scope that a preflight-ordering bug would clear")
					destinationBefore := dumpKeyRangeTest(t, cur.PebbleEngine(), nil, nil)

					_, err = driver.replay(ctx, cur.PebbleEngine(), prev.PebbleEngine(), corruption.replayScope)
					require.ErrorContains(t, err, "preflight")
					scope, readErr := curDriver.readScope(ctx)
					require.NoError(t, readErr)
					require.Equal(t, corruption.replayScope, scope)
					require.Equal(t, destinationBefore, dumpKeyRangeTest(t, cur.PebbleEngine(), nil, nil),
						"source preflight rejection mutated the occupied destination")
					require.NoError(t, auditSourceScopeBiconditional(cur.PebbleEngine()))
					require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil),
						"source preflight rejection mutated source content or metadata")
				})
			}
		})
	}
}

// C03: primary and source-scope ownership are one batch obligation. A failed
// typed commit may expose neither half of a new/moved/deleted transition.
func TestVerificationSourceScopeMutationAtomicity(t *testing.T) {
	injected := errors.New("verification injected record commit failure")
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()

			fresh := newAdapter(t)
			_, err := fresh.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			freshDriver := newSourceScopeMutationDriver(t, fresh, kind)
			fresh.PebbleEngine().db.SetRecordCommitTestHook(func() error { return injected })
			err = freshDriver.put(ctx, "scope-a")
			require.ErrorIs(t, err, injected)
			fresh.PebbleEngine().db.SetRecordCommitTestHook(nil)
			require.Zero(t, countKeys(t, fresh.PebbleEngine(), freshDriver.indexLo))
			assertSourceScopeReplayRows(t, freshDriver, fresh.PebbleEngine(), "scope-a", 0)

			existing := newAdapter(t)
			_, err = existing.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			driver := newSourceScopeMutationDriver(t, existing, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))

			existing.PebbleEngine().db.SetRecordCommitTestHook(func() error { return injected })
			err = driver.put(ctx, "scope-b")
			require.ErrorIs(t, err, injected)
			existing.PebbleEngine().db.SetRecordCommitTestHook(nil)
			scope, err := driver.readScope(ctx)
			require.NoError(t, err)
			require.Equal(t, "scope-a", scope)
			require.Equal(t, 1, countKeys(t, existing.PebbleEngine(), driver.indexLo))
			assertSourceScopeReplayRows(t, driver, existing.PebbleEngine(), "scope-a", 1)
			assertSourceScopeReplayRows(t, driver, existing.PebbleEngine(), "scope-b", 0)

			existing.PebbleEngine().db.SetRecordCommitTestHook(func() error { return injected })
			err = driver.delete(ctx)
			require.ErrorIs(t, err, injected)
			existing.PebbleEngine().db.SetRecordCommitTestHook(nil)
			scope, err = driver.readScope(ctx)
			require.NoError(t, err)
			require.Equal(t, "scope-a", scope)
			require.Equal(t, 1, countKeys(t, existing.PebbleEngine(), driver.indexLo))
		})
	}
}

type sourceScopeAuditFamily struct {
	name      string
	recordTyp byte
	field     protowire.Number
	primaryLo []byte
	primaryHi []byte
	indexLo   []byte
	indexHi   []byte
}

func sourceScopeAuditFamilies() []sourceScopeAuditFamily {
	return []sourceScopeAuditFamily{
		{
			name: "resources", recordTyp: typeResource, field: 12,
			primaryLo: encodeResourcePrefix(), primaryHi: upperBoundOf(encodeResourcePrefix()),
			indexLo: ResourceBySourceScopeLowerBound(), indexHi: ResourceBySourceScopeUpperBound(),
		},
		{
			name: "entitlements", recordTyp: typeEntitlement, field: 11,
			primaryLo: encodeEntitlementPrefix(), primaryHi: upperBoundOf(encodeEntitlementPrefix()),
			indexLo: EntitlementBySourceScopeLowerBound(), indexHi: EntitlementBySourceScopeUpperBound(),
		},
		{
			name: "grants", recordTyp: typeGrant, field: 10,
			primaryLo: encodeGrantPrefix(), primaryHi: upperBoundOf(encodeGrantPrefix()),
			indexLo: GrantBySourceScopeLowerBound(), indexHi: GrantBySourceScopeUpperBound(),
		},
	}
}

// auditSourceScopeBiconditional is independent of replay: it walks both sides
// of the primary↔source-index obligation and derives each counterpart directly
// from the keyspace contract.
func auditSourceScopeBiconditional(e *Engine) error {
	for _, family := range sourceScopeAuditFamilies() {
		primaries, err := e.db.NewIter(&pebble.IterOptions{LowerBound: family.primaryLo, UpperBound: family.primaryHi})
		if err != nil {
			return err
		}
		for primaries.First(); primaries.Valid(); primaries.Next() {
			scope, err := rawdb.ScanSourceScopeKeyRaw(primaries.Value(), family.field)
			if err != nil {
				primaries.Close()
				return fmt.Errorf("%s primary %x has malformed scope field: %w", family.name, primaries.Key(), err)
			}
			if scope == "" {
				continue
			}
			indexKey, ok := rawdb.AppendBySourceScopeKeyFromPrimary(nil, primaries.Key(), scope)
			if !ok {
				primaries.Close()
				return fmt.Errorf("%s primary %x cannot derive source index", family.name, primaries.Key())
			}
			_, closer, err := e.db.Get(indexKey)
			if err != nil {
				primaries.Close()
				return fmt.Errorf("%s primary %x missing source index: %w", family.name, primaries.Key(), err)
			}
			closer.Close()
		}
		if err := primaries.Error(); err != nil {
			primaries.Close()
			return err
		}
		if err := primaries.Close(); err != nil {
			return err
		}

		indexes, err := e.db.NewIter(&pebble.IterOptions{LowerBound: family.indexLo, UpperBound: family.indexHi})
		if err != nil {
			return err
		}
		for indexes.First(); indexes.Valid(); indexes.Next() {
			key := indexes.Key()
			if len(key) < 5 || key[3] != 0 {
				indexes.Close()
				return fmt.Errorf("%s malformed source index key %x", family.name, key)
			}
			scope, next, err := codec.DecodeTupleStringTo(nil, key[4:], 0)
			if err != nil || 4+next >= len(key) {
				indexes.Close()
				return fmt.Errorf("%s malformed source index scope %x: %w", family.name, key, err)
			}
			identityTail := key[4+next+1:]
			primaryKey := make([]byte, 0, 3+len(identityTail))
			primaryKey = append(primaryKey, versionV3, family.recordTyp, 0)
			primaryKey = append(primaryKey, identityTail...)
			value, closer, err := e.db.Get(primaryKey)
			if err != nil {
				indexes.Close()
				return fmt.Errorf("%s source index %x has no primary: %w", family.name, key, err)
			}
			actualScope, scanErr := rawdb.ScanSourceScopeKeyRaw(value, family.field)
			closer.Close()
			if scanErr != nil {
				indexes.Close()
				return fmt.Errorf("%s source index %x resolves to malformed primary: %w", family.name, key, scanErr)
			}
			if actualScope != string(scope) {
				indexes.Close()
				return fmt.Errorf("%s source index scope %q resolves to primary stamped %q", family.name, scope, actualScope)
			}
		}
		if err := indexes.Error(); err != nil {
			indexes.Close()
			return err
		}
		if err := indexes.Close(); err != nil {
			return err
		}
	}
	return nil
}

type terminalManifestKey struct {
	kind  string
	scope string
}

func validateExactTimestamp(got, want *timestamppb.Timestamp) error {
	if !proto.Equal(got, want) {
		return fmt.Errorf("timestamp provenance mismatch: got %v, want %v", got, want)
	}
	return nil
}

func validateBatchHighWater(got, limit int) error {
	if got > limit {
		return fmt.Errorf("batch high-water %d exceeds limit %d", got, limit)
	}
	return nil
}

func validateAllKindStats(stats map[string]int64, expected int64) error {
	for _, kind := range []string{"resources", "entitlements", "grants"} {
		if stats[kind] != expected {
			return fmt.Errorf("%s stats = %d, want %d", kind, stats[kind], expected)
		}
	}
	return nil
}

func validateTerminalClaim(operationComplete, manifestFound bool) error {
	if manifestFound && !operationComplete {
		return errors.New("terminal manifest published before operation completed")
	}
	return nil
}

// auditTerminalManifestReconciliation independently checks O10: every stamped
// scope has one replayable manifest owner, while every replayable manifest
// resolves to its exact scoped row count (including zero).
func auditTerminalManifestReconciliation(e *Engine) error {
	manifestCounts := make(map[terminalManifestKey]int)
	manifests, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: SourceCacheEntryLowerBound(),
		UpperBound: SourceCacheEntryUpperBound(),
	})
	if err != nil {
		return err
	}
	for manifests.First(); manifests.Valid(); manifests.Next() {
		rec := &v3.SourceCacheEntryRecord{}
		if err := unmarshalRecord(manifests.Value(), rec); err != nil {
			_ = manifests.Close()
			return fmt.Errorf("manifest %x is malformed: %w", manifests.Key(), err)
		}
		if rec.GetInvalidated() || rec.GetCacheValidator() == "" {
			continue
		}
		if err := sourcecache.ValidateRowKind(sourcecache.RowKind(rec.GetRowKind())); err != nil {
			_ = manifests.Close()
			return fmt.Errorf("manifest %x has invalid row kind: %w", manifests.Key(), err)
		}
		if err := sourcecache.ValidateScopeKey(rec.GetScopeKey()); err != nil {
			_ = manifests.Close()
			return fmt.Errorf("manifest %x has invalid scope: %w", manifests.Key(), err)
		}
		expectedKey := encodeSourceCacheEntryKey(rec.GetRowKind(), rec.GetScopeKey())
		if !bytes.Equal(manifests.Key(), expectedKey) {
			_ = manifests.Close()
			return fmt.Errorf("manifest %x is stored outside its declared %q/%q cell",
				manifests.Key(), rec.GetRowKind(), rec.GetScopeKey())
		}
		key := terminalManifestKey{kind: rec.GetRowKind(), scope: rec.GetScopeKey()}
		manifestCounts[key]++
		if manifestCounts[key] != 1 {
			_ = manifests.Close()
			return fmt.Errorf("terminal manifest %q/%q has %d owners", key.kind, key.scope, manifestCounts[key])
		}
	}
	if err := manifests.Error(); err != nil {
		_ = manifests.Close()
		return err
	}
	if err := manifests.Close(); err != nil {
		return err
	}

	for _, family := range sourceScopeAuditFamilies() {
		primaries, err := e.db.NewIter(&pebble.IterOptions{LowerBound: family.primaryLo, UpperBound: family.primaryHi})
		if err != nil {
			return err
		}
		for primaries.First(); primaries.Valid(); primaries.Next() {
			scope, err := rawdb.ScanSourceScopeKeyRaw(primaries.Value(), family.field)
			if err != nil {
				_ = primaries.Close()
				return fmt.Errorf("%s primary %x has malformed scope field: %w", family.name, primaries.Key(), err)
			}
			if scope == "" {
				continue
			}
			key := terminalManifestKey{kind: family.name, scope: scope}
			if manifestCounts[key] != 1 {
				_ = primaries.Close()
				return fmt.Errorf("stamped scope %q/%q has %d terminal manifest owners", key.kind, key.scope, manifestCounts[key])
			}
		}
		if err := primaries.Error(); err != nil {
			_ = primaries.Close()
			return err
		}
		if err := primaries.Close(); err != nil {
			return err
		}
	}
	return nil
}

func terminalManifestRowCounts(e *Engine) (map[terminalManifestKey]int, error) {
	if err := auditTerminalManifestReconciliation(e); err != nil {
		return nil, err
	}
	counts := make(map[terminalManifestKey]int)
	manifests, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: SourceCacheEntryLowerBound(),
		UpperBound: SourceCacheEntryUpperBound(),
	})
	if err != nil {
		return nil, err
	}
	for manifests.First(); manifests.Valid(); manifests.Next() {
		rec := &v3.SourceCacheEntryRecord{}
		if err := unmarshalRecord(manifests.Value(), rec); err != nil {
			_ = manifests.Close()
			return nil, err
		}
		if !rec.GetInvalidated() && rec.GetCacheValidator() != "" {
			counts[terminalManifestKey{kind: rec.GetRowKind(), scope: rec.GetScopeKey()}] = 0
		}
	}
	if err := manifests.Error(); err != nil {
		_ = manifests.Close()
		return nil, err
	}
	if err := manifests.Close(); err != nil {
		return nil, err
	}
	for _, family := range sourceScopeAuditFamilies() {
		primaries, err := e.db.NewIter(&pebble.IterOptions{LowerBound: family.primaryLo, UpperBound: family.primaryHi})
		if err != nil {
			return nil, err
		}
		for primaries.First(); primaries.Valid(); primaries.Next() {
			scope, err := rawdb.ScanSourceScopeKeyRaw(primaries.Value(), family.field)
			if err != nil {
				_ = primaries.Close()
				return nil, err
			}
			if scope != "" {
				counts[terminalManifestKey{kind: family.name, scope: scope}]++
			}
		}
		if err := primaries.Error(); err != nil {
			_ = primaries.Close()
			return nil, err
		}
		if err := primaries.Close(); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

// C22 mutation adequacy for the source-scope obligation oracle. Every planted
// primary/index disagreement must make the independent auditor red.
func TestVerificationSourceScopeAuditorMutationAdequacy(t *testing.T) {
	newFixture := func(t *testing.T) (*Adapter, []byte, []byte) {
		t.Helper()
		ctx := t.Context()
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		grant := scGrant("member", "alice", false)
		require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
		rec, err := a.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
		require.NoError(t, err)
		id, err := grantIdentityFromRecord(rec)
		require.NoError(t, err)
		return a, encodeGrantIdentityKey(id), encodeGrantBySourceScopeIndexKey("scope-a", id)
	}

	t.Run("healthy", func(t *testing.T) {
		a, _, _ := newFixture(t)
		require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
	t.Run("missing-index", func(t *testing.T) {
		a, _, indexKey := newFixture(t)
		require.NoError(t, a.PebbleEngine().db.UnsafeForTesting().Delete(indexKey, pebble.Sync))
		require.Error(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
	t.Run("orphan-index", func(t *testing.T) {
		a, primaryKey, _ := newFixture(t)
		require.NoError(t, a.PebbleEngine().db.UnsafeForTesting().Delete(primaryKey, pebble.Sync))
		require.Error(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
	t.Run("wrong-scope-index", func(t *testing.T) {
		a, primaryKey, _ := newFixture(t)
		wrongIndex, ok := rawdb.AppendBySourceScopeKeyFromPrimary(nil, primaryKey, "scope-b")
		require.True(t, ok)
		require.NoError(t, a.PebbleEngine().db.UnsafeForTesting().Set(wrongIndex, nil, pebble.Sync))
		require.Error(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
	t.Run("malformed-primary", func(t *testing.T) {
		a, primaryKey, _ := newFixture(t)
		require.NoError(t, a.PebbleEngine().db.UnsafeForTesting().Set(primaryKey, []byte("\xff"), pebble.Sync))
		require.Error(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
}

// C24/I12: the manifest reconciler accepts exact zero-row ownership and goes
// red when a stamped scope loses its terminal manifest.
func TestVerificationTerminalManifestReconcilerMutationAdequacy(t *testing.T) {
	ctx := t.Context()
	newFixture := func(t *testing.T) *Adapter {
		t.Helper()
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		for _, kind := range []sourcecache.RowKind{
			sourcecache.RowKindResources,
			sourcecache.RowKindEntitlements,
			sourcecache.RowKindGrants,
		} {
			driver := newSourceScopeMutationDriver(t, a, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.NoError(t, a.PebbleEngine().PutSourceCacheEntry(ctx, string(kind), "scope-a", "validator-a"))
			require.NoError(t, a.PebbleEngine().PutSourceCacheEntry(ctx, string(kind), "zero-row", "validator-zero"))
		}
		return a
	}

	t.Run("healthy including zero row manifests", func(t *testing.T) {
		a := newFixture(t)
		require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
		counts, err := terminalManifestRowCounts(a.PebbleEngine())
		require.NoError(t, err)
		expected := make(map[terminalManifestKey]int)
		for _, kind := range []sourcecache.RowKind{
			sourcecache.RowKindResources,
			sourcecache.RowKindEntitlements,
			sourcecache.RowKindGrants,
		} {
			expected[terminalManifestKey{kind: string(kind), scope: "scope-a"}] = 1
			expected[terminalManifestKey{kind: string(kind), scope: "zero-row"}] = 0
		}
		require.Equal(t, expected, counts)
	})

	t.Run("stamped scope without manifest", func(t *testing.T) {
		a := newFixture(t)
		key := encodeSourceCacheEntryKey(string(sourcecache.RowKindGrants), "scope-a")
		require.NoError(t, a.PebbleEngine().db.UnsafeForTesting().Delete(key, pebble.Sync))
		require.ErrorContains(t, auditTerminalManifestReconciliation(a.PebbleEngine()), "0 terminal manifest owners")
	})
}

func TestVerificationTerminalManifestReconcilesOperationOutcomes(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, outcome := range []string{"replay", "overlay", "tombstone"} {
			t.Run(string(kind)+"/"+outcome, func(t *testing.T) {
				ctx := t.Context()
				prev := newAdapter(t)
				_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				sourceDriver := newSourceScopeMutationDriver(t, prev, kind)
				require.NoError(t, sourceDriver.put(ctx, "scope-a"))

				current := newAdapter(t)
				_, err = current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				currentDriver := newSourceScopeMutationDriver(t, current, kind)
				_, err = sourceDriver.replay(ctx, current.PebbleEngine(), prev.PebbleEngine(), "scope-a")
				require.NoError(t, err)
				expectedRows := 1
				switch outcome {
				case "overlay":
					require.NoError(t, currentDriver.put(ctx, "scope-a"))
				case "tombstone":
					require.NoError(t, currentDriver.delete(ctx))
					expectedRows = 0
				}
				require.NoError(t, current.PebbleEngine().PutSourceCacheEntry(
					ctx,
					string(kind),
					"scope-a",
					"validator-a",
				))
				counts, err := terminalManifestRowCounts(current.PebbleEngine())
				require.NoError(t, err)
				require.Equal(t, map[terminalManifestKey]int{
					{kind: string(kind), scope: "scope-a"}: expectedRows,
				}, counts)
				require.NoError(t, auditSourceScopeBiconditional(current.PebbleEngine()))
			})
		}
	}
}

func TestVerificationAuxiliaryOracleMutationAdequacy(t *testing.T) {
	t.Run("timestamp swap", func(t *testing.T) {
		require.Error(t, validateExactTimestamp(
			timestamppb.New(time.Unix(222, 0)),
			timestamppb.New(time.Unix(111, 0)),
		))
	})
	t.Run("unbounded batch", func(t *testing.T) {
		require.Error(t, validateBatchHighWater(replayBatchRows+1, replayBatchRows))
	})
	t.Run("stale counter", func(t *testing.T) {
		require.Error(t, validateAllKindStats(map[string]int64{
			"resources":    1,
			"entitlements": 0,
			"grants":       1,
		}, 1))
	})
	t.Run("premature manifest", func(t *testing.T) {
		require.Error(t, validateTerminalClaim(false, true))
	})
}

// C38: exact stats agree with primaries after occupied replacement, overlay,
// tombstones, retry, seal, and hard reopen.
func TestVerificationReplayStatsCoherence(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build()
	entResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()
	entitlement := v2pb.Entitlement_builder{Id: "group:g1:member", Resource: entResource}.Build()
	require.NoError(t, prev.PutResources(sourcecache.WithScope(ctx, "scope-a"), resource))
	require.NoError(t, prev.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), entitlement))
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, "scope-a"), scGrant("member", "alice", false)))

	curEngine, curDir := newTestEngine(t)
	cur := NewAdapter(curEngine)
	syncID, err := cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	obsoleteResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "user", Resource: "obsolete"}.Build(),
	}.Build()
	obsoleteEntResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "obsolete"}.Build(),
	}.Build()
	obsoleteEntitlement := v2pb.Entitlement_builder{
		Id:       "group:obsolete:member",
		Resource: obsoleteEntResource,
	}.Build()
	require.NoError(t, cur.PutResources(sourcecache.WithScope(ctx, "scope-a"), obsoleteResource))
	require.NoError(t, cur.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), obsoleteEntitlement))
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, "scope-a"), scGrant("obsolete", "obsolete", false)))
	_, err = cur.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)

	assertStats := func(e *Engine, label string, expected int64) {
		t.Helper()
		stats, err := e.Stats(ctx, connectorstore.SyncTypeAny, syncID)
		require.NoError(t, err, label)
		require.NoError(t, validateAllKindStats(stats, expected), label)
	}
	assertStats(cur.PebbleEngine(), "after occupied replacement", 1)

	overlayResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
	}.Build()
	overlayEntResource := v2pb.Resource_builder{
		Id: v2pb.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
	}.Build()
	overlayEntitlement := v2pb.Entitlement_builder{
		Id:       "group:g2:member",
		Resource: overlayEntResource,
	}.Build()
	require.NoError(t, cur.PutResources(sourcecache.WithScope(ctx, "scope-a"), overlayResource))
	require.NoError(t, cur.PutEntitlements(sourcecache.WithScope(ctx, "scope-a"), overlayEntitlement))
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, "scope-a"), scGrant("member", "bob", false)))
	assertStats(cur.PebbleEngine(), "after overlay", 2)

	deleted, err := cur.PebbleEngine().DeleteResourcesByIDsInScope(
		ctx,
		"scope-a",
		map[string]struct{}{"bob": {}},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.NoError(t, cur.PebbleEngine().DeleteEntitlementRecord(ctx, overlayEntitlement.GetId()))
	deleted, err = cur.PebbleEngine().DeleteGrantsByPrincipalsInScope(
		ctx,
		"scope-a",
		map[string]struct{}{"bob": {}},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	assertStats(cur.PebbleEngine(), "after tombstones", 1)

	_, err = cur.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	assertStats(cur.PebbleEngine(), "after retry replacement", 1)

	require.NoError(t, cur.EndSync(ctx))
	assertStats(cur.PebbleEngine(), "after seal", 1)
	require.NoError(t, curEngine.Close())
	reopened, err := Open(ctx, filepath.Join(curDir, "db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assertStats(reopened, "after hard reopen", 1)
}

// C10 all-kind first/final-batch failure symmetry: no row or index from the
// rejected batch may land, and the exact retry must converge.
func TestVerificationReplayCommitFailureRetryAllKinds(t *testing.T) {
	injected := errors.New("verification replay commit failure")
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			driver := newSourceScopeMutationDriver(t, prev, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))

			cur := newAdapter(t)
			_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			cur.PebbleEngine().test.sourceCacheReplayCommitHook = func(_ string, _ int, _ bool) error {
				return injected
			}
			_, err = driver.replay(ctx, cur.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.ErrorIs(t, err, injected)
			require.Zero(t, countKeys(t, cur.PebbleEngine(), driver.indexLo))

			cur.PebbleEngine().test.sourceCacheReplayCommitHook = nil
			res, err := driver.replay(ctx, cur.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)
			require.Equal(t, 1, countKeys(t, cur.PebbleEngine(), driver.indexLo))
			require.NoError(t, auditSourceScopeBiconditional(cur.PebbleEngine()))
		})
	}
}

// C10/C25: failure of the terminal manifest write must not publish a false
// validator claim over the already-materialized scope; retry writes exactly
// that claim without disturbing rows or indexes.
func TestVerificationManifestFailureDoesNotPublishClaim(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	grant := scGrant("member", "alice", false)
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), res.Rows)

	injected := errors.New("verification manifest write failure")
	cur.PebbleEngine().test.sourceCacheManifestWriteHook = func() error { return injected }
	err = cur.PebbleEngine().PutSourceCacheEntry(ctx, string(sourcecache.RowKindGrants), "scope-a", "validator-a")
	require.ErrorIs(t, err, injected)
	_, err = cur.PebbleEngine().GetSourceCacheEntry(ctx, string(sourcecache.RowKindGrants), "scope-a")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.NoError(t, validateTerminalClaim(false, false))
	require.Error(t, auditTerminalManifestReconciliation(cur.PebbleEngine()),
		"nonterminal stamped rows without a manifest must not reconcile as terminal")
	require.NoError(t, auditSourceScopeBiconditional(cur.PebbleEngine()))

	cur.PebbleEngine().test.sourceCacheManifestWriteHook = nil
	require.NoError(t, cur.PebbleEngine().PutSourceCacheEntry(ctx, string(sourcecache.RowKindGrants), "scope-a", "validator-a"))
	entry, err := cur.PebbleEngine().GetSourceCacheEntry(ctx, string(sourcecache.RowKindGrants), "scope-a")
	require.NoError(t, err)
	require.NoError(t, validateTerminalClaim(true, true))
	require.Equal(t, "validator-a", entry.GetCacheValidator())
	require.NoError(t, auditSourceScopeBiconditional(cur.PebbleEngine()))
}

// C02 conditional/newer variants must apply the same source-scope transition
// policy as ordinary typed puts, while rejected older writes preserve the
// incumbent stamp and index.
func TestVerificationIfNewerSourceScopeTransitions(t *testing.T) {
	ctx := t.Context()
	older := timestamppb.New(time.Unix(100, 0))
	newer := timestamppb.New(time.Unix(200, 0))

	t.Run("resources", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		build := func(scope string, discoveredAt *timestamppb.Timestamp) *v3.ResourceRecord {
			return v3.ResourceRecord_builder{
				ResourceTypeId: "user", ResourceId: "alice",
				SourceScopeKey: scope, DiscoveredAt: discoveredAt,
			}.Build()
		}
		require.NoError(t, a.PebbleEngine().PutResourceRecords(ctx, build("scope-a", older)))
		require.NoError(t, a.PebbleEngine().PutResourceRecordsIfNewer(ctx, build("scope-b", newer)))
		require.NoError(t, a.PebbleEngine().PutResourceRecordsIfNewer(ctx, build("scope-c", older)))
		rec, err := a.PebbleEngine().GetResourceRecord(ctx, "user", "alice")
		require.NoError(t, err)
		require.Equal(t, "scope-b", rec.GetSourceScopeKey())
		require.Equal(t, 1, countKeys(t, a.PebbleEngine(), ResourceBySourceScopeLowerBound()))
		require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})

	t.Run("entitlements", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		build := func(scope string, discoveredAt *timestamppb.Timestamp) *v3.EntitlementRecord {
			return v3.EntitlementRecord_builder{
				ExternalId: "group:g1:member",
				Resource: v3.ResourceRef_builder{
					ResourceTypeId: "group", ResourceId: "g1",
				}.Build(),
				SourceScopeKey: scope, DiscoveredAt: discoveredAt,
			}.Build()
		}
		require.NoError(t, a.PebbleEngine().PutEntitlementRecords(ctx, build("scope-a", older)))
		require.NoError(t, a.PebbleEngine().PutEntitlementRecordsIfNewer(ctx, build("scope-b", newer)))
		require.NoError(t, a.PebbleEngine().PutEntitlementRecordsIfNewer(ctx, build("scope-c", older)))
		rec, err := a.PebbleEngine().GetEntitlementRecord(ctx, "group:g1:member")
		require.NoError(t, err)
		require.Equal(t, "scope-b", rec.GetSourceScopeKey())
		require.Equal(t, 1, countKeys(t, a.PebbleEngine(), EntitlementBySourceScopeLowerBound()))
		require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})

	t.Run("grants", func(t *testing.T) {
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		build := func(scope string, discoveredAt *timestamppb.Timestamp) *v3.GrantRecord {
			return v3.GrantRecord_builder{
				ExternalId: "grant-1",
				Entitlement: v3.EntitlementRef_builder{
					ResourceTypeId: "group", ResourceId: "g1", EntitlementId: "member",
				}.Build(),
				Principal: v3.PrincipalRef_builder{
					ResourceTypeId: "user", ResourceId: "alice",
				}.Build(),
				SourceScopeKey: scope, DiscoveredAt: discoveredAt,
			}.Build()
		}
		require.NoError(t, a.PebbleEngine().PutGrantRecords(ctx, build("scope-a", older)))
		require.NoError(t, a.PebbleEngine().PutGrantRecordsIfNewer(ctx, build("scope-b", newer)))
		require.NoError(t, a.PebbleEngine().PutGrantRecordsIfNewer(ctx, build("scope-c", older)))
		var records []*v3.GrantRecord
		require.NoError(t, a.PebbleEngine().IterateGrants(ctx, func(rec *v3.GrantRecord) bool {
			records = append(records, rec)
			return true
		}))
		require.Len(t, records, 1)
		require.Equal(t, "scope-b", records[0].GetSourceScopeKey())
		require.Equal(t, 1, countKeys(t, a.PebbleEngine(), GrantBySourceScopeLowerBound()))
		require.NoError(t, auditSourceScopeBiconditional(a.PebbleEngine()))
	})
}

// C23/C26/C27/C33 all-kind symmetry: replay preserves the exact encoded row
// (including discovered_at), read/cancel failures are residue-free and
// retryable, and a replayed artifact with its terminal manifest is a valid
// second-hop source.
func TestVerificationAllKindReplayMetadataFailureAndForwardSymmetry(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			driver := newSourceScopeMutationDriver(t, prev, kind)
			require.NoError(t, driver.put(ctx, "scope-a"))
			require.NoError(t, prev.PebbleEngine().PutSourceCacheEntry(ctx, string(kind), "scope-a", "validator-a"))
			sourceBefore := dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil)

			var family sourceScopeAuditFamily
			for _, candidate := range sourceScopeAuditFamilies() {
				if candidate.name == string(kind) {
					family = candidate
					break
				}
			}
			require.NotEmpty(t, family.name)
			sourceIter, err := prev.PebbleEngine().db.NewIter(&pebble.IterOptions{
				LowerBound: family.primaryLo,
				UpperBound: family.primaryHi,
			})
			require.NoError(t, err)
			require.True(t, sourceIter.First())
			sourceValue := append([]byte(nil), sourceIter.Value()...)
			require.NoError(t, sourceIter.Close())

			readFailed := newAdapter(t)
			_, err = readFailed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			injected := errors.New("verification all-kind source read failure")
			readFailed.PebbleEngine().test.sourceCacheReplayReadHook = func(_ string, _ int) error { return injected }
			_, err = driver.replay(ctx, readFailed.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.ErrorIs(t, err, injected)
			require.Zero(t, countKeys(t, readFailed.PebbleEngine(), driver.indexLo))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil),
				"source read failure mutated source state")

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			_, err = driver.replay(cancelCtx, readFailed.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, countKeys(t, readFailed.PebbleEngine(), driver.indexLo))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil),
				"cancelled replay mutated source state")

			readFailed.PebbleEngine().test.sourceCacheReplayReadHook = nil
			res, err := driver.replay(ctx, readFailed.PebbleEngine(), prev.PebbleEngine(), "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)
			require.NoError(t, readFailed.PebbleEngine().PutSourceCacheEntry(ctx, string(kind), "scope-a", "validator-a"))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil),
				"successful replay mutated source state")

			replayedIter, err := readFailed.PebbleEngine().db.NewIter(&pebble.IterOptions{
				LowerBound: family.primaryLo,
				UpperBound: family.primaryHi,
			})
			require.NoError(t, err)
			require.True(t, replayedIter.First())
			require.True(t, bytes.Equal(sourceValue, replayedIter.Value()), "replay changed encoded %s metadata", kind)
			require.NoError(t, replayedIter.Close())

			next := newAdapter(t)
			_, err = next.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			res, err = driver.replay(ctx, next.PebbleEngine(), readFailed.PebbleEngine(), "scope-a")
			require.NoError(t, err)
			require.Equal(t, int64(1), res.Rows)
			require.Equal(t, 1, countKeys(t, next.PebbleEngine(), driver.indexLo))
			require.NoError(t, auditSourceScopeBiconditional(next.PebbleEngine()))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil),
				"second-hop replay mutated original source state")
		})
	}
}

// C26: explicit sentinels distinguish source-row, current overlay, and current
// manifest-write provenance for every row kind across replay, retry, and reopen.
func TestVerificationAllKindTimestampProvenance(t *testing.T) {
	sourceTimestamp := timestamppb.New(time.Unix(111, 0))
	overlayTimestamp := timestamppb.New(time.Unix(222, 0))

	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			put := func(e *Engine, ts *timestamppb.Timestamp) error {
				switch kind {
				case sourcecache.RowKindResources:
					return e.PutResourceRecords(ctx, v3.ResourceRecord_builder{
						ResourceTypeId: "user",
						ResourceId:     "alice",
						DiscoveredAt:   ts,
						SourceScopeKey: "scope-a",
					}.Build())
				case sourcecache.RowKindEntitlements:
					return e.PutEntitlementRecords(ctx, v3.EntitlementRecord_builder{
						ExternalId: "group:g1:member",
						Resource: v3.ResourceRef_builder{
							ResourceTypeId: "group",
							ResourceId:     "g1",
						}.Build(),
						DiscoveredAt:   ts,
						SourceScopeKey: "scope-a",
					}.Build())
				case sourcecache.RowKindGrants:
					return e.PutGrantRecords(ctx, v3.GrantRecord_builder{
						ExternalId: "group:g1:member:user:alice",
						Entitlement: v3.EntitlementRef_builder{
							ResourceTypeId: "group",
							ResourceId:     "g1",
							EntitlementId:  "group:g1:member",
						}.Build(),
						Principal: v3.PrincipalRef_builder{
							ResourceTypeId: "user",
							ResourceId:     "alice",
						}.Build(),
						DiscoveredAt:   ts,
						SourceScopeKey: "scope-a",
					}.Build())
				default:
					return fmt.Errorf("unsupported row kind %q", kind)
				}
			}
			require.NoError(t, put(prev.PebbleEngine(), sourceTimestamp))

			replay := func(dst *Engine) (SourceCacheReplayResult, error) {
				switch kind {
				case sourcecache.RowKindResources:
					return dst.ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
				case sourcecache.RowKindEntitlements:
					return dst.ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
				case sourcecache.RowKindGrants:
					return dst.ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
				default:
					return SourceCacheReplayResult{}, fmt.Errorf("unsupported row kind %q", kind)
				}
			}
			readTimestamp := func(e *Engine) (*timestamppb.Timestamp, error) {
				switch kind {
				case sourcecache.RowKindResources:
					rec, err := e.GetResourceRecord(ctx, "user", "alice")
					if err != nil {
						return nil, err
					}
					return rec.GetDiscoveredAt(), nil
				case sourcecache.RowKindEntitlements:
					rec, err := e.GetEntitlementRecord(ctx, "group:g1:member")
					if err != nil {
						return nil, err
					}
					return rec.GetDiscoveredAt(), nil
				case sourcecache.RowKindGrants:
					rec, err := e.GetGrantRecord(ctx, "group:g1:member:user:alice")
					if err != nil {
						return nil, err
					}
					return rec.GetDiscoveredAt(), nil
				default:
					return nil, fmt.Errorf("unsupported row kind %q", kind)
				}
			}

			currentEngine, currentDir := newTestEngine(t)
			current := NewAdapter(currentEngine)
			syncID, err := current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			_, err = replay(currentEngine)
			require.NoError(t, err)
			got, err := readTimestamp(currentEngine)
			require.NoError(t, err)
			require.NoError(t, validateExactTimestamp(got, sourceTimestamp))

			require.NoError(t, put(currentEngine, overlayTimestamp))
			got, err = readTimestamp(currentEngine)
			require.NoError(t, err)
			require.NoError(t, validateExactTimestamp(got, overlayTimestamp),
				"overlay must use normal direct-write timestamp provenance")

			_, err = replay(currentEngine)
			require.NoError(t, err)
			got, err = readTimestamp(currentEngine)
			require.NoError(t, err)
			require.NoError(t, validateExactTimestamp(got, sourceTimestamp),
				"retry replacement must restore source-row provenance")

			writtenLowerBound := time.Now()
			require.NoError(t, currentEngine.PutSourceCacheEntry(ctx, string(kind), "scope-a", "validator-a"))
			writtenUpperBound := time.Now()
			entry, err := currentEngine.GetSourceCacheEntry(ctx, string(kind), "scope-a")
			require.NoError(t, err)
			require.False(t, proto.Equal(sourceTimestamp, entry.GetDiscoveredAt()))
			require.False(t, proto.Equal(overlayTimestamp, entry.GetDiscoveredAt()))
			require.False(t, entry.GetDiscoveredAt().AsTime().Before(writtenLowerBound))
			require.False(t, entry.GetDiscoveredAt().AsTime().After(writtenUpperBound))

			require.NoError(t, currentEngine.Close())
			reopened, err := Open(ctx, filepath.Join(currentDir, "db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
			got, err = readTimestamp(reopened)
			require.NoError(t, err)
			require.NoError(t, validateExactTimestamp(got, sourceTimestamp))
			entry, err = reopened.GetSourceCacheEntry(ctx, string(kind), "scope-a")
			require.NoError(t, err)
			require.False(t, entry.GetDiscoveredAt().AsTime().Before(writtenLowerBound))
			require.False(t, entry.GetDiscoveredAt().AsTime().After(writtenUpperBound))
		})
	}
}

// C11: replay, overlay, and tombstone outcomes for every row kind are each
// compared byte-for-byte across an immediate hard reopen.
func TestVerificationAllKindOutcomeReopenDurability(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		for _, outcome := range []string{"replay", "overlay", "tombstone"} {
			t.Run(string(kind)+"/"+outcome, func(t *testing.T) {
				ctx := t.Context()
				prev := newAdapter(t)
				_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				sourceDriver := newSourceScopeMutationDriver(t, prev, kind)
				require.NoError(t, sourceDriver.put(ctx, "scope-a"))

				currentEngine, currentDir := newTestEngine(t)
				current := NewAdapter(currentEngine)
				syncID, err := current.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				currentDriver := newSourceScopeMutationDriver(t, current, kind)
				_, err = sourceDriver.replay(ctx, currentEngine, prev.PebbleEngine(), "scope-a")
				require.NoError(t, err)
				switch outcome {
				case "overlay":
					require.NoError(t, currentDriver.put(ctx, "scope-a"))
				case "tombstone":
					require.NoError(t, currentDriver.delete(ctx))
				}
				before := dumpKeyRangeTest(t, currentEngine, nil, nil)
				require.NoError(t, currentEngine.Close())

				reopened, err := Open(ctx, filepath.Join(currentDir, "db"))
				require.NoError(t, err)
				t.Cleanup(func() { _ = reopened.Close() })
				require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
				require.Equal(t, before, dumpKeyRangeTest(t, reopened, nil, nil))
				require.NoError(t, auditSourceScopeBiconditional(reopened))
			})
		}
	}
}
