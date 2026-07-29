package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

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

// C02/C04 include structurally addressable rows whose stored value is
// malformed. Grant deletion already promises key-derived cleanup in this
// case; adding source-scope ownership must not reintroduce an orphan index.
func TestVerificationMalformedGrantDeleteCleansSourceScopeIndex(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	grant := scGrant("member", "alice", false)
	require.NoError(t, a.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	rec, err := e.GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	id, err := grantIdentityFromRecord(rec)
	require.NoError(t, err)
	primaryKey := encodeGrantIdentityKey(id)
	require.Equal(t, 1, countKeys(t, e, GrantBySourceScopeLowerBound()))

	require.NoError(t, e.db.UnsafeForTesting().Set(primaryKey, []byte("\xff not a proto"), pebble.Sync))
	require.NoError(t, e.DeleteGrantByIdentityRefs(ctx, rec))

	_, closer, err := e.db.Get(primaryKey)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	if closer != nil {
		closer.Close()
	}
	require.Zero(t, countKeys(t, e, GrantBySourceScopeLowerBound()),
		"malformed-value delete left a source-scope index whose primary is gone")
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

// C13: overlay replacement is keyed by connector identity, not merely by the
// storage tuple. If an entitlement keeps its public ID while its resource
// reference changes, the replayed structural identity is obsolete.
func TestVerificationEntitlementOverlayReplacesPriorStructuralIdentity(t *testing.T) {
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
	require.Len(t, rows, 1, "overlay left both old and replacement structural identities")
	require.Equal(t, "new", rows[0].GetResource().GetResourceId())
	require.Equal(t, 1, countKeys(t, cur.PebbleEngine(), EntitlementBySourceScopeLowerBound()))
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

// C32: a completed manifest plus stamped primary but no source index is not a
// legitimate zero-row scope. Replay must detect the inconsistent source rather
// than silently publishing an empty result.
func TestVerificationReplayRejectsMissingSourceScopeIndex(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	grant := scGrant("member", "alice", false)
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, "scope-a"), grant))
	require.NoError(t, prev.PebbleEngine().PutSourceCacheEntry(ctx, string(sourcecache.RowKindGrants), "scope-a", "validator-a"))
	rec, err := prev.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	id, err := grantIdentityFromRecord(rec)
	require.NoError(t, err)
	indexKey := encodeGrantBySourceScopeIndexKey("scope-a", id)
	require.NoError(t, prev.PebbleEngine().db.UnsafeForTesting().Delete(indexKey, pebble.Sync))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), "scope-a")
	require.Error(t, err, "stamped primary without its source index must fail source-integrity preflight")
	_, getErr := cur.PebbleEngine().GetGrantRecord(ctx, grant.GetId())
	require.ErrorIs(t, getErr, pebble.ErrNotFound)
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
