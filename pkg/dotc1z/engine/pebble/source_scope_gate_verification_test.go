package pebble

// Verification for the sourceScopeMayExist gate — the rawdb flag that
// keeps unscoped syncs on the exact pre-scope write cost. The gate's
// contract: FALSE certifies no by_source_scope index entry exists, so
// the record ops skip every scope obligation that exists to maintain
// entries (prior-value scans, the entitlement read-before-write,
// delete-side cleanup). The one unsound state is false-with-entries.
//
// Pinned here: the in-process lifecycle (fresh-unarmed, unscoped
// writes stay unarmed, stamped writes arm — per record kind), the
// Open probe in both directions per kind, the fold surface (arm at
// mint plus maintenance of an actually-borrowed entry), the bulk
// import Finish re-probe (entries installed by SST ingest), the
// post-invalidation write shapes, and a behavioral proof that the
// unarmed path never consults the prior value. NOT mechanically
// covered: the generic IngestSSTs/ReplaceRangeWithSSTs surface, whose
// arming obligation is documented on the rawdb ingest family and
// discharged per caller (bulk import re-probes; synth layer and
// rebuild compactors provably emit no scope keys).

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func scopeGateEntitlement(id, scope string) *v3.EntitlementRecord {
	return v3.EntitlementRecord_builder{
		ExternalId: id,
		Resource: v3.ResourceRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "g1",
		}.Build(),
		SourceScopeKey: scope,
	}.Build()
}

func scopeGateResource(id, scope string) *v3.ResourceRecord {
	return v3.ResourceRecord_builder{
		ResourceTypeId: "user",
		ResourceId:     id,
		SourceScopeKey: scope,
	}.Build()
}

func countAllSourceScopeKeys(t *testing.T, e *Engine) int {
	t.Helper()
	return countKeys(t, e, GrantBySourceScopeLowerBound()) +
		countKeys(t, e, EntitlementBySourceScopeLowerBound()) +
		countKeys(t, e, ResourceBySourceScopeLowerBound())
}

// TestVerificationSourceScopeGateLifecycle pins the in-process
// transitions: unarmed on a fresh store, held unarmed by unscoped
// writes, armed by the first stamped record (self-healing inside
// stageSourceScopeChange — no engine coordination to forget), and
// disarmed by the ResetForNewSync wipe that excises the families.
func TestVerificationSourceScopeGateLifecycle(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	require.False(t, e.db.SourceScopeMayExist(), "fresh store must start unarmed")

	// Unscoped writes across kinds keep the gate unarmed and create no
	// scope index keys. Two batches per kind so the second one runs past
	// the fresh-first-batch skip and exercises the gate's own skip.
	for batch := 0; batch < 2; batch++ {
		require.NoError(t, e.PutResourceRecords(ctx, scopeGateResource(fmt.Sprintf("u-%d", batch), "")))
		require.NoError(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement(fmt.Sprintf("ent-%d", batch), "")))
		require.NoError(t, a.PutGrants(ctx, scGrant("member", fmt.Sprintf("p-%d", batch), false)))
	}
	require.False(t, e.db.SourceScopeMayExist(), "unscoped writes must not arm the gate")
	require.Zero(t, countAllSourceScopeKeys(t, e))

	// The first stamped record arms the gate and stages its entry.
	require.NoError(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-scoped", scopeA)))
	require.True(t, e.db.SourceScopeMayExist(), "a stamped record must arm the gate at staging")
	require.Equal(t, 1, countKeys(t, e, EntitlementBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(e))

	// A replacement sync wipes the families (ResetForNewSync) and must
	// disarm the gate so the new sync starts on the unscoped fast path.
	require.NoError(t, a.EndSync(ctx))
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, e.db.SourceScopeMayExist(), "ResetForNewSync must disarm the gate with the excised families")
	require.Zero(t, countAllSourceScopeKeys(t, e))
}

// TestVerificationSourceScopeGateArmsPerKind pins the self-healing arm
// for every record kind that owns a by_source_scope family: the first
// stamped record of THAT kind arms the gate and stages exactly its
// entry, with the biconditional intact.
func TestVerificationSourceScopeGateArmsPerKind(t *testing.T) {
	kindFamilies := map[sourcecache.RowKind]func() []byte{
		sourcecache.RowKindResources:    ResourceBySourceScopeLowerBound,
		sourcecache.RowKindEntitlements: EntitlementBySourceScopeLowerBound,
		sourcecache.RowKindGrants:       GrantBySourceScopeLowerBound,
	}
	for kind, family := range kindFamilies {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			a := newAdapter(t)
			_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			e := a.PebbleEngine()

			// Two unstamped writes of the kind first: the second runs past
			// the fresh-first-batch skip, so the gate is what keeps it off
			// the scope obligations.
			putFastPathProofRow(t, e, kind, "", "unscoped", 0)
			putFastPathProofRow(t, e, kind, "", "unscoped", 1)
			require.False(t, e.db.SourceScopeMayExist(), "unscoped %s writes must not arm the gate", kind)

			putFastPathProofRow(t, e, kind, scopeA, "scoped", 0)
			require.True(t, e.db.SourceScopeMayExist(), "a stamped %s record must arm the gate", kind)
			require.Equal(t, 1, countKeys(t, e, family()))
			require.NoError(t, auditSourceScopeBiconditional(e))
		})
	}
}

// TestVerificationSourceScopeGateProbeAtOpen pins the Open-time
// derivation: the gate is in-memory only, so a reopen must re-derive it
// from the index families — armed when entries of ANY kind exist,
// unarmed when none do.
func TestVerificationSourceScopeGateProbeAtOpen(t *testing.T) {
	kinds := []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	}
	for _, kind := range kinds {
		for _, tc := range []struct {
			name  string
			scope string
			want  bool
		}{
			{name: "stamped store reopens armed", scope: scopeA, want: true},
			{name: "unstamped store reopens unarmed", scope: "", want: false},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				dir := filepath.Join(t.TempDir(), "db")
				e, err := Open(ctx, dir)
				require.NoError(t, err)
				a := NewAdapter(e)
				_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				putFastPathProofRow(t, e, kind, tc.scope, "probe", 0)
				require.NoError(t, a.EndSync(ctx))
				require.NoError(t, e.Close())

				e2, err := Open(ctx, dir)
				require.NoError(t, err)
				defer func() { require.NoError(t, e2.Close()) }()
				require.Equal(t, tc.want, e2.db.SourceScopeMayExist())
			})
		}
	}
}

// TestVerificationSourceScopeGateUnarmedSkipsPriorValueScan is the
// behavioral proof that the unarmed fast path really skips prior-value
// work: a prior value whose scope field cannot be decoded is planted
// under an entitlement identity, and the unstamped overwrite SUCCEEDS
// while unarmed (the prior value is never fetched or scanned). Arming
// the gate and planting the same poison makes the identical overwrite
// FAIL — proving the armed path consults exactly what the unarmed path
// skips.
func TestVerificationSourceScopeGateUnarmedSkipsPriorValueScan(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Burn the fresh-first-batch skip so the gate is the only thing
	// standing between the put and the read-before-write.
	require.NoError(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-burn", "")))

	plantPoison := func(id string) {
		rec := scopeGateEntitlement(id, "")
		identity, err := entitlementIdentityFromRecord(rec)
		require.NoError(t, err)
		// 0xFF opens a field header whose varint never terminates —
		// ScanSourceScopeKeyRaw must reject it.
		require.NoError(t, e.db.UnsafeForTesting().Set(encodeEntitlementIdentityKey(identity), []byte{0xFF}, nil))
	}

	plantPoison("ent-poison")
	require.False(t, e.db.SourceScopeMayExist(), "premise: gate unarmed")
	require.NoError(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-poison", "")),
		"unarmed overwrite must not consult the prior value")

	// Arm the gate (stamped write on an unrelated identity), re-plant,
	// and prove the armed path does consult the prior value.
	require.NoError(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-scoped", scopeA)))
	require.True(t, e.db.SourceScopeMayExist(), "premise: gate armed")
	plantPoison("ent-poison")
	require.Error(t, e.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-poison", "")),
		"armed overwrite must scan the prior value and reject the poison")
}

// TestVerificationSourceScopeGatePostInvalidationWrites pins the one
// production state where stamped primaries exist WITHOUT index entries:
// a rebuild-compaction invalidation dropped the families and the store
// was reopened (probe → unarmed). Writes on that store must succeed and
// must not manufacture orphan entries — the unarmed gate's certificate
// ("nothing to clean") is exactly correct there — and a fresh stamped
// write must re-arm and restore the biconditional for its row.
func TestVerificationSourceScopeGatePostInvalidationWrites(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "db")
	e, err := Open(ctx, dir)
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutEntitlementRecords(ctx,
		scopeGateEntitlement("ent-1", scopeA),
		scopeGateEntitlement("ent-2", scopeA),
	))
	require.NoError(t, a.EndSync(ctx))

	// Rebuild-compaction shape: validators and scope families dropped,
	// stamps left in the primary values.
	require.NoError(t, e.InvalidateSourceCacheReplayState(ctx, true))
	require.Zero(t, countAllSourceScopeKeys(t, e))
	require.True(t, e.db.SourceScopeMayExist(), "in-session invalidation deliberately leaves the gate armed (conservative)")
	require.NoError(t, e.Close())

	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, e2.Close()) }()
	require.False(t, e2.db.SourceScopeMayExist(), "reopen must derive unarmed from the empty families")

	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))

	// Unstamped overwrite of a stale-stamped row: succeeds, replaces the
	// stamp, creates nothing, stays unarmed.
	require.NoError(t, e2.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-1", "")))
	require.False(t, e2.db.SourceScopeMayExist())
	require.Zero(t, countAllSourceScopeKeys(t, e2))

	// Stamped overwrite of the other stale-stamped row: re-arms, stages
	// its entry; no cleanup was owed for the vanished old entry.
	require.NoError(t, e2.PutEntitlementRecords(ctx, scopeGateEntitlement("ent-2", scopeB)))
	require.True(t, e2.db.SourceScopeMayExist())
	require.Equal(t, 1, countKeys(t, e2, EntitlementBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(e2))
}

// TestVerificationFoldBatchArmsSourceScopeGate pins the fold exemption:
// the fold compactor copies borrowed scope-index keys the typed ops
// never see, so staging a scope-index key through a fold batch must arm
// the gate — false-with-entries is the one unsound state. Merely minting
// the shared fold/rebuild batch must not opt a fresh rebuild destination
// into scope maintenance. The test then proves the arm is load-bearing
// end to end: a scope entry actually copied through the fold surface is
// cleaned up by a later typed overwrite, which only happens because the
// armed gate makes the overwrite scan the prior value.
func TestVerificationFoldBatchArmsSourceScopeGate(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Burn the fresh-first-batch grant skip so the later overwrite takes
	// its ordinary read-before-write path.
	putFastPathProofRow(t, e, sourcecache.RowKindGrants, "", "burn", 0)
	require.False(t, e.db.SourceScopeMayExist(), "premise: unarmed before the fold mint")

	// Borrow a stamped grant row + its scope index entry through the fold
	// surface, byte-exact the way the fold compactor copies them.
	stamped := v3.GrantRecord_builder{
		ExternalId: "fold-grant",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "fold-group",
			EntitlementId:  "fold-ent",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "fold-user",
		}.Build(),
		SourceScopeKey: scopeA,
	}.Build()
	id, err := grantIdentityFromRecord(stamped)
	require.NoError(t, err)
	val, err := marshalRecord(stamped)
	require.NoError(t, err)
	fb := e.NewFoldBatch()
	require.False(t, e.db.SourceScopeMayExist(), "minting the raw compaction batch must not arm the gate")
	require.NoError(t, fb.Set(encodeGrantIdentityKey(id), val))
	require.False(t, e.db.SourceScopeMayExist(), "a stamped primary without an index must not arm the index-presence gate")
	require.NoError(t, fb.Set(encodeGrantBySourceScopeIndexKey(scopeA, id), nil))
	require.True(t, e.db.SourceScopeMayExist(), "staging a raw scope-index entry must arm the gate")
	require.NoError(t, fb.Commit(pebble.NoSync))
	require.NoError(t, fb.Close())
	require.Equal(t, 1, countKeys(t, e, GrantBySourceScopeLowerBound()))

	// The armed gate must maintain the borrowed entry: a typed overwrite
	// under a different scope cleans scope-a and stages scope-b.
	restamped := v3.GrantRecord_builder{
		ExternalId:     stamped.GetExternalId(),
		Entitlement:    stamped.GetEntitlement(),
		Principal:      stamped.GetPrincipal(),
		SourceScopeKey: scopeB,
	}.Build()
	require.NoError(t, e.PutGrantRecords(ctx, restamped))
	require.Equal(t, 1, countKeys(t, e, GrantBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(e))
}

// TestVerificationBulkImportFinishReprobesSourceScopeGate pins the SST
// ingest exemption at its one live discharge point: bulk import's
// Finish installs keys the typed ops never see, so it must re-derive
// the gate from the actual family state. Entries present after the
// ingest (planted here exactly where a scoped import SST would put
// them) must re-arm the gate so later typed writes maintain them.
func TestVerificationBulkImportFinishReprobesSourceScopeGate(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	require.False(t, e.db.SourceScopeMayExist(), "premise: fresh store unarmed")

	// The stamped row + entry a scoped import would install. Planted raw
	// (UnsafeForTesting) because today's v2 translators never stamp —
	// the writer surface (grantIndexKeys) is already wired for it.
	stamped := v3.GrantRecord_builder{
		ExternalId: "imported-grant",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "group",
			ResourceId:     "imported-group",
			EntitlementId:  "imported-ent",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "imported-user",
		}.Build(),
		SourceScopeKey: scopeA,
	}.Build()
	id, err := grantIdentityFromRecord(stamped)
	require.NoError(t, err)
	val, err := marshalRecord(stamped)
	require.NoError(t, err)
	raw := e.db.UnsafeForTesting()
	require.NoError(t, raw.Set(encodeGrantIdentityKey(id), val, nil))
	require.NoError(t, raw.Set(encodeGrantBySourceScopeIndexKey(scopeA, id), nil, nil))

	bulk, err := e.StartBulkSyncImport(ctx, syncID, t.TempDir(), 1)
	require.NoError(t, err)
	defer bulk.Abort()
	require.NoError(t, bulk.AddResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, bulk.AddResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
	}.Build()))
	require.NoError(t, bulk.AddEntitlements(ctx, mkV2Entitlement("group:g1:member", "group", "g1")))
	shard, err := bulk.NewGrantShard()
	require.NoError(t, err)
	shard.Close()
	require.NoError(t, bulk.Finish(ctx))

	require.True(t, e.db.SourceScopeMayExist(), "Finish must re-derive the gate from the ingested state")

	// With the gate armed, an overwrite of the imported identity under a
	// new scope must clean the stale entry — the exact orphan the
	// un-probed Finish used to leave behind.
	restamped := v3.GrantRecord_builder{
		ExternalId:     stamped.GetExternalId(),
		Entitlement:    stamped.GetEntitlement(),
		Principal:      stamped.GetPrincipal(),
		SourceScopeKey: scopeB,
	}.Build()
	require.NoError(t, e.PutGrantRecords(ctx, restamped))
	require.Equal(t, 1, countKeys(t, e, GrantBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(e))
}
