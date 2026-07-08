package c1zsanitize

import (
	"context"
	"io"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Fixture identifiers. The cross-engine matrix sanitizes the same logical
// graph through every engine combination under one secret, so the
// transformed ids are identical across arms and the outputs are directly
// comparable.
const (
	pfRTUser  = "user"  // declared resource type -> token preserved
	pfRTRole  = "role"  // declared resource type
	pfRTGroup = "group" // NOT declared -> token HMAC'd; used to exercise multiset

	pfEntAdmin  = "ent-admin"
	pfEntReader = "ent-reader"

	pfGrantMultiExpand = "grant-multi-expand" // multi-Ent + multi-RT (with a dup RT)
	pfGrantDivergence  = "grant-divergence"   // GrantExpandable AND populated Sources
	pfGrantWhitespace  = "grant-whitespace"   // only blank entitlement ids -> not expandable
	pfGrantSourced     = "grant-sourced"      // multi-edge Sources, no expansion
	pfGrantPlain       = "grant-plain"        // neither

	pfAssetID = "icon-asset-1"
)

// pfSourceIconBytes is the original asset payload. The sanitizer must
// REPLACE it with a content-type placeholder, never copy these bytes
// through (that would be an identity leak).
var pfSourceIconBytes = []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}

// buildParityFixture writes the same logical graph into store through the
// engine-agnostic connectorstore.Writer surface, so the GrantExpandable
// annotations and Sources land in each engine's real side-state exactly as
// production files store them.
func buildParityFixture(t *testing.T, ctx context.Context, store c1zstore.Store) {
	t.Helper()

	_, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: pfRTUser, DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: pfRTRole, DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))

	// A resolvable asset: PutAsset writes the bytes; a UserTrait icon ref on
	// a resource carries the id so the sanitizer's resource walk registers it
	// and copyAssets fetches it (a dangling ref would be skipped and break the
	// count assertion).
	require.NoError(t, store.PutAsset(ctx, v2.AssetRef_builder{Id: pfAssetID}.Build(), "image/png", pfSourceIconBytes))

	parent := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: pfRTRole, Resource: "admins"}.Build(),
		DisplayName: "Admins",
	}.Build()
	// children parent_resource_id points at parent -> structural-linkage check.
	children := make([]*v2.Resource, 0, 5)
	for i, id := range []string{"alice", "bob", "carol", "dave", "erin"} {
		display := "User " + id
		if i == 0 {
			display = "Alice"
		}
		var annos []*anypb.Any
		if i == 0 {
			annos = []*anypb.Any{anyTB(t, v2.UserTrait_builder{
				Login: "alice@acme.com",
				Icon:  v2.AssetRef_builder{Id: pfAssetID}.Build(),
			}.Build())}
		}
		children = append(children, v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: pfRTUser, Resource: id}.Build(),
			ParentResourceId: v2.ResourceId_builder{ResourceType: pfRTRole, Resource: "admins"}.Build(),
			DisplayName:      display,
			Annotations:      annos,
		}.Build())
	}
	require.NoError(t, store.PutResources(ctx, append([]*v2.Resource{parent}, children...)...))

	entAdmin := v2.Entitlement_builder{Id: pfEntAdmin, Resource: parent, DisplayName: "admin", Slug: "admin"}.Build()
	entReader := v2.Entitlement_builder{Id: pfEntReader, Resource: parent, DisplayName: "reader", Slug: "reader"}.Build()
	require.NoError(t, store.PutEntitlements(ctx, entAdmin, entReader))

	sources := v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
		pfEntAdmin:  v2.GrantSources_GrantSource_builder{IsDirect: true}.Build(),
		pfEntReader: v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
	}}.Build()

	grants := []*v2.Grant{
		// multi-EntitlementId + multi-ResourceTypeId, with pfRTGroup duplicated
		// so the resource_type_ids multiset (multiplicity 2) is exercised.
		v2.Grant_builder{
			Id: pfGrantMultiExpand, Entitlement: entAdmin, Principal: children[0],
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{pfEntAdmin, pfEntReader},
				Shallow:         true,
				ResourceTypeIds: []string{pfRTUser, pfRTGroup, pfRTGroup},
			}.Build()),
		}.Build(),
		// Divergence trigger: carries a GrantExpandable AND already-populated
		// Sources (expanded-but-still-annotated). Proves the first-write
		// needs_expansion projection is identical across engines for this shape.
		v2.Grant_builder{
			Id: pfGrantDivergence, Entitlement: entAdmin, Principal: children[1],
			Sources: sources,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{pfEntReader},
				Shallow:        false,
			}.Build()),
		}.Build(),
		// Blank entitlement ids only -> annotation stripped, needs_expansion=false
		// on both engines.
		v2.Grant_builder{
			Id: pfGrantWhitespace, Entitlement: entReader, Principal: children[2],
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds: []string{"  ", ""},
			}.Build()),
		}.Build(),
		// Multi-edge Sources, no expansion.
		v2.Grant_builder{Id: pfGrantSourced, Entitlement: entReader, Principal: children[3], Sources: sources}.Build(),
		// Neither.
		v2.Grant_builder{Id: pfGrantPlain, Entitlement: entAdmin, Principal: children[4]}.Build(),
	}
	require.NoError(t, store.PutGrants(ctx, grants...))
	require.NoError(t, store.EndSync(ctx))
}

// newEngineStore opens a fresh writable store for the given engine.
func newEngineStore(t *testing.T, ctx context.Context, path string, eng c1zstore.Engine) c1zstore.Store {
	t.Helper()
	s, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(eng))
	require.NoError(t, err)
	return s
}

// openEngineStoreRO reopens a store read-only with its latest finished sync
// selected, so the neutral reader + Grants() surfaces resolve content.
func openEngineStoreRO(t *testing.T, ctx context.Context, path string) c1zstore.Store {
	t.Helper()
	s, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	latest, err := s.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, latest, "sanitized output must carry a finished sync")
	require.NoError(t, s.SetCurrentSync(ctx, latest.ID))
	return s
}

type expandBlob struct {
	ents    []string
	shallow bool
	rts     []string // multiset: sorted, multiplicity preserved
}

// pendingExpansionBlobs returns the full GrantExpandable blob for every
// expandable grant, keyed by the grant external id, read off the
// needs_expansion index via the neutral Grants().PendingExpansion surface.
func pendingExpansionBlobs(t *testing.T, ctx context.Context, store c1zstore.Store) map[string]expandBlob {
	t.Helper()
	out := map[string]expandBlob{}
	for pe, err := range store.Grants().PendingExpansion(ctx) {
		require.NoError(t, err)
		require.NotNil(t, pe.Annotation, "PendingExpansion row must carry its annotation")
		ents := append([]string(nil), pe.Annotation.GetEntitlementIds()...)
		for i, ent := range ents {
			ents[i] = parityEntitlementID(ent)
		}
		sort.Strings(ents)
		rts := append([]string(nil), pe.Annotation.GetResourceTypeIds()...)
		sort.Strings(rts) // sorted but NOT deduped -> multiset
		key := parityEntitlementID(pe.TargetEntitlementID) + "|" + pe.PrincipalResourceTypeID + "/" + pe.PrincipalResourceID
		out[key] = expandBlob{ents: ents, shallow: pe.Annotation.GetShallow(), rts: rts}
	}
	return out
}

// grantSourcesCanonical returns, per grant id, the sorted expansion-source
// edge set ("<sourceEntitlementID>=<isDirect>"), read through the neutral
// ListGrants surface. nil and empty Sources both normalize to an empty slice.
func grantSourcesCanonical(t *testing.T, ctx context.Context, store c1zstore.Store) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			srcMap := g.GetSources().GetSources()
			edges := make([]string, 0, len(srcMap))
			for k, v := range srcMap {
				edges = append(edges, k+"="+boolStr(v.GetIsDirect()))
			}
			sort.Strings(edges)
			out[parityGrantRef(g)] = edges
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	return out
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func grantCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	n := 0
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		n += len(resp.GetList())
		if resp.GetNextPageToken() == "" {
			return n
		}
		pageToken = resp.GetNextPageToken()
	}
}

func resourceCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}

func resourceTypeCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}

func entitlementCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	return len(resp.GetList())
}

// resourceParentChains maps each resource id to its parent resource id.
func resourceParentChains(t *testing.T, ctx context.Context, store c1zstore.Store) map[string]string {
	t.Helper()
	resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	out := map[string]string{}
	for _, r := range resp.GetList() {
		out[r.GetId().GetResource()] = r.GetParentResourceId().GetResource()
	}
	return out
}

// entitlementOwnership maps each entitlement id to its owning resource id.
func entitlementOwnership(t *testing.T, ctx context.Context, store c1zstore.Store) map[string]string {
	t.Helper()
	resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageSize: 1000}.Build())
	require.NoError(t, err)
	out := map[string]string{}
	for _, e := range resp.GetList() {
		out[parityEntitlementID(e.GetId())] = e.GetResource().GetId().GetResource()
	}
	return out
}

// grantRefs maps each grant id to its "<entitlementID>|<principalType>/<principalID>".
func grantRefs(t *testing.T, ctx context.Context, store c1zstore.Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 1000, PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			ref := parityGrantRef(g)
			out[ref] = ref
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	return out
}

func parityGrantRef(g *v2.Grant) string {
	pid := g.GetPrincipal().GetId()
	return parityEntitlementID(g.GetEntitlement().GetId()) + "|" + pid.GetResourceType() + "/" + pid.GetResource()
}

// parityEntitlementID: ids are raw pass-through on both engines now; the
// helper survives as the identity function so call sites read the same.
func parityEntitlementID(id string) string {
	return id
}

func readAsset(t *testing.T, ctx context.Context, store c1zstore.Store, assetID string) (string, []byte) {
	t.Helper()
	ct, r, err := store.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{Asset: v2.AssetRef_builder{Id: assetID}.Build()}.Build())
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	if c, ok := r.(io.Closer); ok {
		_ = c.Close()
	}
	return ct, b
}

// TestSanitizeCrossEnginePebbleParity sanitizes the same fixture across all
// four engine combinations (sqlite/pebble source x sqlite/pebble dest) under a
// single secret, then asserts every sanitized output is identical on the
// dimensions the intent names: cardinality, structural linkage, expansion-source
// edges, the full GrantExpandable blob, the needs_expansion enumeration, and the
// asset identity-strip.
//
// The matrix proves RELATIVE parity-to-SQLite; ABSOLUTE correctness of the
// transform is anchored by the sqlite->sqlite arm together with
// TestSanitizeGrantExpansionRoundTrip, so a regression uniform across all
// engines is not invisible.
func TestSanitizeCrossEnginePebbleParity(t *testing.T) {
	ctx := context.Background()
	secret := bytes32("cross-engine-parity")

	engines := []struct {
		name string
		eng  c1zstore.Engine
	}{
		{"sqlite", c1zstore.EngineSQLite},
		{"pebble", c1zstore.EnginePebble},
	}

	type arm struct {
		name string
		path string
	}
	var arms []arm

	for _, srcE := range engines {
		// Build one source per source-engine, reused for both dest engines.
		srcPath := filepath.Join(t.TempDir(), "src-"+srcE.name+".c1z")
		func() {
			src := newEngineStore(t, ctx, srcPath, srcE.eng)
			buildParityFixture(t, ctx, src)
			require.NoError(t, src.Close(ctx))
		}()

		for _, dstE := range engines {
			dstPath := filepath.Join(t.TempDir(), "dst-"+srcE.name+"-"+dstE.name+".c1z")
			src := openEngineStoreRO(t, ctx, srcPath)
			dst := newEngineStore(t, ctx, dstPath, dstE.eng)
			require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}),
				"sanitize %s->%s", srcE.name, dstE.name)
			require.NoError(t, dst.Close(ctx))
			require.NoError(t, src.Close(ctx))
			arms = append(arms, arm{name: srcE.name + "->" + dstE.name, path: dstPath})
		}
	}
	require.Len(t, arms, 4)

	// Reference transform (secret-only) to compute expected transformed ids.
	ref := newTestSanitizer(secret)
	ref.knownResourceTypes[pfRTUser] = struct{}{}
	ref.knownResourceTypes[pfRTRole] = struct{}{}
	wantAssetID := SanitizeID(secret, pfAssetID)

	// Collect every comparable projection per arm.
	type snapshot struct {
		rtCount, resCount, entCount, grantCount int
		parents, entOwn, refs                   map[string]string
		sources                                 map[string][]string
		blobs                                   map[string]expandBlob
		assetCT                                 string
		assetBytes                              []byte
	}
	snaps := make([]snapshot, len(arms))
	for i, a := range arms {
		ro := openEngineStoreRO(t, ctx, a.path)
		ct, ab := readAsset(t, ctx, ro, wantAssetID)
		snaps[i] = snapshot{
			rtCount:    resourceTypeCount(t, ctx, ro),
			resCount:   resourceCount(t, ctx, ro),
			entCount:   entitlementCount(t, ctx, ro),
			grantCount: grantCount(t, ctx, ro),
			parents:    resourceParentChains(t, ctx, ro),
			entOwn:     entitlementOwnership(t, ctx, ro),
			refs:       grantRefs(t, ctx, ro),
			sources:    grantSourcesCanonical(t, ctx, ro),
			blobs:      pendingExpansionBlobs(t, ctx, ro),
			assetCT:    ct,
			assetBytes: ab,
		}
		require.NoError(t, ro.Close(ctx))
	}

	// (1) Cardinality parity: equal to the known source cardinality and equal
	// across all four arms. The fixture has 2 resource types, 6 resources, 2
	// entitlements, 5 grants.
	for i, a := range arms {
		require.Equalf(t, 2, snaps[i].rtCount, "%s resource-type count", a.name)
		require.Equalf(t, 6, snaps[i].resCount, "%s resource count", a.name)
		require.Equalf(t, 2, snaps[i].entCount, "%s entitlement count", a.name)
		require.Equalf(t, 5, snaps[i].grantCount, "%s grant count", a.name)
	}

	// (2) Everything else: every arm identical to the first arm.
	base := snaps[0]
	for i := 1; i < len(snaps); i++ {
		require.Equalf(t, base.parents, snaps[i].parents, "%s resource parent chains", arms[i].name)
		require.Equalf(t, base.entOwn, snaps[i].entOwn, "%s entitlement ownership", arms[i].name)
		require.Equalf(t, base.refs, snaps[i].refs, "%s grant entitlement/principal refs", arms[i].name)
		require.Equalf(t, base.sources, snaps[i].sources, "%s expansion-source edges", arms[i].name)
		require.Equalf(t, base.blobs, snaps[i].blobs, "%s GrantExpandable blobs + needs_expansion enumeration", arms[i].name)
	}

	// (3) needs_expansion membership: the multi-expand and divergence-trigger
	// grants are present on EVERY arm; the whitespace-only grant is absent on
	// every arm. Keyed by transformed semantic grant ref.
	wantMulti := ref.transformID(pfEntAdmin) + "|user/" + ref.transformID("alice")
	wantDiverge := ref.transformID(pfEntAdmin) + "|user/" + ref.transformID("bob")
	wantWhitespace := ref.transformID(pfEntReader) + "|user/" + ref.transformID("carol")
	for i := range snaps {
		_, hasMulti := snaps[i].blobs[wantMulti]
		_, hasDiverge := snaps[i].blobs[wantDiverge]
		_, hasWhitespace := snaps[i].blobs[wantWhitespace]
		require.Truef(t, hasMulti, "%s: multi-expand grant must need expansion", arms[i].name)
		require.Truef(t, hasDiverge, "%s: divergence-trigger grant must need expansion", arms[i].name)
		require.Falsef(t, hasWhitespace, "%s: blank-entitlement-id grant must NOT need expansion", arms[i].name)
	}

	// (4) Multiset semantics for resource_type_ids: the multi-expand blob keeps
	// the duplicated (HMAC'd) group token at multiplicity 2, plus the preserved
	// "user" token -> 3 entries total.
	multiBlob := base.blobs[wantMulti]
	require.Len(t, multiBlob.rts, 3, "resource_type_ids multiset must preserve the duplicate group token")
	require.Contains(t, multiBlob.rts, pfRTUser, "declared resource-type token preserved verbatim")
	require.NotContains(t, multiBlob.rts, pfRTGroup, "undeclared resource-type token must be HMAC'd")

	// (5) Asset: identity STRIPPED (placeholder, not source bytes) and identical
	// across engines. Byte survival would be an identity leak.
	wantPlaceholder := placeholderForContentType("image/png")
	for i, a := range arms {
		require.Equalf(t, "image/png", snaps[i].assetCT, "%s asset content type", a.name)
		require.Equalf(t, wantPlaceholder, snaps[i].assetBytes, "%s asset payload must be the content-type placeholder", a.name)
		require.NotEqualf(t, pfSourceIconBytes, snaps[i].assetBytes, "%s asset payload must NOT be the source bytes (identity leak)", a.name)
	}
}
