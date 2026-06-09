package c1zsanitize

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// mustAnyB is the benchmark-side mustAny: no *testing.T, panics on error.
func mustAnyB(m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		panic(err)
	}
	return a
}

// newTestSanitizer builds a sanitizer the same way Sanitize does, for
// same-package unit tests of the transform/handler layer.
func newTestSanitizer(secret []byte) *sanitizer {
	return &sanitizer{
		secret:                 secret,
		hmacPool:               newHMACPool(secret),
		domains:                newDomainMap(),
		shifter:                newTimestampShifter(time.Time{}, time.Time{}),
		dropUnknownAnnotations: true,
		log:                    zap.NewNop(),
		handlers:               defaultAnnotationHandlers(),
		syncIDMap:              map[string]string{},
		knownResourceTypes:     map[string]struct{}{},
		warnedUndeclaredTypes:  map[string]struct{}{},
		droppedAnnotations:     map[string]uint64{},
		passedAnnotations:      map[string]uint64{},
		failedAnnotations:      map[string]uint64{},
	}
}

func mustUnpack[T proto.Message](t *testing.T, a *anypb.Any, dst T) T {
	t.Helper()
	require.NoError(t, a.UnmarshalTo(dst))
	return dst
}

// TestGraphAnnotationHandlers checks that the non-trait
// graph/topology annotations must be PRESERVED-and-SANITIZED, never dropped.
func TestGraphAnnotationHandlers(t *testing.T) {
	s := newTestSanitizer(bytes32("graph-annos"))
	refs := newAssetRefSet()

	in := []*anypb.Any{
		mustAny(t, v2.GrantExpandable_builder{
			EntitlementIds:  []string{"ent-a", "ent-b"},
			ResourceTypeIds: []string{"user"},
			Shallow:         true,
		}.Build()),
		mustAny(t, v2.GrantImmutable_builder{
			SourceId: "src-grant-1",
			Metadata: &structpb.Struct{Fields: map[string]*structpb.Value{
				"k": structpb.NewStringValue("secret-value"),
			}},
		}.Build()),
		mustAny(t, v2.EntitlementImmutable_builder{SourceId: "src-ent-1"}.Build()),
		mustAny(t, v2.ExternalLink_builder{Url: "https://acme.example/private/path?token=abc"}.Build()),
		mustAny(t, v2.ETag_builder{Value: "etag-xyz", EntitlementId: "ent-a"}.Build()),
		mustAny(t, v2.ChildResourceType_builder{ResourceTypeId: "group"}.Build()),
	}

	out := s.transformAnnotations(in, refs)
	require.Len(t, out, len(in), "every graph annotation must survive (none dropped)")

	byType := map[string]*anypb.Any{}
	for _, a := range out {
		byType[a.GetTypeUrl()] = a
	}

	// GrantExpandable: entitlement ids transformed via transformID; an
	// unknown resource-type token is HMAC'd (not a declared type here).
	ge := mustUnpack(t, byType[typeURL(&v2.GrantExpandable{})], &v2.GrantExpandable{})
	require.Equal(t, []string{s.transformID("ent-a"), s.transformID("ent-b")}, ge.GetEntitlementIds())
	require.NotContains(t, ge.GetEntitlementIds(), "ent-a")
	require.True(t, ge.GetShallow(), "structural shallow flag preserved")
	require.Equal(t, []string{s.id("user")}, ge.GetResourceTypeIds())

	// GrantImmutable: source id transformed, metadata string leaf HMAC'd.
	gi := mustUnpack(t, byType[typeURL(&v2.GrantImmutable{})], &v2.GrantImmutable{})
	require.Equal(t, s.transformID("src-grant-1"), gi.GetSourceId())
	require.Equal(t, s.id("secret-value"), gi.GetMetadata().GetFields()["k"].GetStringValue())

	// EntitlementImmutable: source id transformed.
	ei := mustUnpack(t, byType[typeURL(&v2.EntitlementImmutable{})], &v2.EntitlementImmutable{})
	require.Equal(t, s.transformID("src-ent-1"), ei.GetSourceId())

	// ExternalLink: URL redacted to the placeholder.
	el := mustUnpack(t, byType[typeURL(&v2.ExternalLink{})], &v2.ExternalLink{})
	require.Equal(t, redactedURL, el.GetUrl())

	// ETag: entitlement id transformed; value sanitized (not preserved).
	et := mustUnpack(t, byType[typeURL(&v2.ETag{})], &v2.ETag{})
	require.Equal(t, s.transformID("ent-a"), et.GetEntitlementId())
	require.NotEqual(t, "etag-xyz", et.GetValue())
	require.Equal(t, s.id("etag-xyz"), et.GetValue())

	// ChildResourceType: token HMAC'd (not a declared type here).
	crt := mustUnpack(t, byType[typeURL(&v2.ChildResourceType{})], &v2.ChildResourceType{})
	require.Equal(t, s.id("group"), crt.GetResourceTypeId())
}

// TestGraphAnnotationDeterminism: the same (secret, input) yields identical
// sanitized annotations across independent sanitizer instances.
func TestGraphAnnotationDeterminism(t *testing.T) {
	in := []*anypb.Any{
		mustAny(t, v2.GrantExpandable_builder{EntitlementIds: []string{"e1", "e2"}}.Build()),
		mustAny(t, v2.ETag_builder{Value: "v", EntitlementId: "e1"}.Build()),
	}
	a := newTestSanitizer(bytes32("det")).transformAnnotations(in, newAssetRefSet())
	b := newTestSanitizer(bytes32("det")).transformAnnotations(in, newAssetRefSet())
	require.Len(t, a, 2)
	require.Len(t, b, 2)
	for i := range a {
		require.True(t, proto.Equal(a[i], b[i]), "annotation %d differs across runs", i)
	}
}

// TestGrantSubCacheEquivalence: the memoized embedded-transform path
// must produce output identical to the uncached path for grants that share
// embedded entitlements/principals.
func TestGrantSubCacheEquivalence(t *testing.T) {
	secret := bytes32("cache-eq")

	mkGrant := func(grantID string) *v2.Grant {
		ent := v2.Entitlement_builder{
			Id:          "ent-shared",
			DisplayName: "Shared Entitlement",
			Resource: v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "role", Resource: "r-shared"}.Build(),
				DisplayName: "Shared Role",
			}.Build(),
		}.Build()
		principal := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u-shared"}.Build(),
			DisplayName: "Shared User",
			Annotations: []*anypb.Any{mustAny(t, v2.UserTrait_builder{Login: "shared@acme.com"}.Build())},
		}.Build()
		return v2.Grant_builder{
			Id:          grantID,
			Entitlement: ent,
			Principal:   principal,
			Annotations: []*anypb.Any{mustAny(t, v2.ETag_builder{Value: "e", EntitlementId: "ent-shared"}.Build())},
		}.Build()
	}

	grantIDs := []string{"g1", "g2", "g3"}

	// Cached path: one sanitizer + one shared cache across all grants.
	cachedSan := newTestSanitizer(secret)
	cache := newGrantSubCache(true) // verify guard on
	var cached []*v2.Grant
	for _, id := range grantIDs {
		cached = append(cached, cachedSan.transformGrant(mkGrant(id), newAssetRefSet(), cache))
	}

	// Uncached path: fresh sanitizer, nil cache (falls back to direct transform).
	uncachedSan := newTestSanitizer(secret)
	var uncached []*v2.Grant
	for _, id := range grantIDs {
		uncached = append(uncached, uncachedSan.transformGrant(mkGrant(id), newAssetRefSet(), nil))
	}

	require.Len(t, cached, len(grantIDs))
	for i := range cached {
		require.True(t, proto.Equal(cached[i], uncached[i]),
			"cached vs uncached grant %d differ", i)
	}

	// The shared entitlement transform must be physically reused on hits.
	require.Same(t, cached[0].GetEntitlement(), cached[1].GetEntitlement(),
		"embedded entitlement should be memoized (shared pointer) across grants")
}

// benchGrants builds n grants drawn from e distinct entitlements and p
// distinct principals, each carrying trait + graph annotations — the shape
// the memo cache targets. Grants are emitted grouped by entitlement
// (as dotc1z does), so the cache sees long hit runs.
func benchGrants(n, e, p int) []*v2.Grant {
	out := make([]*v2.Grant, 0, n)
	for i := 0; i < n; i++ {
		ei := i % e
		pi := i % p
		ent := v2.Entitlement_builder{
			Id:          "ent-" + itoa(ei),
			DisplayName: "Entitlement " + itoa(ei),
			Resource: v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "role", Resource: "r-" + itoa(ei)}.Build(),
				DisplayName: "Role " + itoa(ei),
			}.Build(),
			Annotations: []*anypb.Any{mustAnyB(v2.EntitlementImmutable_builder{SourceId: "esrc-" + itoa(ei)}.Build())},
		}.Build()
		principal := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u-" + itoa(pi)}.Build(),
			DisplayName: "User " + itoa(pi),
			Annotations: []*anypb.Any{mustAnyB(v2.UserTrait_builder{Login: "u" + itoa(pi) + "@acme.com"}.Build())},
		}.Build()
		out = append(out, v2.Grant_builder{
			Id:          "grant-" + itoa(i),
			Entitlement: ent,
			Principal:   principal,
			Annotations: []*anypb.Any{
				mustAnyB(v2.ETag_builder{Value: "etag-" + itoa(i), EntitlementId: "ent-" + itoa(ei)}.Build()),
				mustAnyB(v2.ExternalLink_builder{Url: "https://acme.example/g/" + itoa(i)}.Build()),
			},
		}.Build())
	}
	return out
}

func itoa(i int) string { return strconv.Itoa(i) }

func BenchmarkTransformGrant(b *testing.B) {
	const (
		grants       = 5000
		entitlements = 5 // G/E = 1000, the plan's default ratio
		principals   = 500
	)
	src := benchGrants(grants, entitlements, principals)
	secret := bytes32("bench")

	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := newTestSanitizer(secret)
			cache := newGrantSubCache(false)
			for _, g := range src {
				_ = s.transformGrant(g, newAssetRefSet(), cache)
			}
		}
	})

	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := newTestSanitizer(secret)
			for _, g := range src {
				_ = s.transformGrant(g, newAssetRefSet(), nil)
			}
		}
	})
}

// TestTransformResourceTypeDoesNotMutateKnownSet is the order-independence guard:
// transformResourceType must be pure w.r.t. knownResourceTypes. Registration
// happens only in copyResourceTypes' buffering pre-pass, so an embedded
// resource type reached during the entitlements phase (GrantableTo ->
// transformResourceTypeSlice -> transformResourceType) must NOT become
// "known", or transformID's type-token decision would flip with stream order.
//
// With the pre-pass model the set is read-only by construction; this locks
// that transformResourceType never writes it.
func TestTransformResourceTypeDoesNotMutateKnownSet(t *testing.T) {
	s := newTestSanitizer(bytes32("c2-order"))

	// Simulate the pre-pass having declared only "user".
	s.knownResourceTypes["user"] = struct{}{}

	// Composite id whose TYPE component "etype" is NOT a declared type.
	before := s.transformID("etype:res1")

	// Process an entitlement whose GrantableTo references the undeclared
	// "etype" — the exact embedded path that previously mutated the set.
	ent := v2.Entitlement_builder{
		Id:          "ent1",
		GrantableTo: []*v2.ResourceType{v2.ResourceType_builder{Id: "etype"}.Build()},
	}.Build()
	_ = s.transformEntitlement(ent, newAssetRefSet())

	after := s.transformID("etype:res1")

	require.Equal(t, before, after,
		"transformID must be order-independent: an embedded resource type must not change the known-type decision")
	require.NotContains(t, s.knownResourceTypes, "etype",
		"embedded resource type leaked into knownResourceTypes — transformResourceType must be pure w.r.t. the set (B1)")
	require.Contains(t, s.knownResourceTypes, "user")
}
