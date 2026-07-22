package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// grantDiscoveredAtAnnotation returns the GrantDiscoveredAt annotation
// on the grant, or nil when none is present.
func grantDiscoveredAtAnnotation(t *testing.T, g *v2.Grant) *v2.GrantDiscoveredAt {
	t.Helper()
	for _, a := range g.GetAnnotations() {
		if a == nil || !a.MessageIs((*v2.GrantDiscoveredAt)(nil)) {
			continue
		}
		out := &v2.GrantDiscoveredAt{}
		require.NoError(t, a.UnmarshalTo(out))
		return out
	}
	return nil
}

// findGrant returns the grant in the list whose id matches, or nil.
func findGrant(list []*v2.Grant, id string) *v2.Grant {
	for _, g := range list {
		if g.GetId() == id {
			return g
		}
	}
	return nil
}

// TestListGrantsForEntitlementWithDiscoveredAt verifies the opt-in
// reader carries each grant's stored discovered_at back as a
// GrantDiscoveredAt annotation, while the default
// ListGrantsForEntitlement output stays free of the annotation.
func TestListGrantsForEntitlementWithDiscoveredAt(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seeded := timestamppb.New(time.Date(2021, 6, 7, 8, 9, 10, 0, time.UTC))
	seed := v3.GrantRecord_builder{
		ExternalId: "g-1",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "alice",
		}.Build(),
		DiscoveredAt: seeded,
	}.Build()
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	// publicGrantRecordID returns the record's ExternalId verbatim when set.
	wantID := "g-1"

	// 1. The opt-in reader carries discovered_at as an annotation.
	respWith, err := e.ListGrantsForEntitlementWithDiscoveredAt(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	gotWith := findGrant(respWith.GetList(), wantID)
	require.NotNil(t, gotWith, "expected grant %q in WithDiscoveredAt response", wantID)
	ann := grantDiscoveredAtAnnotation(t, gotWith)
	require.NotNil(t, ann, "WithDiscoveredAt grant must carry a GrantDiscoveredAt annotation")
	require.Equal(t, seeded.AsTime(), ann.GetDiscoveredAt().AsTime(),
		"annotation timestamp must equal the seeded discovered_at")

	// 2. The default reader returns the same grant WITHOUT the annotation.
	respDefault, err := e.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	gotDefault := findGrant(respDefault.GetList(), wantID)
	require.NotNil(t, gotDefault, "expected grant %q in default response", wantID)
	require.Nil(t, grantDiscoveredAtAnnotation(t, gotDefault),
		"default ListGrantsForEntitlement must not carry a GrantDiscoveredAt annotation")
}

// TestListGrantsForEntitlementWithDiscoveredAtNil verifies a grant with
// no recorded discovered_at carries no annotation on the opt-in path.
func TestListGrantsForEntitlementWithDiscoveredAtNil(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, filepath.Join(t.TempDir(), "engine"))
	require.NoError(t, err)
	defer func() { _ = e.Close() }()

	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	seed := v3.GrantRecord_builder{
		ExternalId: "g-nil",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-B"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user", ResourceId: "bob",
		}.Build(),
	}.Build()
	require.Nil(t, seed.GetDiscoveredAt())
	require.NoError(t, e.PutGrantRecord(ctx, seed))

	resp, err := e.ListGrantsForEntitlementWithDiscoveredAt(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-B")}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	got := findGrant(resp.GetList(), "g-nil")
	require.NotNil(t, got)
	require.Nil(t, grantDiscoveredAtAnnotation(t, got),
		"grant with nil discovered_at must carry no GrantDiscoveredAt annotation")
}
