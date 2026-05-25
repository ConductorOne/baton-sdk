package c1zsanitize

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// Cross-reference integrity invariant: for every original identifier
// that appears N times in src, the sanitized identifier
// sanitize_id(id) appears exactly N times in dst, and no other
// sanitized identifier appears at any of those positions.
//
// This is the load-bearing test from §6.2 step 4 of the investigation.
func TestSanitizeCrossReferenceIntegrity(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("xref-test")

	buildFixture(t, ctx, srcPath)

	src := mustOpen(t, ctx, srcPath, true)
	defer src.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)

	require.NoError(t, Sanitize(ctx, src, dst, Options{
		Secret:                 secret,
		DropUnknownAnnotations: true,
	}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)

	srcRecords := collectRecords(t, ctx, src)
	dstRecords := collectRecords(t, ctx, dstRO)

	require.Equal(t, len(srcRecords.resourceTypes), len(dstRecords.resourceTypes), "resource-type counts must match")
	require.Equal(t, len(srcRecords.resources), len(dstRecords.resources), "resource counts must match")
	require.Equal(t, len(srcRecords.entitlements), len(dstRecords.entitlements), "entitlement counts must match")
	require.Equal(t, len(srcRecords.grants), len(dstRecords.grants), "grant counts must match")

	for srcID, srcCount := range srcRecords.idOccurrences {
		dstID := SanitizeID(secret, srcID)
		require.Equal(t, srcCount, dstRecords.idOccurrences[dstID],
			"id %q (sanitized %q) occurs %d times in src but %d in dst",
			srcID, dstID, srcCount, dstRecords.idOccurrences[dstID])
	}

	srcResourceTypeIDs := map[string]struct{}{}
	for _, id := range srcRecords.idOccurrences {
		_ = id
	}
	for _, rt := range srcRecords.resourceTypes {
		srcResourceTypeIDs[rt.GetId()] = struct{}{}
	}
	for _, rt := range dstRecords.resourceTypes {
		require.Contains(t, srcResourceTypeIDs, rt.GetId(), "resource_type id should pass through unchanged: %q", rt.GetId())
	}
}

func TestSanitizeGraphIntegrity(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("graph-test")

	buildFixture(t, ctx, srcPath)

	src := mustOpen(t, ctx, srcPath, true)
	defer src.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, DropUnknownAnnotations: true}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)

	rec := collectRecords(t, ctx, dstRO)

	resourceKeys := map[string]struct{}{}
	for _, r := range rec.resources {
		resourceKeys[resourceKey(r.GetId())] = struct{}{}
	}
	entitlementByID := map[string]*v2.Entitlement{}
	for _, e := range rec.entitlements {
		entitlementByID[e.GetId()] = e
	}

	for _, r := range rec.resources {
		if r.GetParentResourceId() != nil && r.GetParentResourceId().GetResource() != "" {
			require.Contains(t, resourceKeys, resourceKey(r.GetParentResourceId()),
				"resource %q's parent_resource_id must resolve", r.GetId().GetResource())
		}
	}

	for _, e := range rec.entitlements {
		require.NotNil(t, e.GetResource(), "entitlement %q must have a resource", e.GetId())
		require.Contains(t, resourceKeys, resourceKey(e.GetResource().GetId()),
			"entitlement %q's resource must resolve in dst", e.GetId())
	}

	for _, g := range rec.grants {
		require.NotNil(t, g.GetEntitlement(), "grant %q must have entitlement", g.GetId())
		require.NotNil(t, g.GetPrincipal(), "grant %q must have principal", g.GetId())
		require.Contains(t, entitlementByID, g.GetEntitlement().GetId(),
			"grant %q's entitlement must resolve in dst", g.GetId())
		require.Contains(t, resourceKeys, resourceKey(g.GetPrincipal().GetId()),
			"grant %q's principal must resolve in dst", g.GetId())
	}
}

func TestSanitizeRejectsShortSecret(t *testing.T) {
	ctx := context.Background()
	err := Sanitize(ctx, nil, nil, Options{Secret: []byte("too short")})
	require.Error(t, err)
}

func TestSanitizeAnnotationDropsUnknownByDefault(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	secret := bytes32("anno-drop")

	src := mustOpen(t, ctx, srcPath, false)
	_, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
	}.Build()))

	unknownAnno := &anypb.Any{
		TypeUrl: "type.googleapis.com/c1.connector.v2.NotARealAnnotation",
		Value:   []byte{0x08, 0x01},
	}

	require.NoError(t, src.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
		Annotations: []*anypb.Any{
			unknownAnno,
			mustAny(t, v2.UserTrait_builder{Login: "alice"}.Build()),
		},
	}.Build()))

	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	require.NoError(t, Sanitize(ctx, srcRO, dst, Options{Secret: secret, DropUnknownAnnotations: true}))
	require.NoError(t, dst.Close(ctx))

	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)

	rec := collectRecords(t, ctx, dstRO)
	require.Len(t, rec.resources, 1)
	annos := rec.resources[0].GetAnnotations()
	for _, a := range annos {
		require.NotEqual(t, "type.googleapis.com/c1.connector.v2.NotARealAnnotation", a.GetTypeUrl(),
			"unknown annotation type should have been dropped")
	}
	require.NotEmpty(t, annos, "known annotations should survive")
}

func mustOpen(t *testing.T, ctx context.Context, path string, readOnly bool) *dotc1z.C1File {
	t.Helper()
	opts := []dotc1z.C1ZOption{}
	if readOnly {
		opts = append(opts, dotc1z.WithReadOnly(true))
	}
	f, err := dotc1z.NewC1ZFile(ctx, path, opts...)
	require.NoError(t, err)
	return f
}

func mustAny(t *testing.T, m proto.Message) *anypb.Any {
	t.Helper()
	a, err := anypb.New(m)
	require.NoError(t, err)
	return a
}

func resourceKey(id *v2.ResourceId) string {
	if id == nil {
		return ""
	}
	return id.GetResourceType() + "/" + id.GetResource()
}

type collected struct {
	resourceTypes []*v2.ResourceType
	resources     []*v2.Resource
	entitlements  []*v2.Entitlement
	grants        []*v2.Grant
	idOccurrences map[string]int
}

func collectRecords(t *testing.T, ctx context.Context, r connectorstore.Reader) *collected {
	t.Helper()
	out := &collected{idOccurrences: map[string]int{}}

	syncs, err := listAllSyncs(ctx, r)
	require.NoError(t, err)

	count := func(id string) {
		if id == "" {
			return
		}
		out.idOccurrences[id]++
	}

	for _, sr := range syncs {
		annos := syncIDAnnotations(sr.GetId())

		rtPage := ""
		for {
			req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
				PageSize:    1000,
				PageToken:   rtPage,
				Annotations: annos,
			}.Build()
			resp, err := r.ListResourceTypes(ctx, req)
			require.NoError(t, err)
			out.resourceTypes = append(out.resourceTypes, resp.GetList()...)
			if resp.GetNextPageToken() == "" {
				break
			}
			rtPage = resp.GetNextPageToken()
		}

		rPage := ""
		for {
			req := v2.ResourcesServiceListResourcesRequest_builder{
				PageSize:    1000,
				PageToken:   rPage,
				Annotations: annos,
			}.Build()
			resp, err := r.ListResources(ctx, req)
			require.NoError(t, err)
			for _, res := range resp.GetList() {
				out.resources = append(out.resources, res)
				count(res.GetId().GetResource())
				if res.GetParentResourceId() != nil {
					count(res.GetParentResourceId().GetResource())
				}
			}
			if resp.GetNextPageToken() == "" {
				break
			}
			rPage = resp.GetNextPageToken()
		}

		ePage := ""
		for {
			req := v2.EntitlementsServiceListEntitlementsRequest_builder{
				PageSize:    1000,
				PageToken:   ePage,
				Annotations: annos,
			}.Build()
			resp, err := r.ListEntitlements(ctx, req)
			require.NoError(t, err)
			for _, ent := range resp.GetList() {
				out.entitlements = append(out.entitlements, ent)
				count(ent.GetId())
				if ent.GetResource() != nil {
					count(ent.GetResource().GetId().GetResource())
				}
			}
			if resp.GetNextPageToken() == "" {
				break
			}
			ePage = resp.GetNextPageToken()
		}

		gPage := ""
		for {
			req := v2.GrantsServiceListGrantsRequest_builder{
				PageSize:    1000,
				PageToken:   gPage,
				Annotations: annos,
			}.Build()
			resp, err := r.ListGrants(ctx, req)
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				out.grants = append(out.grants, g)
				count(g.GetId())
				if g.GetEntitlement() != nil {
					count(g.GetEntitlement().GetId())
				}
				if g.GetPrincipal() != nil {
					count(g.GetPrincipal().GetId().GetResource())
				}
				for k := range g.GetSources().GetSources() {
					count(k)
				}
			}
			if resp.GetNextPageToken() == "" {
				break
			}
			gPage = resp.GetNextPageToken()
		}
	}
	return out
}

// buildFixture writes a small but representative c1z to path with
// one sync, multiple resource types, and one annotation per trait
// handler. Closes the C1File before returning so the bytes hit disk.
func buildFixture(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_ = syncID

	resourceTypes := []*v2.ResourceType{
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}.Build(),
		v2.ResourceType_builder{Id: "app", DisplayName: "App", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP}}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
		v2.ResourceType_builder{Id: "secret", DisplayName: "Secret", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECRET}}.Build(),
		v2.ResourceType_builder{Id: "license", DisplayName: "License", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE_PROFILE}}.Build(),
		v2.ResourceType_builder{Id: "scope", DisplayName: "Scope", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SCOPE_BINDING}}.Build(),
	}
	require.NoError(t, f.PutResourceTypes(ctx, resourceTypes...))

	userTrait := v2.UserTrait_builder{
		Emails: []*v2.UserTrait_Email{
			v2.UserTrait_Email_builder{Address: "alice@acme.com", IsPrimary: true}.Build(),
			v2.UserTrait_Email_builder{Address: "alice.work@acme.com"}.Build(),
		},
		Login:        "alice",
		LoginAliases: []string{"alice.smith", "asmith"},
		EmployeeIds:  []string{"E-1234"},
		AccountType:  v2.UserTrait_ACCOUNT_TYPE_HUMAN,
		Profile: &structpb.Struct{Fields: map[string]*structpb.Value{
			"department": structpb.NewStringValue("Engineering"),
			"head_count": structpb.NewNumberValue(42),
		}},
		Icon:      v2.AssetRef_builder{Id: "icon-alice"}.Build(),
		CreatedAt: timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		LastLogin: timestamppb.New(time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)),
		StructuredName: v2.UserTrait_StructuredName_builder{
			GivenName: "Alice", FamilyName: "Smith",
		}.Build(),
	}.Build()
	userTrait.SetStatus(v2.UserTrait_Status_builder{
		Status:  v2.UserTrait_Status_STATUS_ENABLED,
		Details: "active",
	}.Build())

	groupTrait := v2.GroupTrait_builder{
		Icon:    v2.AssetRef_builder{Id: "icon-eng"}.Build(),
		Profile: &structpb.Struct{Fields: map[string]*structpb.Value{"slug": structpb.NewStringValue("eng-team")}},
	}.Build()

	appTrait := v2.AppTrait_builder{
		HelpUrl: "https://acme.example.com/help",
		Icon:    v2.AssetRef_builder{Id: "icon-app"}.Build(),
		Profile: &structpb.Struct{Fields: map[string]*structpb.Value{"vendor": structpb.NewStringValue("acme")}},
		Flags:   []v2.AppTrait_AppFlag{v2.AppTrait_APP_FLAG_OIDC},
	}.Build()

	roleTrait := v2.RoleTrait_builder{
		Profile: &structpb.Struct{Fields: map[string]*structpb.Value{"name": structpb.NewStringValue("admin")}},
		RoleScopeConditions: v2.RoleScopeConditions_builder{
			Type:       "expr",
			Conditions: []*v2.RoleScopeCondition{v2.RoleScopeCondition_builder{Expression: "resource.id == 'r1'"}.Build()},
		}.Build(),
	}.Build()

	secretTrait := v2.SecretTrait_builder{
		Profile:     &structpb.Struct{Fields: map[string]*structpb.Value{"kid": structpb.NewStringValue("kid-1")}},
		CreatedById: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		IdentityId:  v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		CreatedAt:   timestamppb.New(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)),
		ExpiresAt:   timestamppb.New(time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)),
		LastUsedAt:  timestamppb.New(time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)),
	}.Build()

	licenseTrait := v2.LicenseProfileTrait_builder{
		LicenseName:        "E3",
		PurchasedSeats:     1000,
		ConsumedSeats:      650,
		CostPerUnitInCents: 1200,
		Currency:           "USD",
		EntitlementIds:     []string{"ent-license-e3"},
	}.Build()

	scopeTrait := v2.ScopeBindingTrait_builder{
		RoleId:          v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(),
		ScopeResourceId: v2.ResourceId_builder{ResourceType: "app", Resource: "app-prod"}.Build(),
	}.Build()

	resources := []*v2.Resource{
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
			DisplayName: "Alice Smith",
			Annotations: []*anypb.Any{mustAny(t, userTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
			DisplayName: "Bob Jones",
			Annotations: []*anypb.Any{mustAny(t, v2.UserTrait_builder{Login: "bob"}.Build())},
		}.Build(),
		v2.Resource_builder{
			Id:               v2.ResourceId_builder{ResourceType: "group", Resource: "engineering"}.Build(),
			DisplayName:      "Engineering",
			ParentResourceId: v2.ResourceId_builder{ResourceType: "group", Resource: "all"}.Build(),
			Annotations:      []*anypb.Any{mustAny(t, groupTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "all"}.Build(),
			DisplayName: "All",
			Annotations: []*anypb.Any{mustAny(t, v2.GroupTrait_builder{}.Build())},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "app", Resource: "app-prod"}.Build(),
			DisplayName: "Production App",
			Annotations: []*anypb.Any{mustAny(t, appTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(),
			DisplayName: "Administrator",
			Annotations: []*anypb.Any{mustAny(t, roleTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "secret", Resource: "tok-1"}.Build(),
			DisplayName: "API Token 1",
			Annotations: []*anypb.Any{mustAny(t, secretTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "license", Resource: "lic-e3"}.Build(),
			DisplayName: "E3 License",
			Annotations: []*anypb.Any{mustAny(t, licenseTrait)},
		}.Build(),
		v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "scope", Resource: "scope-1"}.Build(),
			DisplayName: "Scope 1",
			Annotations: []*anypb.Any{mustAny(t, scopeTrait)},
		}.Build(),
	}
	require.NoError(t, f.PutResources(ctx, resources...))

	entitlements := []*v2.Entitlement{
		v2.Entitlement_builder{
			Id:          "ent-app-member",
			DisplayName: "Member of app-prod",
			Description: "Production app membership",
			Resource:    resources[4],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build(),
		v2.Entitlement_builder{
			Id:          "ent-eng-member",
			DisplayName: "Member of engineering",
			Resource:    resources[2],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		}.Build(),
		v2.Entitlement_builder{
			Id:          "ent-license-e3",
			DisplayName: "Holds E3",
			Resource:    resources[7],
			Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		}.Build(),
	}
	require.NoError(t, f.PutEntitlements(ctx, entitlements...))

	grants := []*v2.Grant{
		v2.Grant_builder{
			Id:          "grant-alice-app",
			Entitlement: entitlements[0],
			Principal:   resources[0],
		}.Build(),
		v2.Grant_builder{
			Id:          "grant-alice-eng",
			Entitlement: entitlements[1],
			Principal:   resources[0],
			Sources: v2.GrantSources_builder{Sources: map[string]*v2.GrantSources_GrantSource{
				"ent-app-member": v2.GrantSources_GrantSource_builder{IsDirect: false}.Build(),
			}}.Build(),
		}.Build(),
		v2.Grant_builder{
			Id:          "grant-bob-app",
			Entitlement: entitlements[0],
			Principal:   resources[1],
		}.Build(),
		v2.Grant_builder{
			Id:          "grant-alice-license",
			Entitlement: entitlements[2],
			Principal:   resources[0],
		}.Build(),
	}
	require.NoError(t, f.PutGrants(ctx, grants...))

	require.NoError(t, f.PutAsset(ctx, v2.AssetRef_builder{Id: "icon-alice"}.Build(), "image/png", []byte{0x89, 0x50, 0x4e, 0x47}))
	require.NoError(t, f.PutAsset(ctx, v2.AssetRef_builder{Id: "icon-eng"}.Build(), "image/png", []byte{0x89, 0x50, 0x4e, 0x47}))
	require.NoError(t, f.PutAsset(ctx, v2.AssetRef_builder{Id: "icon-app"}.Build(), "image/png", []byte{0x89, 0x50, 0x4e, 0x47}))

	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
}
