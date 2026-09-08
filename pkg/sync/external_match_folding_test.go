package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// External-resource key/value matching compares with strings.EqualFold. These
// tests pin that comparison as a behavioral contract at the
// processGrantsWithExternalPrincipals seam rather than at whatever internal
// helper happens to implement it, so an implementation that swaps the
// comparison out has to answer for the difference.
//
// EqualFold is Unicode simple case folding, which is not the same relation as
// lowercasing both sides. The two disagree in both directions, and each
// direction is a distinct access-correctness failure:
//
//   - EqualFold true, lowercase forms differ: a real grant is silently dropped
//     and a principal loses access it should have.
//   - EqualFold false, lowercase forms equal: a grant is manufactured and a
//     principal gains access it should not have.
//
// Every case below states its expectation and then pins that expectation to
// strings.EqualFold, so a case added later cannot quietly encode a divergence.

// externalMatchFoldCase is one (stored value, queried value) pair together with
// whether external matching must pair them.
type externalMatchFoldCase struct {
	name          string
	stored, query string
	matches       bool
}

func externalMatchFoldCases() []externalMatchFoldCase {
	return []externalMatchFoldCase{
		{name: "identical", stored: "target@example.com", query: "target@example.com", matches: true},
		{name: "ascii case differs", stored: "TARGET@Example.com", query: "target@example.com", matches: true},
		{name: "latin with diaeresis", stored: "Ann-Sofie.Ö", query: "ann-sofie.ö", matches: true},
		{name: "cyrillic", stored: "ПЕТРОВ", query: "петров", matches: true},
		{name: "cjk is caseless", stored: "山田太郎", query: "山田太郎", matches: true},
		{name: "different values", stored: "target@example.com", query: "other@example.com", matches: false},

		// Medial and final sigma case-fold together, so these match even though
		// their lowercase forms differ ("σ" vs "ς"). An implementation that
		// bucketed on strings.ToLower would drop the grant.
		{name: "greek final sigma", stored: "Σ", query: "ς", matches: true},
		{name: "greek final sigma in word", stored: "ΣΤΕΦΑΝΟΣ", query: "στεφανος", matches: true},
		// Long s folds with ordinary s, and strings.ToLower leaves it alone.
		{name: "latin long s", stored: "ſ", query: "s", matches: true},

		// The dotted capital I does not fold to "i", so these must not match
		// even though strings.ToLower("İ") is "i". An implementation that
		// bucketed on strings.ToLower would manufacture the grant.
		{name: "turkish dotted capital i", stored: "İ", query: "i", matches: false},
		{name: "turkish dotted capital i in word", stored: "İSTANBUL", query: "istanbul", matches: false},
		// Dotless i likewise does not fold with "i".
		{name: "turkish dotless i", stored: "ı", query: "i", matches: false},

		// Case folding is per-rune, so it never expands "ß" into "ss".
		{name: "sharp s does not expand", stored: "ß", query: "ss", matches: false},
		{name: "capital sharp s folds", stored: "ẞ", query: "ß", matches: true},
	}
}

func TestExternalResourceMatchProfileFoldingContract(t *testing.T) {
	for _, tc := range externalMatchFoldCases() {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.matches, strings.EqualFold(tc.stored, tc.query),
				"premise: external matching compares with strings.EqualFold")

			principal := externalMatchPrincipal(t, "external-user-1",
				map[string]any{"upn": tc.stored})
			matched := runExternalMatchFold(t, principal, "upn", tc.query)

			require.Equal(t, tc.matches, matched,
				"profile %q vs match value %q: expected matched=%t", tc.stored, tc.query, tc.matches)
		})
	}
}

func TestExternalResourceMatchEmailFoldingContract(t *testing.T) {
	for _, tc := range externalMatchFoldCases() {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.matches, strings.EqualFold(tc.stored, tc.query),
				"premise: external matching compares with strings.EqualFold")

			principal := externalMatchPrincipal(t, "external-user-1", nil,
				rs.WithEmail(tc.stored, true))
			matched := runExternalMatchFold(t, principal, "email", tc.query)

			require.Equal(t, tc.matches, matched,
				"user-trait email %q vs match value %q: expected matched=%t", tc.stored, tc.query, tc.matches)
		})
	}
}

// A principal that a carrier can reach by more than one route still receives
// exactly one grant. Both routes are reachable at once for the "email" key,
// which matches a user-trait address as well as a profile field of that name.
func TestExternalResourceMatchEmitsOneGrantPerPrincipal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		principal func(*testing.T) *v2.Resource
	}{
		{
			name: "two trait addresses folding alike",
			principal: func(t *testing.T) *v2.Resource {
				return externalMatchPrincipal(t, "external-user-1", nil,
					rs.WithEmail("shared@example.com", true),
					rs.WithEmail("SHARED@example.com", false),
				)
			},
		},
		{
			name: "trait address and profile email",
			principal: func(t *testing.T) *v2.Resource {
				return externalMatchPrincipal(t, "external-user-1",
					map[string]any{"email": "shared@example.com"},
					rs.WithEmail("SHARED@example.com", true),
				)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			principals := []*v2.Resource{tc.principal(t)}
			observed := observeExternalMatch(t, principals, "email", "shared@example.com")
			require.Equal(t, []string{"external-user-1"}, observed.principalIDs,
				"a principal reachable by two routes must still receive one grant")
			require.Zero(t, observed.carriers, "the carrier must not survive")
		})
	}
}

// externalMatchPrincipal builds an external user principal. The BatonID marker
// is what processGrantsWithExternalPrincipals uses to recognize a copied
// external principal, so a principal without it is ignored outright.
func externalMatchPrincipal(
	t *testing.T,
	objectID string,
	profile map[string]any,
	traitOpts ...rs.UserTraitOption,
) *v2.Resource {
	t.Helper()
	opts := make([]rs.ResourceOption, 0, 1)
	if profile != nil {
		opts = append(opts, rs.WithResourceProfile(profile))
	}
	principal, err := rs.NewUserResource(objectID, userResourceType, objectID, traitOpts, opts...)
	require.NoError(t, err)

	annos := annotations.Annotations(principal.GetAnnotations())
	annos.Update(&v2.BatonID{})
	principal.SetAnnotations(annos)
	return principal
}

// runExternalMatchFold reports whether the single supplied principal received a
// rewritten grant, and asserts the carrier was consumed either way.
func runExternalMatchFold(t *testing.T, principal *v2.Resource, key, value string) bool {
	t.Helper()
	observed := observeExternalMatch(t, []*v2.Resource{principal}, key, value)
	require.Zero(t, observed.carriers,
		"the carrier is consumed whether or not it matched")
	require.LessOrEqual(t, len(observed.principalIDs), 1, "one principal cannot yield two grants")
	return len(observed.principalIDs) == 1
}

type externalMatchObservation struct {
	principalIDs []string
	carriers     int
}

// observeExternalMatch seeds a store with one carrier grant annotated for
// external matching, runs the production rewrite step against principals, and
// reports which principals ended up granted.
func observeExternalMatch(
	t *testing.T,
	principals []*v2.Resource,
	key, value string,
) externalMatchObservation {
	t.Helper()
	ctx := t.Context()
	tmpDir := t.TempDir()

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "internal.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	group, err := rs.NewGroupResource("Internal Group", groupResourceType, "internal-group-1", nil)
	require.NoError(t, err)
	placeholder := v2.ResourceId_builder{
		ResourceType: userResourceType.GetId(),
		Resource:     externalMatchPlaceholderID,
	}.Build()
	carrier := gt.NewGrant(group, "member", placeholder,
		gt.WithAnnotation(v2.ExternalResourceMatch_builder{
			Key:          key,
			Value:        value,
			ResourceType: v2.ResourceType_TRAIT_USER,
		}.Build()),
	)
	require.NoError(t, store.PutGrants(ctx, carrier))

	state := newState()
	state.SetHasExternalResourcesGrants()
	state.PushAction(ctx, Action{Op: SyncExternalResourcesOp})

	syncer := &syncer{state: state}
	syncer.setStore(store)
	require.NoError(t, syncer.processGrantsWithExternalPrincipals(ctx, principals))

	return readExternalMatchGrants(t, ctx, store)
}

const externalMatchPlaceholderID = "placeholder"

func readExternalMatchGrants(
	t *testing.T,
	ctx context.Context,
	store c1zstore.Store,
) externalMatchObservation {
	t.Helper()
	observed := externalMatchObservation{}
	for annotated, err := range store.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		principalID := annotated.Grant.GetPrincipal().GetId().GetResource()
		if principalID == externalMatchPlaceholderID {
			observed.carriers++
			continue
		}
		observed.principalIDs = append(observed.principalIDs, principalID)
	}
	return observed
}
