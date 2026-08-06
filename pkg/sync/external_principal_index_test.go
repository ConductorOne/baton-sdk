package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func testUserPrincipal(t *testing.T, objectID string, profile map[string]any, traitOpts ...rs.UserTraitOption) *v2.Resource {
	t.Helper()
	opts := make([]rs.ResourceOption, 0, 1)
	if profile != nil {
		opts = append(opts, rs.WithResourceProfile(profile))
	}
	r, err := rs.NewUserResource(objectID, userResourceType, objectID, traitOpts, opts...)
	require.NoError(t, err)
	return r
}

func testGroupPrincipal(t *testing.T, objectID string, profile map[string]any) *v2.Resource {
	t.Helper()
	r, err := rs.NewGroupResource(objectID, groupResourceType, objectID, nil, rs.WithResourceProfile(profile))
	require.NoError(t, err)
	return r
}

// resolvedIDs maps ascending index positions back to principal resource ids so
// assertions read in terms of principals rather than slice offsets.
func resolvedIDs(idx *externalPrincipalIndex, positions []int) []string {
	ids := make([]string, 0, len(positions))
	for _, i := range positions {
		ids = append(ids, idx.principalAt(i).GetId().GetResource())
	}
	return ids
}

func TestExternalPrincipalIndexMatchProfile(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "user_a", map[string]any{"userPrincipalName": "a@example.com"}),
		testUserPrincipal(t, "user_b", map[string]any{"userPrincipalName": "B@Example.com"}),
		testUserPrincipal(t, "user_c", map[string]any{"other": "a@example.com"}),
		testUserPrincipal(t, "user_d", map[string]any{"userPrincipalName": "a@example.com"}),
	}
	idx := newExternalUserPrincipalIndex(principals, zap.NewNop())

	t.Run("exact match returns every principal with that value, in order", func(t *testing.T) {
		got := idx.matchProfile("userPrincipalName", "a@example.com")
		require.Equal(t, []string{"user_a", "user_d"}, resolvedIDs(idx, got))
	})

	t.Run("match is case insensitive on both sides", func(t *testing.T) {
		got := idx.matchProfile("userPrincipalName", "b@EXAMPLE.COM")
		require.Equal(t, []string{"user_b"}, resolvedIDs(idx, got))
	})

	t.Run("a value present only under a different key does not match", func(t *testing.T) {
		got := idx.matchProfile("userPrincipalName", "nobody@example.com")
		require.Empty(t, got)
	})

	t.Run("an unindexed key matches nothing", func(t *testing.T) {
		got := idx.matchProfile("noSuchKey", "a@example.com")
		require.Empty(t, got)
	})

	t.Run("repeated lookups on a cached key are stable", func(t *testing.T) {
		first := idx.matchProfile("userPrincipalName", "a@example.com")
		second := idx.matchProfile("userPrincipalName", "a@example.com")
		require.Equal(t, resolvedIDs(idx, first), resolvedIDs(idx, second))
	})
}

// GetProfile falls back to the deprecated trait-level profile for data written
// by older connectors, so the index must resolve those principals too.
func TestExternalPrincipalIndexMatchLegacyTraitProfile(t *testing.T) {
	legacy, err := rs.NewUserResource("legacy", userResourceType, "legacy", []rs.UserTraitOption{
		rs.WithUserProfile(map[string]any{"userPrincipalName": "legacy@example.com"}),
	})
	require.NoError(t, err)

	idx := newExternalUserPrincipalIndex([]*v2.Resource{legacy}, zap.NewNop())
	require.Equal(t, []string{"legacy"}, resolvedIDs(idx, idx.matchProfile("userPrincipalName", "legacy@example.com")))
}

func TestExternalPrincipalIndexMatchUserTraitEmail(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "user_a", nil, rs.WithEmail("a@example.com", true)),
		testUserPrincipal(t, "user_b", nil, rs.WithEmail("b@example.com", true), rs.WithEmail("shared@example.com", false)),
		testUserPrincipal(t, "user_c", nil, rs.WithEmail("SHARED@example.com", true)),
	}
	idx := newExternalUserPrincipalIndex(principals, zap.NewNop())

	t.Run("primary address matches", func(t *testing.T) {
		require.Equal(t, []string{"user_a"}, resolvedIDs(idx, idx.matchUserTraitEmail("a@example.com")))
	})

	t.Run("secondary address matches", func(t *testing.T) {
		require.Equal(t, []string{"user_b", "user_c"}, resolvedIDs(idx, idx.matchUserTraitEmail("shared@example.com")))
	})

	t.Run("match is case insensitive", func(t *testing.T) {
		require.Equal(t, []string{"user_a"}, resolvedIDs(idx, idx.matchUserTraitEmail("A@EXAMPLE.COM")))
	})

	t.Run("unknown address matches nothing", func(t *testing.T) {
		require.Empty(t, idx.matchUserTraitEmail("nobody@example.com"))
	})
}

// A user holding two addresses that fold to the same key is bucketed under that
// key twice. It must still yield one position -- the linear scan this replaced
// moved on to the next principal as soon as it found an email match, so it could
// never emit two grants for one user.
func TestExternalPrincipalIndexDuplicateFoldedEmails(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "dupe", nil,
			rs.WithEmail("Dupe@example.com", true),
			rs.WithEmail("DUPE@EXAMPLE.COM", false),
		),
		testUserPrincipal(t, "other", nil, rs.WithEmail("other@example.com", true)),
	}
	idx := newExternalUserPrincipalIndex(principals, zap.NewNop())

	require.Equal(t, []string{"dupe"}, resolvedIDs(idx, idx.matchUserTraitEmail("dupe@example.com")))
}

// The "email" match key considers both a user-trait email address and a profile
// field literally named "email". The union must stay in principal order and must
// not emit a principal twice when it matches both ways.
func TestExternalPrincipalIndexEmailKeyUnion(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "trait_only", nil, rs.WithEmail("target@example.com", true)),
		testUserPrincipal(t, "profile_only", map[string]any{"email": "target@example.com"}),
		testUserPrincipal(t, "both", map[string]any{"email": "target@example.com"}, rs.WithEmail("target@example.com", true)),
		testUserPrincipal(t, "neither", nil, rs.WithEmail("other@example.com", true)),
	}
	idx := newExternalUserPrincipalIndex(principals, zap.NewNop())

	positions := mergePositions(
		idx.matchUserTraitEmail("target@example.com"),
		idx.matchProfile("email", "target@example.com"),
	)
	require.Equal(t, []string{"trait_only", "profile_only", "both"}, resolvedIDs(idx, positions))
}

func TestExternalPrincipalIndexGroupProfile(t *testing.T) {
	principals := []*v2.Resource{
		testGroupPrincipal(t, "group_a", map[string]any{"external_id": "ext_123"}),
		testGroupPrincipal(t, "group_b", map[string]any{"external_id": "EXT_123"}),
		testGroupPrincipal(t, "group_c", map[string]any{"external_id": "ext_999"}),
	}
	idx := newExternalPrincipalIndex(principals)

	require.Equal(t, []string{"group_a", "group_b"}, resolvedIDs(idx, idx.matchProfile("external_id", "ext_123")))
	require.Equal(t, []string{"group_c"}, resolvedIDs(idx, idx.matchProfile("external_id", "ext_999")))
	require.Empty(t, idx.matchProfile("external_id", "ext_000"))
}

// A principal whose user trait cannot be read is excluded from key/value
// matching entirely, including for non-email keys.
func TestExternalPrincipalIndexSkipsUnreadableUserTrait(t *testing.T) {
	readable := testUserPrincipal(t, "readable", map[string]any{"upn": "shared@example.com"})

	// A resource with no user trait annotation stands in for one whose trait
	// cannot be unmarshalled: GetUserTrait fails either way.
	noTrait, err := rs.NewResource("no_trait", userResourceType, "no_trait",
		rs.WithResourceProfile(map[string]any{"upn": "shared@example.com"}))
	require.NoError(t, err)

	idx := newExternalUserPrincipalIndex([]*v2.Resource{readable, noTrait}, zap.NewNop())
	require.True(t, idx.skip[1], "principal with an unreadable user trait should be skipped")
	require.Equal(t, []string{"readable"}, resolvedIDs(idx, idx.matchProfile("upn", "shared@example.com")))
}

// Bucketing and the confirmation pass both normalize through foldKey, so they
// can never disagree about whether two values match. Nothing is restricted to
// ASCII: non-ASCII values are lowercased and compared like any other. A
// mismatch here would mean a real grant is silently dropped -- the candidate
// would be filtered out by bucketing before confirmation ever runs.
func TestExternalPrincipalIndexNonASCIIMatching(t *testing.T) {
	for _, tc := range []struct {
		name          string
		stored, query string
		want          bool
	}{
		{name: "ascii", stored: "UPPER@example.com", query: "upper@example.com", want: true},
		{name: "latin with diaeresis", stored: "Ann-Sofie.Ö", query: "ann-sofie.ö", want: true},
		{name: "cyrillic", stored: "ПЕТРОВ", query: "петров", want: true},
		{name: "greek accented", stored: "ΜΆΙΟΣ", query: "μάιοσ", want: true},
		{name: "cjk is caseless", stored: "山田太郎", query: "山田太郎", want: true},
		// Medial and final sigma fold together even though their lowercase
		// forms differ, so bucketing has to follow the fold, not the lowercase.
		{name: "greek final sigma", stored: "ΣΤΕΦΑΝΟΣ", query: "στεφανος", want: true},
		// The mirror case: the dotted capital I lowercases to "i" but folds
		// only with itself. Bucketing on lowercase would merge these and
		// manufacture a grant, so they must stay in separate buckets.
		{name: "turkish dotted capital i does not fold with i", stored: "İSTANBUL", query: "istanbul", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The index must agree with strings.EqualFold, the relation the
			// folding contract tests pin at the syncer boundary.
			require.Equal(t, tc.want, strings.EqualFold(tc.stored, tc.query),
				"test case expectation must match strings.EqualFold")

			idx := newExternalUserPrincipalIndex([]*v2.Resource{
				testUserPrincipal(t, "user_a", map[string]any{"userPrincipalName": tc.stored}),
			}, zap.NewNop())

			got := resolvedIDs(idx, idx.matchProfile("userPrincipalName", tc.query))
			if tc.want {
				require.Equal(t, []string{"user_a"}, got, "stored %q should match query %q", tc.stored, tc.query)
				return
			}
			require.Empty(t, got, "stored %q must not match query %q", tc.stored, tc.query)
		})
	}
}

// Emails go through the same normalization as profile values, so a non-ASCII
// local part matches case-insensitively the same way.
func TestExternalPrincipalIndexNonASCIIEmail(t *testing.T) {
	idx := newExternalUserPrincipalIndex([]*v2.Resource{
		testUserPrincipal(t, "user_a", nil, rs.WithEmail("ÄNNA@example.com", true)),
	}, zap.NewNop())

	require.Equal(t, []string{"user_a"}, resolvedIDs(idx, idx.matchUserTraitEmail("änna@example.com")))
}

func TestMergePositions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		a, b     []int
		expected []int
	}{
		{name: "both empty", a: nil, b: nil, expected: nil},
		{name: "left empty", a: nil, b: []int{1, 4}, expected: []int{1, 4}},
		{name: "right empty", a: []int{2, 3}, b: nil, expected: []int{2, 3}},
		{name: "interleaved", a: []int{0, 3, 7}, b: []int{1, 2, 9}, expected: []int{0, 1, 2, 3, 7, 9}},
		{name: "fully overlapping deduplicates", a: []int{1, 2}, b: []int{1, 2}, expected: []int{1, 2}},
		{name: "partial overlap deduplicates", a: []int{1, 5, 6}, b: []int{5, 7}, expected: []int{1, 5, 6, 7}},
		{name: "disjoint ranges", a: []int{0, 1}, b: []int{8, 9}, expected: []int{0, 1, 8, 9}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, mergePositions(tc.a, tc.b))
		})
	}
}
