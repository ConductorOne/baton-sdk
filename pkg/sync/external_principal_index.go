package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

// externalPrincipalIndex answers "which external principals match this
// key/value?" without rescanning every principal.
//
// External-resource grant matching used to walk the whole principal slice for
// every matched grant, unmarshalling that principal's user trait and profile
// annotation on each visit. That is O(grants x principals) proto unmarshals.
// On a tenant with ~99k externally-matched user grants and ~93k external
// principals the step ran for over five hours without finishing, so the sync
// activity was killed on its deadline and every attempt redid the same work.
//
// Indexing the principals once turns each grant into a map lookup, making the
// step O(grants + principals). Profiles and user traits are unmarshalled
// exactly once per principal, in the constructor.
//
// Profile-key buckets are built lazily: connectors reference only a handful of
// distinct match keys, so there is no reason to index every profile field of
// every principal up front.
//
// An index instance is scoped to one trait's principals (users or groups), so
// lookups can never return a principal of the wrong trait.
type externalPrincipalIndex struct {
	// principals is the principal set in discovery order. Lookups return
	// ascending positions into this slice, so callers iterate matches in the
	// same order the previous linear scan did.
	principals []*v2.Resource

	// profiles[i] is the resolved profile for principals[i], or nil.
	profiles []*structpb.Struct

	// emails[i] holds the user-trait email addresses for principals[i]. It is
	// only populated by newExternalUserPrincipalIndex.
	emails [][]*v2.UserTrait_Email

	// skip[i] excludes principals[i] from all key/value matching. The previous
	// linear scan skipped a user principal outright when its user trait failed
	// to unmarshal, including for non-email keys; this preserves that.
	skip []bool

	// byProfileKey maps a profile key to foldKey(value) -> ascending positions.
	byProfileKey map[string]map[string][]int

	// byEmail maps foldKey(address) -> ascending positions.
	byEmail map[string][]int
}

// newExternalPrincipalIndex indexes principals that are matched on their
// profile only. Used for group (and other non-user trait) principals.
func newExternalPrincipalIndex(principals []*v2.Resource) *externalPrincipalIndex {
	idx := &externalPrincipalIndex{
		principals:   principals,
		profiles:     make([]*structpb.Struct, len(principals)),
		skip:         make([]bool, len(principals)),
		byProfileKey: make(map[string]map[string][]int),
	}
	for i, p := range principals {
		idx.profiles[i] = resource.GetProfile(p)
	}
	return idx
}

// newExternalUserPrincipalIndex indexes user principals, which can be matched
// on either a user-trait email address or a profile field. Each principal's
// user trait is unmarshalled once here rather than once per candidate grant.
func newExternalUserPrincipalIndex(principals []*v2.Resource, l *zap.Logger) *externalPrincipalIndex {
	idx := newExternalPrincipalIndex(principals)
	idx.emails = make([][]*v2.UserTrait_Email, len(principals))
	idx.byEmail = make(map[string][]int, len(principals))

	for i, p := range principals {
		userTrait, err := resource.GetUserTrait(p)
		if err != nil {
			// Principals reach this slice because they carry a user-trait
			// annotation, so this only fires on an unreadable annotation.
			l.Error("error getting user trait", zap.Any("userPrincipal", p))
			idx.skip[i] = true
			continue
		}
		idx.emails[i] = userTrait.GetEmails()
		for _, email := range idx.emails[i] {
			key := foldKey(email.GetAddress())
			idx.byEmail[key] = append(idx.byEmail[key], i)
		}
	}
	return idx
}

// principalAt returns the principal at an ascending position returned by one of
// the lookup methods.
func (idx *externalPrincipalIndex) principalAt(i int) *v2.Resource {
	return idx.principals[i]
}

// matchProfile returns the ascending positions of principals whose profile
// value for key is case-insensitively equal to value.
func (idx *externalPrincipalIndex) matchProfile(key, value string) []int {
	buckets, ok := idx.byProfileKey[key]
	if !ok {
		buckets = make(map[string][]int)
		for i, profile := range idx.profiles {
			if idx.skip[i] {
				continue
			}
			v, ok := resource.GetProfileStringValue(profile, key)
			if !ok {
				continue
			}
			bucket := foldKey(v)
			buckets[bucket] = append(buckets[bucket], i)
		}
		idx.byProfileKey[key] = buckets
	}

	candidates := buckets[foldKey(value)]
	matches := make([]int, 0, len(candidates))
	for _, i := range candidates {
		// Re-confirm with EqualFold: the bucket is a prefilter, so a bucket
		// collision can never manufacture a match the linear scan would not
		// have made.
		if v, ok := resource.GetProfileStringValue(idx.profiles[i], key); ok && strings.EqualFold(v, value) {
			matches = append(matches, i)
		}
	}
	return matches
}

// matchUserTraitEmail returns the ascending positions of user principals
// carrying address among their user-trait emails.
func (idx *externalPrincipalIndex) matchUserTraitEmail(address string) []int {
	candidates := idx.byEmail[foldKey(address)]
	matches := make([]int, 0, len(candidates))
	for _, i := range candidates {
		if idx.skip[i] {
			continue
		}
		// Re-confirm for the same reason as matchProfile.
		if userTraitContainsEmail(idx.emails[i], address) {
			matches = append(matches, i)
		}
	}
	return matches
}

// foldKey normalizes a value into a bucket key.
//
// Buckets are only a prefilter — every lookup re-confirms candidates with
// strings.EqualFold — so bucketing can only ever narrow the candidate set. It
// assumes case-fold-equal values share a lowercase form, which holds for the
// ASCII identifiers used as external match values (emails, user principal
// names, login names).
func foldKey(s string) string {
	return strings.ToLower(s)
}

// mergePositions returns the ascending, deduplicated union of two ascending
// position slices.
func mergePositions(a, b []int) []int {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	merged := make([]int, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			merged = append(merged, a[i])
			i++
		case a[i] > b[j]:
			merged = append(merged, b[j])
			j++
		default:
			merged = append(merged, a[i])
			i++
			j++
		}
	}
	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)
	return merged
}
