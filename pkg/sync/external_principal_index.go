package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"strings"
	"unicode"

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
	idx.byEmail = make(map[string][]int, len(principals))

	for i, p := range principals {
		userTrait, err := resource.GetUserTrait(p)
		if err != nil {
			// Principals reach this slice because they carry a user-trait
			// annotation, so this only fires on an unreadable annotation.
			//
			// Identifiers only: logging the resource serializes its whole
			// profile, which is where a directory keeps email addresses,
			// employee numbers and the like. The id is what identifies the
			// principal to go and look at.
			l.Error("error getting user trait",
				zap.String("principal_resource_type_id", p.GetId().GetResourceType()),
				zap.String("principal_resource_id", p.GetId().GetResource()),
				zap.Error(err),
			)
			idx.skip[i] = true
			continue
		}
		for _, email := range userTrait.GetEmails() {
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

	// candidates are already exactly the positions bucketed under
	// foldKey(value) -- that's how they got into the bucket during the build
	// above -- so re-fetching and re-comparing each profile value here would
	// only ever reconfirm what bucketing already guarantees. Copy rather than
	// return the bucket slice directly so a caller can't mutate our cached
	// index state.
	candidates := buckets[foldKey(value)]
	matches := make([]int, len(candidates))
	copy(matches, candidates)
	return matches
}

// matchUserTraitEmail returns the ascending positions of user principals
// carrying address among their user-trait emails.
//
// Buckets are keyed by foldKey(address) and looked up the same way, so a bucket
// hit already is the match; there is no separate confirmation pass that could
// disagree with the bucketing.
func (idx *externalPrincipalIndex) matchUserTraitEmail(address string) []int {
	candidates := idx.byEmail[foldKey(address)]
	matches := make([]int, 0, len(candidates))
	for _, i := range candidates {
		if idx.skip[i] {
			continue
		}
		// A principal is bucketed once per matching address, so a user with two
		// addresses that fold alike appears twice -- and always adjacently,
		// since positions are appended in ascending principal order. Collapse
		// the repeat to keep the caller's one-grant-per-principal contract,
		// which the linear scan held by moving to the next principal as soon as
		// it found an email match.
		if len(matches) > 0 && matches[len(matches)-1] == i {
			continue
		}
		matches = append(matches, i)
	}
	return matches
}

// foldKey normalizes a value into a bucket key. Two values land in the same
// bucket exactly when strings.EqualFold reports them equal, which is the
// external-resource match relation pinned by the folding contract tests.
//
// strings.ToLower is deliberately not used. It is a context-free per-rune
// lowercase mapping, not case folding, and the two relations disagree in both
// directions -- each one an access-correctness failure:
//
//   - Medial and final sigma fold together, and long s folds with ordinary s,
//     but their lowercase forms differ. Bucketing on ToLower would separate
//     them, dropping a grant a principal should have.
//   - The dotted capital I lowercases to "i" but does not fold with it.
//     Bucketing on ToLower would merge them, manufacturing a grant a principal
//     should not have.
//
// strings.EqualFold compares rune by rune over unicode.SimpleFold orbits, so
// mapping each rune to its orbit's canonical representative makes bucket
// equality exactly EqualFold equality: equal keys require the same rune count
// and a shared orbit at every position, which is the same test EqualFold makes.
// Bucketing therefore never has to be re-confirmed and can never disagree with
// the contract.
func foldKey(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteRune(foldRune(r))
	}
	return b.String()
}

// foldRune returns the canonical representative of r's simple-case-folding
// orbit: the smallest rune reachable from r via unicode.SimpleFold, which
// cycles through every rune that folds with it. Runes outside any orbit --
// caseless scripts, and the Turkish dotted and dotless I, which deliberately
// fold only with themselves -- are their own representative.
func foldRune(r rune) rune {
	lowest := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if f < lowest {
			lowest = f
		}
	}
	return lowest
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
