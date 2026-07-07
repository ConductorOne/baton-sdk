package pebble

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEntitlementIdentityRoundTrip pins the byte-prefix compression
// contract: for ANY (rt, rid, external_id) triple,
// externalID(entitlementIdentityFromParts(...)) reproduces the exact input
// bytes, and stripped/opaque forms cannot collide (bijectivity given the
// resource components). No grammar is involved — colons, backslashes, and
// escape-looking sequences are just bytes.
func TestEntitlementIdentityRoundTrip(t *testing.T) {
	cases := []struct {
		rt, rid, extID string
		wantStripped   bool
	}{
		{"group", "g1", "group:g1:member", true},
		{"group", "g1", "member", false},
		{"group", "g1", "group:g1:", true},                               // empty tail
		{"group", "g1", "group:g1", false},                               // no trailing colon
		{"group", "g1", "group:g1:custom:zeta", true},                    // "custom" marker is just bytes
		{"group", "g1", `group:g1:a\:b`, true},                           // escape-looking bytes
		{"group", "g1", `group:g1:a\\b`, true},                           // double backslash
		{"repo", "arn:aws:s3:::bkt", "repo:arn:aws:s3:::bkt:push", true}, // colon-bearing rid
		{"repo", "arn:aws:s3:::bkt", "repo:arn:aws:x:push", false},       // near-miss prefix
		{"", "", "::x", true},                                            // degenerate empty components
		{"", "", "x", false},                                             //
		{"a", "b", "a:b:c", true},                                        //
		{"a", "b:c", "a:b:c", false},                                     // rid mismatch: prefix would need "a:b:c:"
		{"a:b", "c", "a:b:c:d", true},                                    // rt containing a colon still byte-matches exactly
	}
	for _, tc := range cases {
		id := entitlementIdentityFromParts(tc.rt, tc.rid, tc.extID)
		require.Equal(t, tc.wantStripped, id.stripped, "stripped flag for %q on %s/%s", tc.extID, tc.rt, tc.rid)
		require.Equal(t, tc.extID, id.externalID(), "round trip for %q on %s/%s", tc.extID, tc.rt, tc.rid)
	}
}

// TestEntitlementIdentityRoundTripRandom sweeps randomized component bytes
// (colons, backslashes, NULs, high bytes) through derive → reconstruct and
// asserts byte-exact round trips, plus injectivity: two distinct external
// ids on the same resource never derive to the same identity.
func TestEntitlementIdentityRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data.
	alphabet := []byte(`ab:\` + "\x00\x01\xff")
	randStr := func(maxLen int) string {
		n := rng.Intn(maxLen + 1)
		var b strings.Builder
		for i := 0; i < n; i++ {
			_ = b.WriteByte(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	for i := 0; i < 5000; i++ {
		rt, rid := randStr(4), randStr(6)
		var extID string
		if rng.Intn(2) == 0 {
			extID = rt + ":" + rid + ":" + randStr(8) // prefix-shaped
		} else {
			extID = randStr(10) // arbitrary
		}
		id := entitlementIdentityFromParts(rt, rid, extID)
		require.Equal(t, extID, id.externalID(), "round trip rt=%q rid=%q ext=%q", rt, rid, extID)

		other := entitlementIdentityFromParts(rt, rid, randStr(10))
		if other.externalID() != extID {
			require.NotEqual(t, id, other, "distinct external ids must derive distinct identities")
		}
	}
}
