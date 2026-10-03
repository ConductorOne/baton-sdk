package pebble

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_GrantSourceFactsHostileValuesTerminate pins the
// hostile-input half of the grant-content-fact scan: attacker-authored
// grant values decide the length AND order of the sources fact list,
// and the digest paths must stay linearithmic in its length and honor
// context cancellation — a descending-ordered list fed the old
// insertion sort (sortGrantSourceFacts, "source sets are tiny"
// unenforced), and a sources map sized by the file burned an hour-plus
// of CPU per row with no ctx check inside a row. Fix posture, same
// policy as the C1Z-SEC-006 scans: reject oversized hostile state with
// the malformed-value error, never burn unbounded CPU on it.
func TestSecurity_GrantSourceFactsHostileValuesTerminate(t *testing.T) {
	t.Run("sortGrantSourceFacts is linearithmic on adversarial order", func(t *testing.T) {
		// Descending distinct keys: the insertion sort's worst case.
		// The cap keeps hostile wire-sized inputs off the digest paths
		// entirely; the largest legal list must still not regress to
		// quadratic behavior. Pre-fix, this subtest times out.
		const n = maxGrantSourceFacts
		srcs := make([]grantSourceFact, 0, n)
		for i := n - 1; i >= 0; i-- {
			srcs = append(srcs, grantSourceFact{key: fmt.Appendf(nil, "src-%06d", i), isDirect: i%7 == 0})
		}
		start := time.Now()
		got := sortGrantSourceFacts(srcs)
		require.Less(t, time.Since(start), 3*time.Second,
			"a descending %d-entry fact list is the insertion-sort worst case; the sort must be linearithmic", n)
		require.Len(t, got, n)
		for i := 1; i < len(got); i++ {
			require.Negative(t, bytes.Compare(got[i-1].key, got[i].key), "must be sorted ascending")
		}
	})

	t.Run("scanGrantContentFactsRawBytes rejects oversized sources maps", func(t *testing.T) {
		// A legal-wire GrantRecord value whose sources map exceeds
		// maxGrantSourceFacts: the cap fires at the raw scan, before
		// any digest path pays per-fact work. The error is returned,
		// not panicked, and names the cap.
		const n = maxGrantSourceFacts + 1
		val := buildHostileGrantRecordValue(t, n)
		_, _, err := scanGrantContentFactsRawBytes(val, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "maxGrantSourceFacts")
	})

	t.Run("BuildGrantDigests terminates fast on hostile oversized-sources row", func(t *testing.T) {
		// BuildGrantDigests deliberately swallows build failures: it
		// drops digest state and returns nil so grant-diff callers
		// re-read grants (digests-absent fail-closed semantics; a
		// hostile row must not wedge the build). The rejection is
		// observable as: the build returns promptly (pre-fix, the
		// sort burned ~9s at only 2^16 facts; hostile wire sized the
		// burn, unbounded) AND the hostile row is absent from the
		// hash index the digest fold consumes.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		// Plant the hostile grant row through the raw DB: a sources
		// map sized by the file is unconstructible through the typed
		// API in practice, and the scan's contract is the stored
		// value, not the typed writer.
		val := buildHostileGrantRecordValue(t, maxGrantSourceFacts+1)
		key := encodeGrantIdentityKey(grantIdentity{
			entitlement:     entitlementIdentityFromParts("app", "github", "app:github:hostile-ent"),
			principalTypeID: "user",
			principalID:     "mallory",
		})
		require.NoError(t, e.db.UnsafeForTesting().Set(key, val, nil))

		require.NoError(t, e.BuildGrantDigests(ctx),
			"build failure must surface as digests-absent, not an error that wedges seal")
		require.False(t, e.db.GrantDigestsPresent(),
			"the hostile row must fail the build; digest state must be dropped, never half-built")
	})

	t.Run("canonical sources still hash identically (control)", func(t *testing.T) {
		// The raw-scan path and the from-record path must agree on
		// the content hash for a canonical multi-source grant — the
		// cap and the sort change may not alter ABI-stable hashes.
		g := v2.Grant_builder{
			Id: "app:github:member:user:alice",
			Entitlement: v2.Entitlement_builder{
				Id: "app:github:member",
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
				}.Build(),
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
			}.Build(),
			Sources: v2.GrantSources_builder{
				Sources: map[string]*v2.GrantSources_GrantSource{
					"src-b": {IsDirect: true},
					"src-a": {IsDirect: false},
					"src-c": {IsDirect: true},
				},
			}.Build(),
		}.Build()
		fromRecord, err := GrantContentHash(g)
		require.NoError(t, err)

		rec := V2GrantToV3("sync", g)
		id, err := grantIdentityFromRecord(rec)
		require.NoError(t, err)
		val, err := marshalRecord(rec)
		require.NoError(t, err)
		isImmutable, srcs, err := scanGrantContentFactsRawBytes(val, nil)
		require.NoError(t, err)
		require.False(t, isImmutable)
		srcs = sortGrantSourceFacts(srcs)
		// The raw path splices the stored primary key's tail exactly
		// as the digest build does (iter.Key()[grantPrimaryKeyPrefixLen:]).
		key := encodeGrantIdentityKey(id)
		rawHash, _ := grantContentHash64(nil, key[grantPrimaryKeyPrefixLen:], isImmutable, srcs)
		require.Equal(t, fromRecord, rawHash,
			"raw-scan and from-record paths must produce the identical content hash")
	})
}

// buildHostileGrantRecordValue marshals a legal-wire GrantRecord whose
// sources map holds n entries, keys ascending.
func buildHostileGrantRecordValue(t *testing.T, n int) []byte {
	t.Helper()
	sources := make(map[string]*v3.GrantSourceRecord, n)
	for i := 0; i < n; i++ {
		sources[fmt.Sprintf("src-%06d", i)] = v3.GrantSourceRecord_builder{IsDirect: true}.Build()
	}
	rec := v3.GrantRecord_builder{
		ExternalId: "hostile-sources-grant",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  "app:github:hostile-ent",
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: "user",
			ResourceId:     "mallory",
		}.Build(),
		Sources: sources,
	}.Build()
	val, err := marshalRecord(rec)
	require.NoError(t, err)
	return val
}
