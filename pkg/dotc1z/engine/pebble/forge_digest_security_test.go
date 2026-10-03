package pebble

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// TestSecurity_ForgedDigestRootNotSealedAsExact guards the grant-digest
// present-means-exact contract against attacker-authored keyspace state.
//
// Finding: dotc1z/pebble.digest-global-root-presence-trust
//
// A hostile .c1z payload can pre-plant a well-formed global grant-digest
// root (attacker-chosen XOR+count, leaf framing count(8BE)|digest(8)) plus
// a current-ABI stamp in the extracted LSM, with NO per-partition roots or
// hash-index rows. Pre-fix, repairMissingGrantDigestsAttempt's fast path
// trusted the root's mere PRESENCE ("nothing to repair"), so EndSync sealed
// the forged digest and GrantGenerationDigest served attacker-chosen bytes
// over grants the engine never hashed — defeating the documented
// present-means-exact contract (digest.go) that lets consumers skip grant
// reads.
//
// Post-fix, the fast path verifies rather than trusts: the stored global
// root must be a fold of THIS file's stored partition roots; a forged root
// with zero partitions fails the check, the digest state is dropped, and
// the scan-and-repair path rebuilds honestly from primaries. The served
// digest must therefore equal the honest fold over the same grants.
func TestSecurity_ForgedDigestRootNotSealedAsExact(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	e := a.PebbleEngine()
	dir := e.dbDir
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Two real grants via the typed path.
	g1 := scGrant("member", "alice", false)
	g2 := scGrant("member", "bob", false)
	require.NoError(t, a.PutGrants(ctx, g1, g2))

	// FORGED digest state: global root with attacker values (correct leaf
	// framing), ABI stamp naming the current version, and nothing else —
	// no per-partition roots, no hash index.
	forgeRoot := func(xor uint64, count int64) []byte {
		require.GreaterOrEqual(t, count, int64(0), "forge helper count must be non-negative")
		val := make([]byte, 0, 16)
		var c [8]byte
		binary.BigEndian.PutUint64(c[:], uint64(count)) // #nosec G115 -- count asserted non-negative immediately above.
		val = append(val, c[:]...)
		var x [8]byte
		binary.BigEndian.PutUint64(x[:], xor)
		val = append(val, x[:]...)
		return val
	}
	const forgedXOR = uint64(0x4142434445464748)
	require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeRoot(forgedXOR, 2), pebble.Sync))
	stamp := make([]byte, 4)
	binary.BigEndian.PutUint32(stamp, GrantDigestABIVersion)
	require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), stamp, pebble.Sync))
	// Arm the presence gate the way the next Open re-derives it.
	e.db.SetGrantDigestsPresent(true)

	// EndSync runs RepairMissingGrantDigests: the forged root must NOT be
	// trusted as present-means-exact. Post-fix it is either dropped and
	// rebuilt from primaries, or the fold check fails loudly.
	require.NoError(t, a.EndSync(ctx))

	root, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok, "post-fix seal must still produce a digest root (rebuilt from primaries)")

	// Honest oracle: a clean engine over the SAME grants must produce the
	// same digest the sealed file now serves. If the forged XOR survived,
	// present-means-exact is violated by construction.
	a3 := newAdapter(t)
	e3 := a3.PebbleEngine()
	_, err = a3.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a3.PutGrants(ctx, g1, g2))
	require.NoError(t, a3.EndSync(ctx))
	want, okW, err := e3.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, okW)

	require.EqualValues(t, want.Count, root.Count,
		"sealed digest count must match the honest fold over the same grants")
	require.Equal(t, want.Hash, root.Hash,
		"sealed digest must equal the honest fold over the same grants: a surviving forged root means present-means-exact is violated")

	// And the forged bytes specifically must not be what is served.
	var gotXOR uint64
	for _, b := range root.Hash {
		gotXOR = gotXOR<<8 | uint64(b)
	}
	require.NotEqual(t, forgedXOR, gotXOR,
		"attacker-chosen XOR was sealed as the grant generation digest")

	// Persist: the honest (rebuilt) root survives close/reopen.
	require.NoError(t, e.Close())
	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	defer func() { _ = e2.Close() }()
	require.NoError(t, e2.InitCurrentSync(ctx))
	root2, ok2, err2 := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err2)
	require.True(t, ok2)
	require.Equal(t, want.Hash, root2.Hash,
		"rebuilt digest must persist through close/reopen and keep matching the honest fold")
}
