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

// TestSecurity_ForgedDigestStateRejectedAtImport guards the
// present-means-exact contract against attacker-authored keyspace state.
//
// A hostile .c1z payload can pre-plant grant-digest state in the
// extracted LSM. Digest state stamped with the current ABI is trusted at
// the next EndSync, so state that does not match the grant primaries
// fails the open before any reader is served, and nothing is dropped.
// Absent digest state is the accepted cold case; state under any other
// stamp is untrusted and never rejected
// (TestGrantDigestNonCurrentABIStateUntrustedNotRejected).
func TestSecurity_ForgedDigestStateRejectedAtImport(t *testing.T) {
	ctx := context.Background()

	// Forged values use the production encodings so each case reaches
	// the content check it names, not the malformed-value check.
	digestOf := func(xor uint64) []byte {
		var h [hashLen]byte
		binary.BigEndian.PutUint64(h[:], xor)
		return h[:]
	}
	forgeGlobalRoot := func(xor uint64, count int64) []byte { return packDigestLeaf(count, digestOf(xor)) }
	forgePartitionRoot := func(xor uint64, count int64) []byte { return packDigestRoot(0, count, digestOf(xor)) }

	t.Run("zero-partition forged global root", func(t *testing.T) {
		// Global root + current-ABI stamp, NO per-partition roots, no
		// index rows, over a file WITH grants: the grants were never
		// hashed, so presence lies.
		e, dir, syncID := sealedGrantDigestEngine(t, "ent-A", 3)
		_ = syncID
		const forgedXOR = uint64(0x4142434445464748)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeGlobalRoot(forgedXOR, 2), pebble.Sync))
		stamp := make([]byte, 4)
		binary.BigEndian.PutUint32(stamp, GrantDigestABIVersion)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), stamp, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.ErrorContains(t, err, "pebble: imported",
			"a forged global root over grants the engine never hashed must reject at open, not seal")
	})

	t.Run("self-consistent forged root pair", func(t *testing.T) {
		// A forged per-entitlement root plus a global root equal to its
		// fold is self-consistent; only the recomputation from primaries
		// catches it.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		const forgedXOR = uint64(0xdeadbeefdeadbeef)
		partKey := rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-A"))
		require.NoError(t, e.UnsafeForTesting().Set(append(partKey, digestLevelRoot), forgePartitionRoot(forgedXOR, 99), pebble.Sync))
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeGlobalRoot(forgedXOR, 99), pebble.Sync))
		stamp := make([]byte, 4)
		binary.BigEndian.PutUint32(stamp, GrantDigestABIVersion)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), stamp, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorContains(t, err, "pebble: imported")
	})

	t.Run("all-partitions-covered-but-wrong-hash", func(t *testing.T) {
		// Every grant-bearing partition has A root and the global root
		// folds them — but the root's hash is not the fold of the
		// partition's actual grants.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		// Overwrite the honest root value with a wrong hash, keeping
		// the count and the global root's fold arithmetic intact by
		// also rewriting the global root from the tampered root.
		partKey := rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-A"))
		tampered := forgePartitionRoot(0x0102030405060708, 2)
		require.NoError(t, e.UnsafeForTesting().Set(append(partKey, digestLevelRoot), tampered, pebble.Sync))
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeGlobalRoot(0x0102030405060708, 2), pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorContains(t, err, "pebble: imported")
	})

	t.Run("phantom root", func(t *testing.T) {
		// A root for a partition with no grants and no entitlement
		// record.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		phantom := rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-phantom"))
		require.NoError(t, e.UnsafeForTesting().Set(append(phantom, digestLevelRoot), forgePartitionRoot(0, 0), pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorContains(t, err, "pebble: imported")
	})

	t.Run("missing root for grant-bearing partition", func(t *testing.T) {
		// Delete one honest per-partition root: present-means-exact
		// would fold a partial set as if complete.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		rootKey := append(rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-A")), digestLevelRoot)
		require.NoError(t, e.UnsafeForTesting().Delete(rootKey, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorContains(t, err, "pebble: imported")
	})

	t.Run("honest state accepted (control)", func(t *testing.T) {
		// ACCEPT control: an independently-built honest digest over the
		// same grants must open and serve the exact root.
		e, dir, syncID := sealedGrantDigestEngine(t, "ent-A", 3)
		want, ok, err := e.GetGrantDigestGlobalRoot(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.Close())
		_ = syncID

		e2, err := Open(ctx, dir)
		require.NoError(t, err, "honest sealed digest state must open (validation accepts)")
		defer func() { _ = e2.Close() }()
		got, ok2, err2 := e2.GetGrantDigestGlobalRoot(ctx)
		require.NoError(t, err2)
		require.True(t, ok2)
		require.Equal(t, want.Hash, got.Hash, "validated open must serve the same sealed root")
		require.Equal(t, want.Count, got.Count)
	})

	t.Run("absent digest state accepted cold (control)", func(t *testing.T) {
		// ACCEPT control: no digest state at all is the always-safe
		// cold case.
		e, dir := newTestEngine(t)
		a := NewAdapter(e)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		putEnt(t, e, ctx, "ent-A")
		require.NoError(t, e.PutGrantRecords(ctx, makeTestGrants("ent-A", 2)...))
		require.NoError(t, e.Close())
		// (Never sealed: no digest nodes exist.)

		e2, err := Open(ctx, dir)
		require.NoError(t, err)
		defer func() { _ = e2.Close() }()
		_, ok, err := e2.GetGrantDigestGlobalRoot(ctx)
		require.NoError(t, err)
		require.False(t, ok, "cold state must read as never-built, not reject")
	})

	t.Run("rejection drops nothing", func(t *testing.T) {
		// If the failed open had dropped the forged state, the reopen
		// would accept it as the cold case.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		rootKey := append(rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-A")), digestLevelRoot)
		require.NoError(t, e.UnsafeForTesting().Set(rootKey, forgePartitionRoot(0x0102030405060708, 2), pebble.Sync))
		require.NoError(t, e.Close())

		for range 2 {
			_, err := Open(ctx, dir)
			require.ErrorContains(t, err, "pebble: imported")
		}
	})
}
