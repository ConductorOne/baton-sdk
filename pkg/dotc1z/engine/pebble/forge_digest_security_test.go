package pebble

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// TestSecurity_ForgedDigestStateRejectedAtImport guards the
// present-means-exact contract against attacker-authored keyspace state.
//
// Finding: dotc1z/pebble.digest-global-root-presence-trust
//
// A hostile .c1z payload can pre-plant grant-digest state in the
// extracted LSM. Policy (reject-not-repair): a .c1z is
// untrusted-at-import — every digest keyspace byte is attacker-authored,
// so ANY advertised-present-but-incorrect state is REJECTED at open
// with c1zstore.ErrDataRejected, before any reader is served. No
// rebuild, no derived output, source bytes unchanged. Wholly absent
// digest state stays the accepted cold case, and engine-owned
// interrupted-build recovery keeps its trusted drop path.
//
// This rewrite replaces the PR's earlier repair-shaped regression
// (drop + honest rebuild at EndSync) — hostile input is never repaired.
func TestSecurity_ForgedDigestStateRejectedAtImport(t *testing.T) {
	ctx := context.Background()

	forgeRoot := func(xor uint64, count int64) []byte {
		require.GreaterOrEqual(t, count, int64(0), "forge helper count must be non-negative")
		val := make([]byte, 0, 16)
		var c [8]byte
		binary.BigEndian.PutUint64(c[:], uint64(count)) // #nosec G115 -- count asserted non-negative immediately above.
		val = append(val, c[:]...)
		var x [8]byte
		binary.BigEndian.PutUint64(x[:], xor)
		return append(val, x[:]...)
	}

	t.Run("zero-partition forged global root", func(t *testing.T) {
		// Global root + current-ABI stamp, NO per-partition roots, no
		// index rows, over a file WITH grants: the grants were never
		// hashed, so presence lies.
		e, dir, syncID := sealedGrantDigestEngine(t, "ent-A", 3)
		_ = syncID
		const forgedXOR = uint64(0x4142434445464748)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeRoot(forgedXOR, 2), pebble.Sync))
		stamp := make([]byte, 4)
		binary.BigEndian.PutUint32(stamp, GrantDigestABIVersion)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), stamp, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorIs(t, err, c1zstore.ErrDataRejected,
			"a forged global root over grants the engine never hashed must reject at import, not seal")
	})

	t.Run("self-consistent forged root pair", func(t *testing.T) {
		// Thread 4170953115: a forged per-entitlement root PLUS a global
		// root equal to its fold is self-consistent attacker bytes —
		// self-consistency proves nothing; the recomputation from
		// primaries must reject it.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		const forgedXOR = uint64(0xdeadbeefdeadbeef)
		partKey := rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-A"))
		require.NoError(t, e.UnsafeForTesting().Set(append(partKey, digestLevelRoot), forgeRoot(forgedXOR, 99), pebble.Sync))
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeRoot(forgedXOR, 99), pebble.Sync))
		stamp := make([]byte, 4)
		binary.BigEndian.PutUint32(stamp, GrantDigestABIVersion)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), stamp, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorIs(t, err, c1zstore.ErrDataRejected)
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
		tampered := forgeRoot(0x0102030405060708, 2)
		require.NoError(t, e.UnsafeForTesting().Set(append(partKey, digestLevelRoot), tampered, pebble.Sync))
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GlobalGrantDigestNodeKey(), forgeRoot(0x0102030405060708, 2), pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorIs(t, err, c1zstore.ErrDataRejected)
	})

	t.Run("malformed ABI stamp bytes", func(t *testing.T) {
		// A stamp value of the wrong length must reject, never silently
		// classify as old ABI (readGrantDigestABIStamp maps malformed
		// to 0 today).
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), []byte{0x01, 0x02}, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorIs(t, err, c1zstore.ErrDataRejected)
	})

	t.Run("phantom root", func(t *testing.T) {
		// A root for a partition with no grants and no entitlement
		// record.
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		phantom := rawdb.DigestPartitionPrefix(rawdb.IdxGrantByEntitlementPrincipalHash, testEntPartition("ent-phantom"))
		require.NoError(t, e.UnsafeForTesting().Set(append(phantom, digestLevelRoot), forgeRoot(0, 0), pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		_, err := Open(ctx, dir)
		require.Error(t, err)
		require.ErrorIs(t, err, c1zstore.ErrDataRejected)
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
		require.ErrorIs(t, err, c1zstore.ErrDataRejected)
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

	t.Run("source bytes unchanged by rejection", func(t *testing.T) {
		// The rejection path must not mutate the on-disk state it
		// rejected: reopenable bytes stay byte-identical (the engine
		// dir is the "source" at this layer; the c1z envelope layer
		// is covered by the dotc1z hostile-trigger suite).
		e, dir, _ := sealedGrantDigestEngine(t, "ent-A", 2)
		require.NoError(t, e.UnsafeForTesting().Set(rawdb.GrantDigestABIStampKey(), []byte{0x01}, pebble.Sync))
		e.db.SetGrantDigestsPresent(true)
		require.NoError(t, e.Close())

		manifest := filepath.Join(dir, "MANIFEST-000001")
		before, err := os.ReadFile(manifest)
		if err == nil {
			_, openErr := Open(ctx, dir)
			require.Error(t, openErr)
			require.ErrorIs(t, openErr, c1zstore.ErrDataRejected)
			after, rerr := os.ReadFile(manifest)
			require.NoError(t, rerr)
			require.Equal(t, string(before), string(after), "rejection must not write to the rejected source")
		}
	})
}
