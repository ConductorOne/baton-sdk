package pebble

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/stretchr/testify/require"

	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// failReadsInjector fails every READ-class vfs op once armed, and counts the
// faults it injected. It is the read-path analog of the sweep's
// failFromInjector (which only fails writes). Implements errorfs.Injector.
type failReadsInjector struct {
	armed    atomic.Bool
	injected atomic.Int64
}

func (f *failReadsInjector) String() string { return "(fail all reads when armed)" }

func (f *failReadsInjector) MaybeError(op errorfs.Op) error {
	if !f.armed.Load() {
		return nil
	}
	if op.Kind.ReadOrWrite() != errorfs.OpIsRead {
		return nil
	}
	f.injected.Add(1)
	return errorfs.ErrInjected
}

func (f *failReadsInjector) arm()    { f.injected.Store(0); f.armed.Store(true) }
func (f *failReadsInjector) disarm() { f.armed.Store(false) }

// === V-12(b): an injected READ IO fault surfaces as an ERROR, never a silent
// zero discovered_at; the injection counter proves the seam actually fired ===
//
// Build a sealed artifact on a MemFS, then cold-reopen it over errorfs with a
// 1-byte block cache (so the grant read must hit the filesystem, not a warm
// cache). Arm the read injector and read a grant through the v3 reader: it must
// return an error, and the injector must report it actually failed a read.
func TestV3ReadIOFaultSurfacesErrorNeverSilentZero(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()

	fs := vfs.NewCrashableMem()

	// --- build + seal the artifact (no faults) ---
	build, err := Open(ctx, "iofault-db", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	_, err = build.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seedGrantWithDiscoveredAt(ctx, t, build, "g-io", "ent-A", "alice", time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC))
	require.NoError(t, build.EndSync(ctx))
	require.NoError(t, build.Close())

	// --- cold reopen over errorfs, tiny cache forces FS reads ---
	inj := &failReadsInjector{}
	efs := errorfs.Wrap(fs, inj)
	cache := pebble.NewCache(1) // 1 byte: effectively no block caching
	defer cache.Unref()

	e, err := Open(ctx, "iofault-db", WithVFS(efs), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err, "reopen (unarmed) must succeed")
	t.Cleanup(func() { _ = e.Close() })

	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok)
	r := provider.V3GrantReader()

	// Arm read faults only now — Open already read the manifest/sstables it
	// needs to bootstrap. The grant lookup below must touch the FS.
	inj.arm()
	resp, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-io"}.Build())
	inj.disarm()

	require.Error(t, err, "an injected read fault must surface as an error, never a silent zero discovered_at")
	if resp != nil {
		require.Nil(t, resp.GetGrant(), "no partial GrantRecord may accompany a read fault")
	}
	require.Positive(t, inj.injected.Load(), "the injection seam must have actually fired (else the assertion is vacuous)")
}
