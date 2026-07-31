package pebble

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// === V-13 (in-repo direction) + reinforces V-02/V-14: exact discovered_at
// survives write -> seal -> close -> cold reopen ===
//
// The durability oracle: planted raw value == pre-close read == reopened read,
// across a full Close + fresh Open of the same on-disk engine. Distinct-nanos
// and a far-past instant are planted so a truncation/normalization regression
// (e.g. seconds-only, or now()-on-reopen) is caught to the nanosecond.
//
// Full cross-BINARY compatibility (a pinned OLD SDK writing the artifact, or
// the c1-consumer-side deletion contract) is a KNOWN GAP — see evidence.md and
// plan §6.1/§6.2; this test proves the same-build seal/reopen direction only.
func TestV3DiscoveredAtSurvivesEngineReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "engine")

	// Two grants: a distinct-nanos instant and a far-past instant, to catch
	// truncation/normalization on the durable path.
	nanos := time.Date(2020, 6, 15, 8, 30, 0, 123456789, time.UTC)
	farPast := time.Date(1971, 1, 1, 0, 0, 1, 0, time.UTC)

	// --- write + seal + close ---
	e, err := Open(ctx, dir)
	require.NoError(t, err)
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-nanos", "ent-A", "alice", nanos)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-far", "ent-A", "bob", farPast)

	// Pre-close read through the v3 reader (diagnostic cell of the oracle).
	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok)
	preNanos := readV3DiscoveredAt(ctx, t, provider.V3GrantReader(), "g-nanos")
	require.True(t, nanos.Equal(preNanos), "pre-close read must equal planted value")

	require.NoError(t, e.EndSync(ctx))
	require.NoError(t, e.Close())

	// --- cold reopen (fresh Open of the same dir, no SetCurrentSync) ---
	e2, err := Open(ctx, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e2.Close() })
	provider2, ok := any(e2).(connectorstore.V3GrantReaderProvider)
	require.True(t, ok, "reopened engine must still expose V3GrantReaderProvider")
	r2 := provider2.V3GrantReader()

	gotNanos := readV3DiscoveredAt(ctx, t, r2, "g-nanos")
	require.Truef(t, nanos.Equal(gotNanos),
		"reopened discovered_at %s must equal planted %s to the nanosecond (no truncation / no reopen re-stamp)", gotNanos, nanos)
	require.Equal(t, nanos.Nanosecond(), gotNanos.Nanosecond(), "nanos must survive reopen exactly")

	gotFar := readV3DiscoveredAt(ctx, t, r2, "g-far")
	require.Truef(t, farPast.Equal(gotFar), "far-past discovered_at %s must survive reopen as %s", gotFar, farPast)

	// Reopened == pre-close, byte-exact.
	require.True(t, preNanos.Equal(gotNanos), "reopened value must equal the pre-close value exactly")
}

func readV3DiscoveredAt(ctx context.Context, t *testing.T, r connectorstore.V3GrantReader, extID string) time.Time {
	t.Helper()
	resp, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: extID}.Build())
	require.NoError(t, err, "GetGrant(%s)", extID)
	require.NotNil(t, resp.GetGrant(), "GetGrant(%s) returned no record", extID)
	require.NotNil(t, resp.GetGrant().GetDiscoveredAt(), "GetGrant(%s) missing discovered_at", extID)
	return resp.GetGrant().GetDiscoveredAt().AsTime()
}
