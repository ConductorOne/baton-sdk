package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Generator for the version-skew fixture's INPUTS (see
// testdata/README-old-fold.md): two new-format, source-cache-stamped
// sync files that an old-binary compactor is then run over. Env-gated
// and skipped in normal runs; retained so the fixture is regenerable:
//
//	BATON_GEN_SKEW_INPUTS=/tmp/skew-gen go test -run TestGenerateSkewInputs ./pkg/sync/

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/logging"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

func TestGenerateSkewInputs(t *testing.T) {
	outDir := os.Getenv("BATON_GEN_SKEW_INPUTS")
	if outDir == "" {
		t.Skip("set BATON_GEN_SKEW_INPUTS to an output dir")
	}
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(outDir, 0o755)) //nolint:gosec // operator-supplied output dir in an env-gated generator
	tmpDir := t.TempDir()

	group, ent, alice, bob := sourceCacheTestFixtures(t)
	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.AddResource(ctx, bob)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{gt.NewGrant(group, "member", alice)}
	mc.etagByResource[group.GetId().GetResource()] = `W/"skew-v1"`

	base := filepath.Join(outDir, "base.c1z")
	runSourceCacheSync(ctx, t, mc, base, "", tmpDir)

	// Second sync with a changed grant set (new-format, manifest-stamped).
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{
		gt.NewGrant(group, "member", alice),
		gt.NewGrant(group, "member", bob),
	}
	mc.etagByResource[group.GetId().GetResource()] = `W/"skew-v2"`
	newer := filepath.Join(outDir, "newer.c1z")
	runSourceCacheSync(ctx, t, mc, newer, "", tmpDir)
	t.Logf("wrote %s and %s", base, newer)
}
