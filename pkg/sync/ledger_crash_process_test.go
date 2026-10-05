package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

func TestLedgerCrashProcess(t *testing.T) {
	cut := os.Getenv("BATON_LEDGER_CRASH_CUT")
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types", PageToken: "crash-page"}
	if cut != "" {
		path := os.Getenv("BATON_LEDGER_CRASH_FILE")
		require.NotEmpty(t, path)
		f := newLedgerFixtureAt(t, path)
		f.audit.enter(ledgerHandler)
		ctx := c1zstore.WithOpenPage(t.Context())
		page := f.ledger.BeginPage()
		require.NoError(t, page.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "crash-type"}.Build()))
		require.NoError(t, page.PutAsset(ctx, v2.AssetRef_builder{Id: "crash-asset"}.Build(), "image/png", []byte("asset bytes")))
		require.NoError(t, page.SetFact("crash-fact"))
		require.NoError(t, page.SetCounterBucket("crash-run", 0, c1zstore.LedgerCounters{Counters: map[string]uint64{"pages": 1}}))
		if cut == "committed" {
			require.NoError(t, page.Commit(ctx, id, &c1zstore.LedgerRow{Identity: id}))
		}
		require.Contains(t, []string{"staged", "committed"}, cut)
		require.NoError(t, writeLedgerTestFile(path+".cut", []byte(cut), 0600))
		os.Exit(73)
	}
	for _, cut := range []string{"staged", "committed"} {
		t.Run(cut, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "crash.c1z")
			runLedgerCrashChild(t, "^TestLedgerCrashProcess$", 73, "BATON_LEDGER_CRASH_CUT="+cut, "BATON_LEDGER_CRASH_FILE="+path)
			marker, err := os.ReadFile(path + ".cut")
			require.NoError(t, err)
			require.Equal(t, cut, string(marker))
			dirs, err := filepath.Glob(filepath.Join(filepath.Dir(path), "c1z-pebble*", "db"))
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			recovered, err := engine.Open(t.Context(), dirs[0])
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, recovered.Close()) })
			row, found, err := recovered.Ledger().GetRow(t.Context(), id)
			require.NoError(t, err)
			facts, err := recovered.Ledger().Facts(t.Context())
			require.NoError(t, err)
			counters, err := recovered.Ledger().Counters(t.Context())
			require.NoError(t, err)
			types, err := recovered.ListResourceTypes(context.Background(), &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			_, hasFact := facts["crash-fact"]
			require.Equal(t, found, hasFact)
			require.Equal(t, found, counters.Counters["pages"] == 1)
			require.Equal(t, found, len(types.GetList()) == 1)
			asset, err := recovered.GetAssetRecord(t.Context(), "crash-asset")
			if found {
				require.NoError(t, err)
				require.Equal(t, []byte("asset bytes"), asset.GetData())
				require.Equal(t, "image/png", asset.GetContentType())
			} else {
				require.ErrorIs(t, err, pebble.ErrNotFound)
			}
			if cut == "staged" {
				require.False(t, found)
			}
			if found {
				require.Equal(t, id, row.Identity)
			}
		})
	}
}

func runLedgerCrashChild(t *testing.T, pattern string, exitCode int, env ...string) []byte {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable, "-test.run="+pattern)
	command.Env = append(os.Environ(), env...)
	output, err := command.CombinedOutput()
	var exited *exec.ExitError
	require.ErrorAs(t, err, &exited, string(output))
	require.Equal(t, exitCode, exited.ExitCode(), string(output))
	return output
}
