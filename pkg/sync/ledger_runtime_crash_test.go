package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/stretchr/testify/require"
)

type ledgerCrashMarker struct{ Cut, SyncID string }

type ledgerCrashBeforeCommitStore struct {
	c1zstore.PageLedgerStore
	op   string
	exit func()
}

func (s ledgerCrashBeforeCommitStore) BeginPage() c1zstore.PageWriter {
	return ledgerCrashBeforeCommitWriter{PageWriter: s.PageLedgerStore.BeginPage(), op: s.op, exit: s.exit}
}

type ledgerCrashBeforeCommitWriter struct {
	c1zstore.PageWriter
	op   string
	exit func()
}

func (w ledgerCrashBeforeCommitWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if id.Op == w.op {
		w.exit()
	}
	return w.PageWriter.Commit(ctx, id, row)
}

func TestLedgerRuntimeCrashProcess(t *testing.T) {
	cut := os.Getenv("BATON_LEDGER_RUNTIME_CUT")
	id := c1zstore.LedgerActionIdentity{Op: "list-resource-types", PageToken: "first"}
	child := c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "type", PageToken: "child"}, Spawned: true}
	if cut != "" {
		path := os.Getenv("BATON_LEDGER_RUNTIME_FILE")
		require.NotEmpty(t, path)
		f := newLedgerFixtureAt(t, path)
		crash := func() {
			marker, err := json.Marshal(ledgerCrashMarker{Cut: cut, SyncID: f.engine.CurrentSyncID()})
			require.NoError(t, err)
			require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
			os.Exit(74)
		}
		require.NoError(t, f.ledger.InitializePendingWork(t.Context(), []c1zstore.LedgerWork{{Action: c1zstore.LedgerChild{Identity: id}}}))
		work, _, err := f.ledger.PendingWork(t.Context(), 0, 1)
		require.NoError(t, err)
		source := f.ledger
		if cut == "page-staged" {
			source = ledgerCrashBeforeCommitStore{PageLedgerStore: f.ledger, op: id.Op, exit: crash}
		}
		if cut == "terminal-staged" {
			source = ledgerCrashBeforeCommitStore{PageLedgerStore: f.ledger, op: ledgerTerminalOp, exit: crash}
		}
		runtime, err := newTestLedgerRuntime(t.Context(), source, "runtime-attempt")
		require.NoError(t, err)
		f.audit.enter(ledgerHandler)
		_, err = runtime.runPage(t.Context(), 0, id, func(ctx context.Context, page *ledgerPage) error {
			require.NoError(t, page.writer.SetPendingWork(work[0]))
			require.NoError(t, page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()))
			require.NoError(t, page.setFact("runtime-fact"))
			page.observations.Counters = map[string]uint64{"pages": 1}
			if cut == "page-handler" {
				crash()
			}
			return page.transition("next", child)
		})
		require.NoError(t, err)
		f.audit.enter(ledgerLifecycle)
		if cut == "page-committed" {
			crash()
		}
		f.audit.enter(ledgerHandler)
		require.NoError(t, runLedgerSchedulerFixture(t, runtime, []ledgerAction{{identity: id}}, 1, func(ctx context.Context, s *syncer, action *Action, _ *ledgerPage) error {
			return s.nextPageOrFinishAction(ctx, action, "")
		}))
		f.audit.enter(ledgerLifecycle)
		require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{StepDurationsMs: map[string]int64{"run": 17}}))
		if cut == "terminal-committed" {
			crash()
		}
		require.Equal(t, "sealed", cut)
		syncID := f.engine.CurrentSyncID()
		require.NoError(t, runtime.seal(t.Context()))
		marker, err := json.Marshal(ledgerCrashMarker{Cut: cut, SyncID: syncID})
		require.NoError(t, err)
		require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
		os.Exit(74)
	}
	for _, cut := range []string{"page-handler", "page-staged", "page-committed", "terminal-staged", "terminal-committed", "sealed"} {
		t.Run(cut, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runtime.c1z")
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestLedgerRuntimeCrashProcess$")
			cmd.Env = append(os.Environ(), "BATON_LEDGER_RUNTIME_CUT="+cut, "BATON_LEDGER_RUNTIME_FILE="+path)
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			require.ErrorAs(t, err, &exited, string(output))
			require.Equal(t, 74, exited.ExitCode(), string(output))
			markerData, err := os.ReadFile(path + ".cut")
			require.NoError(t, err)
			var marker ledgerCrashMarker
			require.NoError(t, json.Unmarshal(markerData, &marker))
			require.Equal(t, cut, marker.Cut)
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
			types, err := recovered.ListResourceTypes(t.Context(), &v2.ResourceTypesServiceListResourceTypesRequest{})
			require.NoError(t, err)
			_, hasFact := facts["runtime-fact"]
			require.Equal(t, found, hasFact)
			require.Equal(t, found, counters.Counters["pages"] == 1)
			require.Equal(t, found, len(types.GetList()) == 1)
			if cut == "page-handler" || cut == "page-staged" {
				require.False(t, found)
			}
			if found && cut != "sealed" {
				require.Equal(t, "next", row.NextPageToken)
				require.Equal(t, []c1zstore.LedgerChild{child}, row.Children)
			}
			terminal, terminalFound, err := recovered.Ledger().GetRow(t.Context(), c1zstore.LedgerActionIdentity{Op: ledgerTerminalOp})
			require.NoError(t, err)
			_, ready := facts[ledgerFactSealReady]
			require.Equal(t, terminalFound, ready)
			require.Equal(t, terminalFound, counters.StepDurationsMs["run"] == 17)
			if terminalFound {
				require.True(t, found)
				require.NotNil(t, terminal)
			}
			if cut != "terminal-committed" && cut != "sealed" {
				require.False(t, terminalFound)
			}
			record, err := recovered.GetSyncRunRecord(t.Context(), marker.SyncID)
			require.NoError(t, err)
			require.Equal(t, cut == "sealed", record.GetEndedAt() != nil)
			if cut == "sealed" {
				require.True(t, terminalFound)
				require.True(t, row.Scrubbed)
			}
		})
	}
}
