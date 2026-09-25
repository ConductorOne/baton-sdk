package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/stretchr/testify/require"
)

type ledgerFamilyStore struct {
	c1zstore.PageLedgerStore
	cut   func(c1zstore.LedgerActionIdentity, bool)
	stats c1zstore.SyncStats
}

func (s *ledgerFamilyStore) BeginPage() c1zstore.PageWriter {
	return ledgerFamilyWriter{PageWriter: s.PageLedgerStore.BeginPage(), cut: s.cut}
}
func (s *ledgerFamilyStore) EndSyncWithStats(ctx context.Context, stats c1zstore.SyncStats) error {
	s.stats = stats
	return s.PageLedgerStore.EndSyncWithStats(ctx, stats)
}

type ledgerFamilyWriter struct {
	c1zstore.PageWriter
	cut func(c1zstore.LedgerActionIdentity, bool)
}

func (w ledgerFamilyWriter) Commit(ctx context.Context, id c1zstore.LedgerActionIdentity, row *c1zstore.LedgerRow) error {
	if w.cut != nil {
		w.cut(id, false)
	}
	if err := w.PageWriter.Commit(ctx, id, row); err != nil {
		return err
	}
	if w.cut != nil {
		w.cut(id, true)
	}
	return nil
}

func recoverLedgerFamilyFile(t *testing.T, path string) string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(filepath.Dir(path), "c1z-pebble*", "db"))
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	recovered, err := engine.Open(t.Context(), dirs[0])
	require.NoError(t, err)
	defer func() { require.NoError(t, recovered.Close()) }()
	before := ledgerRawSnapshot(t, recovered)
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, recovered.CheckpointTo(t.Context(), checkpoint))
	manifest, err := engine.BuildManifestWithSyncRuns(t.Context(), recovered, c1zstore.PayloadEncodingTarZstd)
	require.NoError(t, err)
	path = filepath.Join(t.TempDir(), "recovered.c1z")
	out, err := os.Create(path)
	require.NoError(t, err)
	_, err = formatv3.WriteEnvelopeWithReuse(out, manifest, checkpoint, nil)
	require.NoError(t, err)
	require.NoError(t, out.Close())
	f := openLedgerFixtureAt(t, path, false)
	require.Equal(t, before, ledgerRawSnapshot(t, f.engine), "recovery transport must preserve every key/value")
	require.NoError(t, f.store.Close(t.Context()))
	return path
}

type ledgerFamilyResult struct {
	data  []ledgerKV
	stats c1zstore.SyncStats
}

func runLedgerFamilySync(t *testing.T, path string, workers int, cut func(*ledgerFixture, c1zstore.LedgerActionIdentity, bool)) ledgerFamilyResult {
	t.Helper()
	var result ledgerFamilyResult
	synctest.Test(t, func(t *testing.T) {
		f := openLedgerFixtureAt(t, path, false)
		source := &ledgerFamilyStore{PageLedgerStore: f.ledger}
		if cut != nil {
			source.cut = func(id c1zstore.LedgerActionIdentity, after bool) { cut(f, id, after) }
		}
		created, err := NewSyncer(t.Context(), newLedgerFamilyConnector(t), WithConnectorStore(f.store), WithWorkerCount(workers), WithDontExpandGrants())
		require.NoError(t, err)
		s := created.(*syncer)
		s.caps.pageLedger = source
		var walk []ledgerKV
		s.testHooks.ledgerWalk = func(enter bool) {
			if enter {
				walk = ledgerRawSnapshot(t, f.engine)
				f.audit.enter(ledgerWalk)
			} else {
				require.Equal(t, walk, ledgerRawSnapshot(t, f.engine))
				f.audit.enter(ledgerLifecycle)
			}
		}
		require.NoError(t, s.Sync(t.Context()))
		result.stats = source.stats
		result.stats.Run.StepDurationsMs = nil
		for key, value := range result.stats.Run.ConnectorCallStats {
			value.TotalMs = 0
			value.MaxMs = 0
			result.stats.Run.ConnectorCallStats[key] = value
		}
		for key, value := range result.stats.Run.SessionStoreStats {
			value.TotalMs = 0
			value.MaxMs = 0
			result.stats.Run.SessionStoreStats[key] = value
		}
		require.NoError(t, f.store.SetCurrentSync(t.Context(), s.syncID))
		finished, err := f.ledger.BoundSyncFinished(t.Context())
		require.NoError(t, err)
		require.True(t, finished)
		token, err := f.store.CurrentSyncStep(t.Context())
		require.NoError(t, err)
		require.Empty(t, token)
		facts, err := f.ledger.LedgerFacts(t.Context())
		require.NoError(t, err)
		require.Empty(t, facts)
		report, err := f.ledger.GetArchivedLedgerReport(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, report)
		for _, kv := range ledgerRawSnapshot(t, f.engine) {
			if len(kv.key) > 1 && (kv.key[1] >= 1 && kv.key[1] <= 5 || kv.key[1] == 7 || kv.key[1] == 8 || kv.key[1] == 10) {
				result.data = append(result.data, kv)
			}
		}
		require.NoError(t, f.store.Close(t.Context()))
		saved := openLedgerFixtureAt(t, path, false)
		var reopened []ledgerKV
		for _, kv := range ledgerRawSnapshot(t, saved.engine) {
			if len(kv.key) > 1 && (kv.key[1] >= 1 && kv.key[1] <= 5 || kv.key[1] == 7 || kv.key[1] == 8 || kv.key[1] == 10) {
				reopened = append(reopened, kv)
			}
		}
		require.Equal(t, result.data, reopened, "saved artifact must preserve record/index/digest bytes")
		require.NoError(t, saved.store.Close(t.Context()))
		root, err := os.OpenRoot(filepath.Dir(path))
		require.NoError(t, err)
		artifact, err := root.ReadFile(filepath.Base(path))
		require.NoError(t, root.Close())
		require.NoError(t, err)
		t.Logf("artifact SHA-256 %x; equality is logical", sha256.Sum256(artifact))
	})
	return result
}

func TestLedgerPublicFamilyCrashDifferential(t *testing.T) {
	if op := os.Getenv("BATON_LEDGER_FAMILY_OP"); op != "" {
		workers, err := strconv.Atoi(os.Getenv("BATON_LEDGER_FAMILY_WORKERS"))
		require.NoError(t, err)
		path := os.Getenv("BATON_LEDGER_FAMILY_FILE")
		after := os.Getenv("BATON_LEDGER_FAMILY_AFTER") == "true"
		var crash sync.Once
		runLedgerFamilySync(t, path, workers, func(f *ledgerFixture, id c1zstore.LedgerActionIdentity, committed bool) {
			if id.Op != op || id.PageToken != "1" || committed != after {
				return
			}
			crash.Do(func() {
				if os.Getenv("BATON_LEDGER_FAMILY_IMAGE") == "flushed" {
					require.NoError(t, f.engine.Flush(t.Context()))
				}
				marker, err := json.Marshal(id)
				require.NoError(t, err)
				require.NoError(t, writeLedgerTestFile(path+".cut", marker, 0600))
				os.Exit(76)
			})
		})
		t.Fatal("crash cut not reached")
	}
	for _, workers := range []int{1, 4} {
		baseline := runLedgerFamilySync(t, filepath.Join(t.TempDir(), "baseline.c1z"), workers, nil)
		families := make(map[byte]int)
		indexes := make(map[byte]int)
		for _, row := range baseline.data {
			families[row.key[1]]++
			if row.key[1] == 7 {
				indexes[row.key[2]]++
			}
		}
		require.Equal(t, 2, families[1])
		require.Equal(t, 4, families[2])
		require.Equal(t, 12, families[3])
		require.Equal(t, 4, families[4])
		require.Equal(t, 4, indexes[4])
		require.Equal(t, 4, indexes[8])
		require.Positive(t, families[10])
		for _, op := range []ActionOp{SyncResourceTypesOp, SyncResourcesOp, SyncStaticEntitlementsOp, SyncEntitlementsOp, SyncGrantsOp} {
			for _, after := range []bool{false, true} {
				for _, image := range []string{"wal", "flushed"} {
					t.Run(fmt.Sprintf("workers-%d/%s/after-%t/%s", workers, op, after, image), func(t *testing.T) {
						path := filepath.Join(t.TempDir(), "crash.c1z")
						output := runLedgerCrashChild(t, "^TestLedgerPublicFamilyCrashDifferential$", 76, "BATON_LEDGER_FAMILY_OP="+op.String(), "BATON_LEDGER_FAMILY_WORKERS="+strconv.Itoa(workers),
							"BATON_LEDGER_FAMILY_FILE="+path, "BATON_LEDGER_FAMILY_AFTER="+strconv.FormatBool(after), "BATON_LEDGER_FAMILY_IMAGE="+image)
						require.NotContains(t, string(output), "WARNING: DATA RACE")
						marker, err := os.ReadFile(path + ".cut")
						require.NoError(t, err)
						require.NotEmpty(t, marker)
						result := runLedgerFamilySync(t, recoverLedgerFamilyFile(t, path), 5-workers, nil)
						require.Equal(t, baseline.data, result.data)
						require.Equal(t, baseline.stats.Run, result.stats.Run)
						require.Equal(t, baseline.stats.IngestQuality, result.stats.IngestQuality)
					})
				}
			}
		}
	}
}
