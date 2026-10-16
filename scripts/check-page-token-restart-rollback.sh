#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
old_ref=${BATON_ROLLBACK_OLD_REF:-47f5027e75e56872a9325a378badcaaab2718f4a}
scratch=$(mktemp -d)
current_sync_probe=$(mktemp "$repo/pkg/sync/rollback_probe_XXXXXX" --suffix=_test.go)
current_pebble_probe=$(mktemp "$repo/pkg/dotc1z/engine/pebble/rollback_probe_XXXXXX" --suffix=_test.go)
cleanup() {
  rm -f "$current_sync_probe" "$current_pebble_probe"
  git -C "$repo" worktree remove --force "$scratch/old" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT
git -C "$repo" worktree add --quiet --detach "$scratch/old" "$old_ref"
export BATON_ROLLBACK_ARTIFACTS="$scratch/artifacts"
mkdir -p "$BATON_ROLLBACK_ARTIFACTS"

cat > "$current_sync_probe" <<'GO'
package sync

import (
  "os"
  "path/filepath"
  "testing"
  "github.com/stretchr/testify/require"
)

func TestRollbackExportTokens(t *testing.T) {
  for name, action := range map[string]Action{
    "plain": {Op: SyncGrantsOp, PageToken: "opaque"},
    "type-scoped": {Op: SyncGrantsOp, TypeScoped: true, PageToken: "opaque"},
    "restarted": {Op: SyncGrantsOp, PageTokenRestarted: true, PageToken: encodeRestartPageToken(1, restartTestGeneration, "next")},
  } {
    run, stats, _ := newTestRun()
    run.pushAction(t.Context(), action)
    token, err := marshalToken(run, stats)
    require.NoError(t, err)
    require.NoError(t, os.WriteFile(filepath.Join(os.Getenv("BATON_ROLLBACK_ARTIFACTS"), name+".json"), []byte(token), 0600))
  }
}
GO

cat > "$current_pebble_probe" <<'GO'
package pebble

import (
  "os"
  "path/filepath"
  "testing"
  "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
  "github.com/stretchr/testify/require"
)

func TestRollbackExportQueues(t *testing.T) {
  for _, name := range []string{"plain", "restarted", "drained"} {
    dir := filepath.Join(os.Getenv("BATON_ROLLBACK_ARTIFACTS"), name)
    e, err := Open(t.Context(), dir)
    require.NoError(t, err)
    work := pendingTestSeed(t, e)
    if name != "plain" {
      work.PageTokenRestarted = true
      require.NoError(t, pendingTestCommit(t, e, work, "baton-sdk:page-restart:1:1:abcdefghijklmnopqrstuvwxyz:bmV4dA"))
    }
    if name == "drained" {
      items, _, err := e.Ledger().PendingWork(t.Context(), 0, 10)
      require.NoError(t, err)
      expansion := c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "grant-expansion"}}
      require.NoError(t, pendingTestCommit(t, e, items[0], "", expansion))
      require.NoError(t, e.Ledger().BeginExpanding(t.Context()))
    }
    require.NoError(t, os.WriteFile(dir+".sync-id", []byte(work.SyncID), 0600))
    require.NoError(t, e.Close())
  }
}
GO

(cd "$repo" && go test -count=1 -run '^TestRollbackExport' ./pkg/sync ./pkg/dotc1z/engine/pebble)
rm -f "$current_sync_probe" "$current_pebble_probe"

cat > "$scratch/old/pkg/sync/rollback_probe_test.go" <<'GO'
package sync

import (
  "os"
  "path/filepath"
  "testing"
  "github.com/stretchr/testify/require"
)

func TestRollbackOldReaderTokens(t *testing.T) {
  for _, name := range []string{"plain", "type-scoped", "restarted"} {
    t.Run(name, func(t *testing.T) {
      data, err := os.ReadFile(filepath.Join(os.Getenv("BATON_ROLLBACK_ARTIFACTS"), name+".json"))
      require.NoError(t, err)
      if name == "restarted" && os.Getenv("BATON_ROLLBACK_MUTATE") == "1" {
        data = []byte(string(data[:len(data)-2])+"1}")
      }
      parts, err := unmarshalToken(string(data))
      require.NoError(t, err)
      if name == "restarted" {
        require.Empty(t, parts.run.actions, "old reader must discard the recovery action stack")
      } else {
        require.Len(t, parts.run.actions, 1, "old reader must still accept plain and version-2 checkpoints")
        require.Equal(t, "opaque", parts.run.current().PageToken)
      }
    })
  }
}
GO

cat > "$scratch/old/pkg/dotc1z/engine/pebble/rollback_probe_test.go" <<'GO'
package pebble

import (
  "os"
  "path/filepath"
  "testing"
  "github.com/cockroachdb/pebble/v2"
  "github.com/conductorone/baton-sdk/pkg/connectorstore"
  "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
  "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
  "github.com/stretchr/testify/require"
)

func TestRollbackOldReaderQueues(t *testing.T) {
  // BATON_ROLLBACK_MUTATE=1 swaps both declaration versions: the recovered
  // queue drops to 2 and the drained queue keeps 3.
  mutated := map[string]byte{"restarted": 2, "drained": 3}
  for _, name := range []string{"plain", "restarted", "drained"} {
    t.Run(name, func(t *testing.T) {
      dir := filepath.Join(os.Getenv("BATON_ROLLBACK_ARTIFACTS"), name)
      syncID, err := os.ReadFile(dir+".sync-id")
      require.NoError(t, err)
      e, err := Open(t.Context(), dir)
      require.NoError(t, err)
      defer e.Close()
      if version, ok := mutated[name]; ok && os.Getenv("BATON_ROLLBACK_MUTATE") == "1" {
        value, closer, err := e.db.Get(rawdb.LedgerWorkStateKey())
        require.NoError(t, err)
        value = append([]byte(nil), value...)
        require.NoError(t, closer.Close())
        value[0] = version
        batch := e.db.NewRecordBatch()
        require.NoError(t, batch.StageLedgerWorkState(value))
        require.NoError(t, batch.Commit(pebble.Sync))
        batch.Close()
      }
      err = e.SetCurrentSync(t.Context(), string(syncID))
      if name == "restarted" && err != nil {
        require.ErrorContains(t, err, "invalid pending-work state")
      } else {
        require.NoError(t, err)
        work, phase, err := e.Ledger().PendingWork(t.Context(), 0, 10)
        switch name {
        case "restarted":
          require.ErrorContains(t, err, "invalid pending-work state")
          require.Empty(t, work, "old reader must not expose a recovery cursor")
        case "drained":
          require.NoError(t, err, "old reader must resume once only the expansion entry remains")
          require.Equal(t, c1zstore.LedgerQueueExpanding, phase)
          require.Len(t, work, 1)
          require.Equal(t, "grant-expansion", work[0].Action.Identity.Op)
        default:
          require.NoError(t, err)
          require.Len(t, work, 1)
          require.Equal(t, "A", work[0].Action.Identity.PageToken)
        }
      }
      _, err = e.StartNewSync(t.Context(), connectorstore.SyncTypeFull, "")
      if name == "restarted" {
        require.ErrorContains(t, err, "invalid pending-work state", "old reader cannot start over while recovery work is pending")
      } else {
        require.NoError(t, err, "old reader must be able to discard the pass and start a new sync")
      }
    })
  }
}
GO

(cd "$scratch/old" && go test -v -count=1 -run '^TestRollbackOldReader' ./pkg/sync ./pkg/dotc1z/engine/pebble)
