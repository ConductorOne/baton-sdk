package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
)

func rollbackExpansionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "rollback-expansion",
		Short:  "Roll back grant expansion in a c1z so it can be replayed",
		Hidden: true,
		RunE:   runRollbackExpansion,
	}
	cmd.Flags().String("sync-id", "", "Sync to roll back. Defaults to the latest finished sync.")
	cmd.Flags().String("out", "", "Path to write the rolled-back c1z to (required for a write; the input file is never modified).")
	cmd.Flags().Bool("dry-run", false, "Report what would change without modifying anything.")
	cmd.Flags().Bool("replay", false, "Re-run grant expansion over the rolled-back c1z (recommended; without it the output is missing its expanded grants).")
	return cmd
}

func runRollbackExpansion(cmd *cobra.Command, _ []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("info"))
	if err != nil {
		return fmt.Errorf("init logging: %w", err)
	}

	inPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}
	syncID, err := cmd.Flags().GetString("sync-id")
	if err != nil {
		return err
	}
	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}
	dryRun, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		return err
	}
	replay, err := cmd.Flags().GetBool("replay")
	if err != nil {
		return err
	}

	if !dryRun && outPath == "" {
		return fmt.Errorf("--out is required to write a rolled-back c1z; the input is never modified — pass --dry-run to preview")
	}

	if syncID == "" {
		syncID, err = resolveLatestFinishedSync(ctx, inPath)
		if err != nil {
			return err
		}
	}

	if dryRun {
		ro, err := dotc1z.NewC1ZFile(ctx, inPath, dotc1z.WithReadOnly(true))
		if err != nil {
			return fmt.Errorf("open c1z: %w", err)
		}
		defer ro.Close(ctx)
		res, err := ro.RollbackExpansion(ctx, syncID, true)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "dry-run: sync %s — would delete %d expansion-derived grant(s)%s\n",
			res.SyncID, res.DerivedDeleted, replaySuffix(replay))
		return nil
	}

	// Writes go to a fresh clone of the targeted sync; the input is never
	// touched. CloneSync refuses an existing --out and copies only the
	// targeted sync, not every sync in the source.
	src, err := dotc1z.NewC1ZFile(ctx, inPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return fmt.Errorf("open c1z: %w", err)
	}
	cloneErr := src.CloneSync(ctx, outPath, syncID)
	_ = src.Close(ctx)
	if cloneErr != nil {
		return fmt.Errorf("clone to --out: %w", cloneErr)
	}

	store, err := dotc1z.NewC1ZFile(ctx, outPath)
	if err != nil {
		return fmt.Errorf("open --out c1z: %w", err)
	}
	finalized := false
	defer func() {
		if !finalized {
			_ = store.Close(ctx)
		}
	}()

	res, err := store.RollbackExpansion(ctx, syncID, false)
	if err != nil {
		return err
	}

	if replay {
		if err := replayExpansion(ctx, store, syncID); err != nil {
			return fmt.Errorf("replay expansion: %w", err)
		}
	} else {
		_, _ = fmt.Fprintln(os.Stderr,
			"warning: --replay not set; the output c1z has its expansion rolled back and is NOT re-expanded — its expanded grants are absent until expansion runs again")
	}

	finalized = true
	if err := store.Close(ctx); err != nil {
		return fmt.Errorf("finalize %q: %w", outPath, err)
	}

	_, _ = fmt.Fprintf(os.Stdout, "sync %s rolled back: deleted %d expansion-derived grant(s); wrote %s%s\n",
		res.SyncID, res.DerivedDeleted, outPath, replaySuffix(replay))
	return nil
}

func resolveLatestFinishedSync(ctx context.Context, inPath string) (string, error) {
	ro, err := dotc1z.NewC1ZFile(ctx, inPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return "", fmt.Errorf("open c1z: %w", err)
	}
	defer ro.Close(ctx)
	syncID, err := ro.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return "", fmt.Errorf("resolve latest finished sync: %w", err)
	}
	if syncID == "" {
		return "", fmt.Errorf("no finished sync found in %q; pass --sync-id", inPath)
	}
	return syncID, nil
}

// replayExpansion re-runs grant expansion over an already-rolled-back
// c1z through the public syncer path, exactly as the compactor does
// (an empty connector + the existing store + only-expand-grants). This
// runs the full production expansion — graph load, cycle fixing
// (honoring BATON_DONT_FIX_CYCLES), and the expander — so a replay
// matches a fresh sync rather than a hand-rolled subset of it.
func replayExpansion(ctx context.Context, store *dotc1z.C1File, syncID string) error {
	tmpDir, err := os.MkdirTemp("", "baton-rollback-replay")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	emptyConnector, err := sdk.NewEmptyConnector()
	if err != nil {
		return fmt.Errorf("create empty connector: %w", err)
	}

	syncer, err := sync.NewSyncer(ctx, emptyConnector,
		sync.WithConnectorStore(store),
		sync.WithSyncID(syncID),
		sync.WithOnlyExpandGrants(),
		sync.WithTmpDir(tmpDir),
	)
	if err != nil {
		return fmt.Errorf("create syncer: %w", err)
	}
	return syncer.Sync(ctx)
}

func replaySuffix(replay bool) string {
	if replay {
		return "; replayed expansion"
	}
	return ""
}
