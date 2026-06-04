package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/sync"
)

type rollbackExpansionStore interface {
	dotc1z.C1ZStore
	RollbackExpansion(ctx context.Context, syncID string, dryRun bool, opts ...dotc1z.RollbackOption) (*dotc1z.RollbackResult, error)
	GrantSourcesForSync(ctx context.Context, syncID string) (map[string]string, error)
}

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
	cmd.Flags().Bool("preserve-suspect-grants", false, "Keep suspect connector-sourced grants instead of deleting them; avoids dropping real connector data (default: delete).")
	cmd.Flags().Bool("validate", false, "After --replay, fail if any grant's Sources differ before vs after. Requires --replay; default off (default rollback clears connector-set Sources).")
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
	preserveSuspect, err := cmd.Flags().GetBool("preserve-suspect-grants")
	if err != nil {
		return err
	}
	validate, err := cmd.Flags().GetBool("validate")
	if err != nil {
		return err
	}
	if validate && !replay {
		return fmt.Errorf("--validate requires --replay (without re-deriving, deleted grants cannot be compared)")
	}
	var rollbackOpts []dotc1z.RollbackOption
	if preserveSuspect {
		rollbackOpts = append(rollbackOpts, dotc1z.WithPreserveSuspectGrants())
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
		ro, err := openReadOnlyC1ZStore(ctx, inPath)
		if err != nil {
			return fmt.Errorf("open c1z: %w", err)
		}
		defer ro.Close(ctx)
		rollbackStore, ok := ro.(rollbackExpansionStore)
		if !ok {
			return fmt.Errorf("rollback-expansion is not supported by %s-backed c1z files", ro.Metadata().Engine)
		}
		res, err := rollbackStore.RollbackExpansion(ctx, syncID, true, rollbackOpts...)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "dry-run: sync %s — would delete %d expansion-derived grant(s) and clear sources on %d direct grant(s)%s%s\n",
			res.SyncID, res.GrantsDeleted, res.SourcesCleared, suspectSuffix(res), replaySuffix(replay))
		return nil
	}

	// Writes go to a fresh clone of the targeted sync; the input is never
	// touched. CloneSync refuses an existing --out and copies only the
	// targeted sync, not every sync in the source.
	src, err := openReadOnlyC1ZStore(ctx, inPath)
	if err != nil {
		return fmt.Errorf("open c1z: %w", err)
	}
	srcRollbackStore, ok := src.(rollbackExpansionStore)
	if !ok {
		_ = src.Close(ctx)
		return fmt.Errorf("rollback-expansion is not supported by %s-backed c1z files", src.Metadata().Engine)
	}
	// Capture each grant's Sources BEFORE rollback so the replay round trip
	// can be validated: replay re-derives exactly what rollback removed, so
	// every grant's Sources must be identical before and after. Only
	// meaningful on the replay path (without replay the grants are deleted,
	// not re-derived).
	var preSources map[string]string
	if validate {
		preSources, err = srcRollbackStore.GrantSourcesForSync(ctx, syncID)
		if err != nil {
			_ = src.Close(ctx)
			return fmt.Errorf("capture pre-rollback grant sources: %w", err)
		}
	}
	cloneErr := src.FileOps().CloneSync(ctx, outPath, syncID)
	_ = src.Close(ctx)
	if cloneErr != nil {
		return fmt.Errorf("clone to --out: %w", cloneErr)
	}

	// CloneSync has already written outPath. If anything downstream
	// fails, remove the partial output so an identical retry isn't
	// blocked by CloneSync refusing an existing path. Cleared once the
	// output is finalized successfully.
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.Remove(outPath)
		}
	}()

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

	res, err := store.RollbackExpansion(ctx, syncID, false, rollbackOpts...)
	if err != nil {
		return err
	}

	if replay {
		if err := replayExpansion(ctx, store, syncID); err != nil {
			return fmt.Errorf("replay expansion: %w", err)
		}
		// Validate the round trip when asked: every grant's Sources must be
		// identical before rollback and after replay. A divergence means the
		// replay did not faithfully reproduce the original expansion — fail
		// without finalizing so the partial output is removed.
		if validate {
			postSources, err := store.GrantSourcesForSync(ctx, syncID)
			if err != nil {
				return fmt.Errorf("capture post-replay grant sources: %w", err)
			}
			if diffs := compareGrantSources(preSources, postSources); len(diffs) > 0 {
				return fmt.Errorf("rollback+replay changed grant sources (%d divergence(s)); output not finalized:\n%s",
					len(diffs), strings.Join(capLines(diffs, 20), "\n"))
			}
			_, _ = fmt.Fprintf(os.Stderr, "validation: %d grant source set(s) identical before and after replay\n", len(postSources))
		}
	} else {
		_, _ = fmt.Fprintln(os.Stderr,
			"warning: --replay not set; the output c1z has its expansion rolled back and is NOT re-expanded — its expanded grants are absent until expansion runs again")
	}

	finalized = true
	if err := store.Close(ctx); err != nil {
		return fmt.Errorf("finalize %q: %w", outPath, err)
	}
	succeeded = true

	_, _ = fmt.Fprintf(os.Stdout, "sync %s rolled back: deleted %d expansion-derived grant(s), cleared sources on %d direct grant(s)%s; wrote %s%s\n",
		res.SyncID, res.GrantsDeleted, res.SourcesCleared, suspectSuffix(res), outPath, replaySuffix(replay))
	return nil
}

// compareGrantSources reports every grant whose Sources differ between the
// pre-rollback and post-replay snapshots: dropped (present before, gone
// after), added (present after, not before), or changed. An empty result
// means the round trip faithfully reproduced every grant's Sources.
func compareGrantSources(before, after map[string]string) []string {
	var diffs []string
	for id, b := range before {
		a, ok := after[id]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("grant %q: present before, absent after replay", id))
			continue
		}
		if a != b {
			diffs = append(diffs, fmt.Sprintf("grant %q: sources changed before=[%s] after=[%s]", id, b, a))
		}
	}
	for id := range after {
		if _, ok := before[id]; !ok {
			diffs = append(diffs, fmt.Sprintf("grant %q: present after replay, absent before", id))
		}
	}
	sort.Strings(diffs)
	return diffs
}

// capLines returns at most n lines, appending a "+K more" marker when
// truncated, so a divergence error stays readable.
func capLines(lines []string, n int) []string {
	if len(lines) <= n {
		return lines
	}
	out := append([]string{}, lines[:n]...)
	return append(out, fmt.Sprintf("  ... +%d more", len(lines)-n))
}

func resolveLatestFinishedSync(ctx context.Context, inPath string) (string, error) {
	ro, err := openReadOnlyC1ZStore(ctx, inPath)
	if err != nil {
		return "", fmt.Errorf("open c1z: %w", err)
	}
	defer ro.Close(ctx)
	syncID, err := latestSyncID(ctx, ro, connectorstore.SyncTypeAny)
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

func suspectSuffix(res *dotc1z.RollbackResult) string {
	n := res.SuspectConnectorSourced
	if n == 0 {
		return ""
	}
	if res.SuspectPreserved > 0 {
		return fmt.Sprintf(" [%d grant(s) had Sources but no self-source and no expander marker — PRESERVED (not deleted); verify whether they are connector-set]", res.SuspectPreserved)
	}
	return fmt.Sprintf(" [%d deleted grant(s) carried Sources but no self-source and no expander marker — verify they are not connector-set; pass --preserve-suspect-grants to keep them]", n)
}
