package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
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
	cmd.Flags().Bool("replay", false, "Re-run grant expansion over the rolled-back c1z.")
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
		_, _ = fmt.Fprintf(os.Stdout, "dry-run: sync %s — would delete %d derived grant(s) and clear sources on %d grant(s)%s\n",
			res.SyncID, res.DerivedDeleted, res.SourcesCleared, replaySuffix(replay))
		return nil
	}

	// Writes always go to a fresh copy; the input file is left untouched.
	if err := copyFile(inPath, outPath); err != nil {
		return fmt.Errorf("prepare --out: %w", err)
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
	}

	finalized = true
	if err := store.Close(ctx); err != nil {
		return fmt.Errorf("finalize %q: %w", outPath, err)
	}

	_, _ = fmt.Fprintf(os.Stdout, "sync %s rolled back: deleted %d derived grant(s), cleared sources on %d grant(s); wrote %s%s\n",
		res.SyncID, res.DerivedDeleted, res.SourcesCleared, outPath, replaySuffix(replay))
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

// replayExpansion re-runs the full three-phase expansion over an
// already-loaded c1z: rebuild the entitlement graph from the expandable
// grants, fix cycles, then expand. It mirrors the syncer's
// SyncGrantExpansion path (load graph → FixCyclesFromComponents →
// Expander) so a replay matches what a fresh sync would produce,
// including on cyclic entitlement graphs. *C1File satisfies
// expand.ExpanderStore, so no connector is required.
func replayExpansion(ctx context.Context, store *dotc1z.C1File, syncID string) error {
	if err := store.SetCurrentSync(ctx, syncID); err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)
	for def, err := range store.Grants().PendingExpansion(ctx) {
		if err != nil {
			return err
		}
		dstEntitlementID := def.TargetEntitlementID
		for _, srcEntitlementID := range def.Annotation.GetEntitlementIds() {
			// The source entitlement's resource must match the grant's
			// principal, mirroring the sync-path graph load; skip an edge
			// whose source entitlement no longer exists.
			srcEnt, err := store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: srcEntitlementID,
			}.Build())
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return err
			}
			rid := srcEnt.GetEntitlement().GetResource().GetId()
			if rid == nil ||
				def.PrincipalResourceTypeID != rid.GetResourceType() ||
				def.PrincipalResourceID != rid.GetResource() {
				continue
			}
			graph.AddEntitlementID(dstEntitlementID)
			graph.AddEntitlementID(srcEntitlementID)
			if err := graph.AddEdge(ctx, srcEntitlementID, dstEntitlementID, def.Annotation.GetShallow(), def.Annotation.GetResourceTypeIds()); err != nil {
				return fmt.Errorf("add edge: %w", err)
			}
		}
	}
	graph.Loaded = true

	comps, _ := graph.ComputeCyclicComponents(ctx)
	if len(comps) == 0 {
		graph.HasNoCycles = true
	} else if err := graph.FixCyclesFromComponents(ctx, comps); err != nil {
		return fmt.Errorf("fix cycles: %w", err)
	}

	return expand.NewExpander(store, graph).Run(ctx)
}

func replaySuffix(replay bool) string {
	if replay {
		return "; replayed expansion"
	}
	return ""
}

func copyFile(src, dst string) error {
	if _, err := os.Stat(dst); err == nil {
		return fmt.Errorf("--out path %q already exists; refusing to overwrite", dst)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
