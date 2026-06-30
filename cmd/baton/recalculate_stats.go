package main

import (
	"context"
	"time"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func recalculateStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recalculate-stats",
		Short: "Recalculate and persist the cached stats for a c1z",
		Long: "Recalculate the cached stats for every finished sync in the c1z. " +
			"Pass --sync-id to recalculate a single sync instead.",
		RunE: runRecalculateStats,
	}

	cmd.Flags().String("sync-id", "", "Only recalculate stats for this sync ID. Defaults to all finished syncs.")

	return cmd
}

func runRecalculateStats(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("error"))
	if err != nil {
		return err
	}

	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}

	syncID, err := cmd.Flags().GetString("sync-id")
	if err != nil {
		return err
	}

	// Stats are persisted on the underlying store, so we need a writable c1z.
	store, err := dotc1z.OpenStore(ctx, c1zPath)
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	syncIDs, err := syncIDsToRecalculate(ctx, store, syncID)
	if err != nil {
		return err
	}

	if len(syncIDs) == 0 {
		pterm.Info.Println("No finished syncs found to recalculate")
		return nil
	}

	for _, id := range syncIDs {
		start := time.Now()
		if err := store.SyncMeta().RecalculateStats(ctx, id); err != nil {
			return err
		}
		pterm.Success.Printfln("Recalculated stats for sync %s duration: %s", id, time.Since(start))
	}

	pterm.Info.Printfln("Recalculated stats for %d sync(s)", len(syncIDs))

	return nil
}

// syncIDsToRecalculate resolves the set of syncs to recalculate. When syncID
// is set, it is the only sync returned. Otherwise every finished sync in the
// c1z is returned — stats are only meaningful for finished syncs, so
// in-progress runs are skipped.
func syncIDsToRecalculate(ctx context.Context, store dotc1z.C1ZStore, syncID string) ([]string, error) {
	if syncID != "" {
		// Validate the sync exists so a typo'd id fails loudly instead of
		// silently recomputing nothing (the Pebble engine would otherwise
		// happily rewrite its sidecar regardless of the id).
		if _, err := store.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
			SyncId: syncID,
		}.Build()); err != nil {
			return nil, err
		}
		return []string{syncID}, nil
	}

	var ids []string
	pageToken := ""
	for {
		resp, err := store.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
			PageSize:  100,
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, run := range resp.GetSyncs() {
			if run.GetEndedAt() == nil {
				continue
			}
			ids = append(ids, run.GetId())
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return ids, nil
}
