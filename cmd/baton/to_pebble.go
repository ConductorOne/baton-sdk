package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

const defaultToPebbleBatchSize = 10000

func toPebbleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "to-pebble",
		Short: "Convert a SQLite c1z file to Pebble",
		RunE:  runToPebble,
	}

	cmd.Flags().String("out", "./sync.pebble.c1z", "The path to write the Pebble c1z file to")
	cmd.Flags().String("sync-id", "", "The source sync ID to convert. Defaults to the latest finished full sync.")
	cmd.Flags().Int("batch-size", defaultToPebbleBatchSize, "The number of records to write per conversion batch")
	cmd.Flags().String("tmp-dir", "", "The temporary directory to use while converting")

	return cmd
}

func runToPebble(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("error"))
	if err != nil {
		return err
	}
	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}
	outPath, err := cmd.Flags().GetString("out")
	if err != nil {
		return err
	}
	if outPath == "" {
		return fmt.Errorf("an output path is required")
	}
	syncID, err := cmd.Flags().GetString("sync-id")
	if err != nil {
		return err
	}
	batchSize, err := cmd.Flags().GetInt("batch-size")
	if err != nil {
		return err
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch-size must be greater than zero")
	}
	tmpDir, err := cmd.Flags().GetString("tmp-dir")
	if err != nil {
		return err
	}

	openOpts := []dotc1z.C1ZOption{dotc1z.WithReadOnly(true)}
	if tmpDir != "" {
		openOpts = append(openOpts, dotc1z.WithTmpDir(tmpDir))
	}
	//nolint:staticcheck // ToPebble is a SQLite-only *C1File method not on C1ZStore; NewC1ZFile is required here.
	store, err := dotc1z.NewC1ZFile(ctx, c1zPath, openOpts...)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close(ctx) }()

	convertOpts := []dotc1z.ConvertOption{dotc1z.WithConvertBatchSize(batchSize)}
	if tmpDir != "" {
		convertOpts = append(convertOpts, dotc1z.WithConvertTmpDir(tmpDir))
	}
	stats, err := store.ToPebble(ctx, outPath, syncID, convertOpts...)
	if err != nil {
		return err
	}

	printToPebbleStats(stats, outPath)
	return nil
}

func printToPebbleStats(stats *dotc1z.ConvertStats, outPath string) {
	_, _ = fmt.Fprintf(os.Stdout, "Converted C1Z to Pebble successfully.\n")
	_, _ = fmt.Fprintf(os.Stdout, "Output: %s\n", outPath)
	_, _ = fmt.Fprintf(os.Stdout, "Source sync ID: %s\n", stats.SourceSyncID)
	_, _ = fmt.Fprintf(os.Stdout, "Destination sync ID: %s\n", stats.DestSyncID)
	_, _ = fmt.Fprintf(os.Stdout, "Resource types: %d (%s)\n", stats.ResourceTypes.Rows, formatDuration(stats.ResourceTypes.Duration))
	_, _ = fmt.Fprintf(os.Stdout, "Resources: %d (%s)\n", stats.Resources.Rows, formatDuration(stats.Resources.Duration))
	_, _ = fmt.Fprintf(os.Stdout, "Entitlements: %d (%s)\n", stats.Entitlements.Rows, formatDuration(stats.Entitlements.Duration))
	_, _ = fmt.Fprintf(os.Stdout, "Grants: %d (%s)\n", stats.Grants.Rows, formatDuration(stats.Grants.Duration))
	_, _ = fmt.Fprintf(os.Stdout, "Assets: %d (%d bytes, %s)\n", stats.Assets.Rows, stats.AssetBytes, formatDuration(stats.Assets.Duration))
	_, _ = fmt.Fprintf(os.Stdout, "Total: %s\n", formatDuration(stats.Total))
}

func formatDuration(d time.Duration) string {
	return d.Round(time.Millisecond).String()
}
