package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func optimizeDb() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "optimize",
		Short:  "Optimize the c1z file. This may result in a reduction in filesize.",
		RunE:   runOptimizeDb,
		Hidden: true,
	}

	return cmd
}

func runOptimizeDb(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("error"))
	if err != nil {
		return err
	}
	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}

	if err := pebble.Register(); err != nil {
		return err
	}
	store, err := dotc1z.NewStore(ctx, c1zPath)
	if err != nil {
		return err
	}
	c1zStore, ok := store.(dotc1z.C1ZStore)
	if !ok {
		_ = store.Close(ctx)
		return fmt.Errorf("store %T does not implement C1ZStore", store)
	}

	if sqliteStore, ok := dotc1z.AsSQLiteStore(c1zStore); ok {
		err = sqliteStore.Vacuum(ctx)
		if err != nil {
			_ = store.Close(ctx)
			return err
		}
		if err = store.Close(ctx); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(os.Stdout, "Optimized C1Z successfully.")
		return nil
	}

	engine := c1zStore.Metadata().Engine
	err = store.Close(ctx)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stdout, "Optimize is not applicable for %s-backed C1Z; no changes made.", engine)
	return nil
}
