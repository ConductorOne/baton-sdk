package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/conductorone/baton-sdk/pkg/exit"
	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cliCmd := &cobra.Command{
		Use:     "baton",
		Short:   "baton is a utility for working with the output of a baton-based connector",
		Version: version,
	}

	cliCmd.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to work with.")
	cliCmd.PersistentFlags().StringP("output-format", "o", "console", "The format to output results in: (console, json)")

	cliCmd.AddCommand(resourcesCmd())
	cliCmd.AddCommand(resourceTypesCmd())
	cliCmd.AddCommand(entitlementsCmd())
	cliCmd.AddCommand(grantsCmd())
	cliCmd.AddCommand(statsCmd())
	cliCmd.AddCommand(recalculateStatsCmd())
	cliCmd.AddCommand(diffCmd())
	cliCmd.AddCommand(export())
	cliCmd.AddCommand(principalsCmd())
	cliCmd.AddCommand(accessCmd())
	cliCmd.AddCommand(dumpDBCmd())
	cliCmd.AddCommand(syncsCmd())
	cliCmd.AddCommand(optimizeDb())
	cliCmd.AddCommand(toPebbleCmd())
	cliCmd.AddCommand(explorerCmd())
	cliCmd.AddCommand(sanitizeCmd())
	cliCmd.AddCommand(rollbackExpansionCmd())

	err := cliCmd.ExecuteContext(ctx)
	if err != nil {
		exit.LogExit(err)
	}
}
