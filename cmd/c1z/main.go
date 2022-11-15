package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	_ = context.Background()

	cmd := &cobra.Command{
		Use:     "c1z",
		Short:   "c1z is a utility for viewing the contents of a .c1z file",
		Version: version,
	}

	cmd.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to sync with")

	cmd.AddCommand(resourcesCmd())
	cmd.AddCommand(resourceTypesCmd())
	cmd.AddCommand(entitlementsCmd())
	cmd.AddCommand(grantsCmd())
	cmd.AddCommand(usersCmd())
	cmd.AddCommand(statsCmd())
	cmd.AddCommand(diffCmd())
	cmd.AddCommand(export())

	err := cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
