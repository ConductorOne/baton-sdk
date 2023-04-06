//go:build !windows

package cli

import (
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

func IsService() bool {
	return !term.IsTerminal(int(os.Stdin.Fd()))
}

func additionalCommands[T any, PtrT *T](connectorName string, cfg PtrT) []*cobra.Command {
	return nil
}
