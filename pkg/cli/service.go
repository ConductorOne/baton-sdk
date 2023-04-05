//go:build !windows

package cli

import (
	"os"

	"golang.org/x/term"
)

func IsService() bool {
	return !term.IsTerminal(int(os.Stdin.Fd()))
}
