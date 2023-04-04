//go:build !windows

package cli

import (
	"os"

	"golang.org/x/term"
)

func IsService() bool {
	if term.IsTerminal(int(os.Stdin.Fd())) {
		return false
	}

	return true
}
