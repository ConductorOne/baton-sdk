//go:build windows

package cli

import (
	"context"

	"github.com/spf13/cobra"
	"golang.org/x/sys/windows/svc"
)

func IsService() bool {
	if ok, _ := svc.IsWindowsService(); ok {
		return true
	}
	return false
}

func additionalCommands(ctx context.Context) ([]*cobra.Command, error) {
	return nil, nil
}
