//go:build !windows

package commands

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func isService() bool {
	return false
}

func setupService(_ string) error {
	return nil
}

func additionalCommands[T any, PtrT *T](_ string, _ PtrT) []*cobra.Command {
	return nil
}

func runService(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

func initLogger(ctx context.Context, _ string, opts ...logging.Option) (context.Context, error) {
	return logging.Init(ctx, opts...)
}
