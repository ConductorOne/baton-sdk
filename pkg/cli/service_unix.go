//go:build !windows

package cli

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func isService() bool {
	return false
}

func setupService(name string) error {
	return nil
}

func additionalCommands[T any, PtrT *T](connectorName string, cfg PtrT) []*cobra.Command {
	return nil
}

func runService(ctx context.Context, name string) (context.Context, error) {
	return ctx, nil
}

func initLogger(ctx context.Context, name string, opts ...logging.Option) (context.Context, error) {
	return logging.Init(ctx, opts...)
}
