//go:build !windows

package cli

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func isService() bool {
	return false
}

func runService(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

func initLogger(ctx context.Context, _ string, opts ...logging.Option) (context.Context, error) {
	return logging.Init(ctx, opts...)
}

func AdditionalCommands(_ string, _ []field.SchemaField) []*cobra.Command {
	return nil
}
