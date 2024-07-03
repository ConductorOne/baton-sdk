//go:build !windows

package commands

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/logging"
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
