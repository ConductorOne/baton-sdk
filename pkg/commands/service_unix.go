//go:build !windows

package commands

import (
	"context"
	"reflect"

	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type schemafield interface {
	GetName() string
	GetType() reflect.Kind
}

func isService() bool {
	return false
}

func runService(ctx context.Context, _ string) (context.Context, error) {
	return ctx, nil
}

func initLogger(ctx context.Context, _ string, opts ...logging.Option) (context.Context, error) {
	return logging.Init(ctx, opts...)
}

func AdditionalCommands(connectorName string, fields []schemafield, v *viper.Viper) []*cobra.Command {
	return nil
}
