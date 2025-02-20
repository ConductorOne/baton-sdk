//go:build !compile_lambda_support

package lambdasupport

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const CompileLambdaSupport bool = false

func AddCommand(_ *cobra.Command, _ *cobra.Command) {
}

func MakeLambdaServerCommand[T any](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector cli.GetConnectorFunc[T],
	lambdaSchema field.Configuration,
	connectorSchema field.Configuration,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		return nil
	}
}
