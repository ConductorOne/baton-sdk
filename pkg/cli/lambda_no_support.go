//go:build !build_lambda_support

package cli

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/field"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func OptionallyAddLambdaCommand[T any](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc[T],
	connectorSchema field.Configuration,
	mainCmd *cobra.Command,
) error {
	return nil
}

func MakeLambdaMetadataCommand[T any](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc[T],
	confschema field.Configuration,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		return nil
	}
}
