//go:build !baton_lambda_support

package cli

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/conductorone/baton-sdk/pkg/field"
)

func OptionallyAddLambdaCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc[T],
	connectorSchema field.Configuration,
	mainCmd *cobra.Command,
) error {
	return nil
}
