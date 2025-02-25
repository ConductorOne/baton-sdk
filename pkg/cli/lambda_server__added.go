//go:build baton_lambda_support

package cli

import (
	"context"
	"fmt"

	aws_lambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/conductorone/baton-sdk/internal/connector"
	pb_connector_api "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/field"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	c1_lambda_config "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
)

func OptionallyAddLambdaCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc[T],
	connectorSchema field.Configuration,
	mainCmd *cobra.Command,
) error {
	lambdaSchema := field.NewConfiguration(field.LambdaServerFields(), field.LambdaServerRelationships...)

	lambdaCmd, err := AddCommand(mainCmd, v, &lambdaSchema, &cobra.Command{
		Use:           "lambda",
		Short:         "Run a server for a AWS Lambda function",
		SilenceErrors: true,
		SilenceUsage:  true,
	})

	if err != nil {
		return err
	}

	lambdaCmd.RunE = func(cmd *cobra.Command, args []string) error {
		err := v.BindPFlags(cmd.Flags())
		if err != nil {
			return err
		}

		runCtx, err := initLogger(
			ctx,
			name,
			logging.WithLogFormat(v.GetString("log-format")),
			logging.WithLogLevel(v.GetString("log-level")),
		)
		if err != nil {
			return err
		}

		if err := field.Validate(lambdaSchema, v); err != nil {
			return err
		}

		client, err := c1_lambda_config.GetConnectorConfigServiceClient(
			ctx,
			v.GetString(field.LambdaServerClientIDField.GetName()),
			v.GetString(field.LambdaServerClientSecretField.GetName()),
		)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to get connector manager client: %w", err)
		}

		// Get configuration, convert it to viper flag values, then proceed.
		config, err := client.GetConnectorConfig(ctx, &pb_connector_api.GetConnectorConfigRequest{})
		if err != nil {
			return fmt.Errorf("lambda-run: failed to get connector config: %w", err)
		}

		t, err := MakeGenericConfiguration[T](v)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to make generic configuration: %w", err)
		}

		err = mapstructure.Decode(config.Config.AsMap(), t)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to decode config: %w", err)
		}

		if err := field.Validate(connectorSchema, t); err != nil {
			return fmt.Errorf("lambda-run: failed to validate config: %w", err)
		}

		c, err := getconnector(runCtx, t)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to get connector: %w", err)
		}

		// TODO(morgabra/kans): This seems to be OK in practice - just don't invoke the unimplemented methods.
		opts := &connector.RegisterOps{
			Ratelimiter:         nil,
			ProvisioningEnabled: true,
			TicketingEnabled:    true,
		}

		s := c1_lambda_grpc.NewServer(nil)
		connector.Register(ctx, s, c, opts)

		aws_lambda.Start(s.Handler)
		return nil
	}
	return nil
}
