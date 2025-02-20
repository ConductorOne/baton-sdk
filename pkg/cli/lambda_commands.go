package cli

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/conductorone/baton-sdk/internal/connector"
	pb_connector_api "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/field"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	c1_lambda_config "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
	"github.com/conductorone/baton-sdk/pkg/lambda/grpc/transport"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"

	"github.com/mitchellh/mapstructure"

	"github.com/aws/aws-lambda-go/lambda"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	lambda_sdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	aws_transport "github.com/aws/smithy-go/endpoints"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type staticLambdaResolver struct {
	endpoint *url.URL
}

func (l *staticLambdaResolver) ResolveEndpoint(ctx context.Context, params lambda_sdk.EndpointParameters) (aws_transport.Endpoint, error) {
	return aws_transport.Endpoint{
		URI: *l.endpoint,
	}, nil
}

func newStaticLambdaResolver(endpoint string) (lambda_sdk.EndpointResolverV2, error) {
	uri, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %v", err)
	}
	return &staticLambdaResolver{endpoint: uri}, nil
}

func lambdaConnectorClient(ctx context.Context, endpoint string, function string) (types.ConnectorClient, error) {
	cfg, err := aws_config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	var opts []func(*lambda_sdk.Options)
	if endpoint != "" {
		resolver, err := newStaticLambdaResolver(endpoint)
		if err != nil {
			return nil, err
		}
		opts = append(opts, lambda_sdk.WithEndpointResolverV2(resolver))
	}
	lambdaClient := lambda_sdk.NewFromConfig(cfg, opts...)

	lambdaTransport, err := transport.NewLambdaClientTransport(ctx, lambdaClient, function)
	if err != nil {
		return nil, err
	}
	cc := c1_lambda_grpc.NewClientConn(lambdaTransport)
	return connector.NewConnectorClient(ctx, cc), nil
}

func MakeLambdaServerCommand[T any](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc[T],
	lambdaSchema field.Configuration,
	connectorSchema field.Configuration,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
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

		// l := ctxzap.Extract(runCtx)

		if err := field.Validate(lambdaSchema, v); err != nil {
			return err
		}

		client, err := c1_lambda_config.GetConnectorConfigServiceClient(
			ctx,
			v.GetString("lambda-client-id"),
			v.GetString("lambda-client-secret"),
		)
		if err != nil {
			return fmt.Errorf("failed to get connector manager client: %w", err)
		}

		// Get configuration, convert it to viper flag values, then proceed.
		// TODO(morgabra): Should we start the lambda handler first? What are the timeouts for startup?
		config, err := client.GetConnectorConfig(ctx, &pb_connector_api.GetConnectorConfigRequest{})
		if err != nil {
			return fmt.Errorf("failed to get connector config: %w", err)
		}

		t, err := makeGenericConfiguration[T](v)
		if err != nil {
			return fmt.Errorf("failed to make generic configuration: %w", err)
		}

		err = mapstructure.Decode(config.Config.AsMap(), &t)
		if err != nil {
			log.Fatalf("Error decoding: %v", err)
		}
		v := any(t).(*viper.Viper)

		if err := field.Validate(connectorSchema, v); err != nil {
			return err
		}

		c, err := getconnector(runCtx, t)
		if err != nil {
			return err
		}

		opts := &connector.RegisterOps{
			Ratelimiter:         nil,  // FIXME(morgabra/kans): ???
			ProvisioningEnabled: true, // FIXME(morgabra/kans): ??? - these are `--provisioning` flags to the server binary - do we still want to expose these via the config service?
			TicketingEnabled:    true, // FIXME(morgabra/kans): ???
		}

		s := c1_lambda_grpc.NewServer(nil)
		connector.Register(ctx, s, c, opts)

		lambda.Start(s.Handler)
		return nil
	}
}
