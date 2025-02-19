package cli

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"

	"github.com/aws/aws-lambda-go/lambda"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	lambda_sdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	aws_transport "github.com/aws/smithy-go/endpoints"
	"github.com/davecgh/go-spew/spew"
	pb_connector_api "github.com/ductone/c1-lambda/pb/c1/connectorapi/baton/v1"
	c1_lambda_config "github.com/ductone/c1-lambda/pkg/config"
	c1_lambda_grpc "github.com/ductone/c1-lambda/pkg/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/ductone/c1-lambda/pkg/grpc/transport"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
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

func MakeLambdaMainCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	opts ...connectorrunner.Option,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		// NOTE(shackra): bind all the flags (persistent and
		// regular) with our instance of Viper, doing this
		// anywhere else may fail to communicate to Viper the
		// values gathered by Cobra.
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

		l := ctxzap.Extract(runCtx)

		if isService() {
			l.Debug("running as service", zap.String("name", name))
			runCtx, err = runService(runCtx, name)
			if err != nil {
				l.Error("error running service", zap.Error(err))
				return err
			}
		}

		// validate required fields and relationship constraints
		if err := field.Validate(confschema, v); err != nil {
			return err
		}

		daemonMode := v.GetString("client-id") != "" || isService()
		if daemonMode {
			if v.GetString("client-id") == "" {
				return fmt.Errorf("client-id is required in service mode")
			}
			if v.GetString("client-secret") == "" {
				return fmt.Errorf("client-secret is required in service mode")
			}
			opts = append(
				opts,
				connectorrunner.WithClientCredentials(
					v.GetString("client-id"),
					v.GetString("client-secret"),
				),
			)
			if v.GetBool("skip-full-sync") {
				opts = append(opts, connectorrunner.WithFullSyncDisabled())
			}
		} else {
			switch {
			case v.GetString("grant-entitlement") != "":
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandGrant(
						v.GetString("file"),
						v.GetString("grant-entitlement"),
						v.GetString("grant-principal"),
						v.GetString("grant-principal-type"),
					))
			case v.GetString("revoke-grant") != "":
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandRevoke(
						v.GetString("file"),
						v.GetString("revoke-grant"),
					))
			case v.GetBool("event-feed"):
				opts = append(opts, connectorrunner.WithOnDemandEventStream())
			case v.GetString("create-account-login") != "":
				profileMap := v.GetStringMap("create-account-profile")
				if profileMap == nil {
					profileMap = make(map[string]interface{})
				}
				profile, err := structpb.NewStruct(profileMap)
				if err != nil {
					return err
				}
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandCreateAccount(
						v.GetString("file"),
						v.GetString("create-account-login"),
						v.GetString("create-account-email"),
						profile,
					))
			case v.GetString("delete-resource") != "":
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandDeleteResource(
						v.GetString("file"),
						v.GetString("delete-resource"),
						v.GetString("delete-resource-type"),
					))
			case v.GetString("rotate-credentials") != "":
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandRotateCredentials(
						v.GetString("file"),
						v.GetString("rotate-credentials"),
						v.GetString("rotate-credentials-type"),
					))
			case v.GetBool("create-ticket"):
				opts = append(opts,
					connectorrunner.WithTicketingEnabled(),
					connectorrunner.WithCreateTicket(v.GetString("ticket-template-path")))
			case v.GetBool("bulk-create-ticket"):
				opts = append(opts,
					connectorrunner.WithTicketingEnabled(),
					connectorrunner.WithBulkCreateTicket(v.GetString("bulk-ticket-template-path")))
			case v.GetBool("list-ticket-schemas"):
				opts = append(opts,
					connectorrunner.WithTicketingEnabled(),
					connectorrunner.WithListTicketSchemas())
			case v.GetBool("get-ticket"):
				opts = append(opts,
					connectorrunner.WithTicketingEnabled(),
					connectorrunner.WithGetTicket(v.GetString("ticket-id")))
			default:
				opts = append(opts, connectorrunner.WithOnDemandSync(v.GetString("file")))
			}
		}

		if v.GetString("c1z-temp-dir") != "" {
			c1zTmpDir := v.GetString("c1z-temp-dir")
			if _, err := os.Stat(c1zTmpDir); os.IsNotExist(err) {
				return fmt.Errorf("the specified c1z temp dir does not exist: %s", c1zTmpDir)
			}
			opts = append(opts, connectorrunner.WithTempDir(v.GetString("c1z-temp-dir")))
		}

		c, err := lambdaConnectorClient(runCtx, v.GetString("lambda-endpoint"), v.GetString("lambda-function"))
		if err != nil {
			return err
		}

		r, err := connectorrunner.NewClientOnlyConnectorRunner(runCtx, c, opts...)
		if err != nil {
			l.Error("error creating connector runner", zap.Error(err))
			return err
		}
		defer r.Close(runCtx)

		err = r.Run(runCtx)
		if err != nil {
			l.Error("error running connector", zap.Error(err))
			return err
		}

		return nil
	}
}

func MakeLambdaMetadataCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
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

		// validate required fields and relationship constraints
		if err := field.Validate(confschema, v); err != nil {
			return err
		}

		c, err := lambdaConnectorClient(runCtx, v.GetString("lambda-endpoint"), v.GetString("lambda-function"))
		if err != nil {
			return err
		}
		md, err := c.GetMetadata(runCtx, &v2.ConnectorServiceGetMetadataRequest{})
		if err != nil {
			return err
		}

		protoMarshaller := protojson.MarshalOptions{
			Multiline: true,
			Indent:    "  ",
		}

		outBytes, err := protoMarshaller.Marshal(md)
		if err != nil {
			return err
		}

		_, err = fmt.Fprint(os.Stdout, string(outBytes))
		if err != nil {
			return err
		}

		return nil
	}
}

func MakeLambdaServerCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	lambdaConfSchema field.Configuration,
	getConnector GetConnectorFunc,
	connectorConfSchema field.Configuration,
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

		if err := field.Validate(lambdaConfSchema, v); err != nil {
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

		fmt.Println("Received config: ", spew.Sdump(config))

		// For each thing in the schema, see if it exists in the config with the correct type.
		// If it does, set the value.
		for _, f := range connectorConfSchema.Fields {
			// Fetch the config value by field name.
			// TODO(morgabra): Normalization here?
			cv, ok := config.GetConfig().GetFields()[f.FieldName]
			if !ok {
				continue
			}

			switch f.FieldType {
			case reflect.Bool:
				_, ok := cv.GetKind().(*structpb.Value_BoolValue)
				if !ok {
					return fmt.Errorf("field %s configuration type %T doesn't match expected type %s", f.FieldName, cv.GetKind(), f.FieldType.String())
				}
				v.Set(f.FieldName, cv.GetBoolValue())
			case reflect.String:
				_, ok := cv.GetKind().(*structpb.Value_StringValue)
				if !ok {
					return fmt.Errorf("field %s configuration type %T doesn't match expected type %s", f.FieldName, cv.GetKind(), f.FieldType.String())
				}
				v.Set(f.FieldName, cv.GetStringValue())
			default:
				return fmt.Errorf("config: field %s has unsupported type %s", f.FieldName, f.FieldType)
			}
		}

		if err := field.Validate(connectorConfSchema, v); err != nil {
			return err
		}

		c, err := getConnector(runCtx, v)
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
