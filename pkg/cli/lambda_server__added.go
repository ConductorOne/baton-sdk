//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"
	"time"

	aws_lambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-sdk/internal/connector"
	pb_connector_api "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/field"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	c1_lambda_config "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
	"github.com/conductorone/baton-sdk/pkg/lambda/grpc/middleware"
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

		initalLogFields := map[string]interface{}{
			"tenant":       os.Getenv("tenant"),
			"connector":    os.Getenv("connector"),
			"installation": os.Getenv("installation"),
			"app":          os.Getenv("app"),
			"version":      os.Getenv("version"),
		}

		runCtx, err := initLogger(
			ctx,
			name,
			logging.WithLogFormat(v.GetString("log-format")),
			logging.WithLogLevel(v.GetString("log-level")),
			logging.WithInitialFields(initalLogFields),
		)
		if err != nil {
			return err
		}

		runCtx, otelShutdown, err := initOtel(context.Background(), name, v, initalLogFields)
		if err != nil {
			return err
		}
		defer func() {
			if otelShutdown == nil {
				return
			}
			shutdownCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(otelShutdownTimeout))
			defer cancel()
			err := otelShutdown(shutdownCtx)
			if err != nil {
				zap.L().Error("error shutting down otel", zap.Error(err))
			}
		}()

		if err := field.Validate(lambdaSchema, v); err != nil {
			return err
		}

		client, webKey, err := c1_lambda_config.GetConnectorConfigServiceClient(
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

		ed25519PrivateKey, ok := webKey.Key.(ed25519.PrivateKey)
		if !ok {
			return fmt.Errorf("lambda-run: failed to cast webkey to ed25519.PrivateKey")
		}

		decrypted, err := jwk.DecryptED25519(ed25519PrivateKey, config.Config)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to decrypt config: %w", err)
		}

		configStruct := structpb.Struct{}
		err = json.Unmarshal(decrypted, &configStruct)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to unmarshal decrypted config: %w", err)
		}

		t, err := MakeGenericConfiguration[T](v)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to make generic configuration: %w", err)
		}
		switch cfg := any(t).(type) {
		case *viper.Viper:
			for k, v := range configStruct.AsMap() {
				cfg.Set(k, v)
			}
		default:
			err = mapstructure.Decode(configStruct.AsMap(), cfg)
			if err != nil {
				return fmt.Errorf("lambda-run: failed to decode config: %w", err)
			}
		}

		if err := field.Validate(connectorSchema, t); err != nil {
			return fmt.Errorf("lambda-run: failed to validate config: %w", err)
		}

		c, err := getconnector(runCtx, t)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to get connector: %w", err)
		}

		// Ensure only one auth method is provided
		jwk := v.GetString(field.LambdaServerAuthJWTSigner.GetName())
		jwksUrl := v.GetString(field.LambdaServerAuthJWTJWKSUrl.GetName())
		if (jwk == "" && jwksUrl == "") || (jwk != "" && jwksUrl != "") {
			return fmt.Errorf("lambda-run: must specify exactly one of %s or %s", field.LambdaServerAuthJWTSigner.GetName(), field.LambdaServerAuthJWTJWKSUrl.GetName())
		}

		authConfig := auth.Config{
			PublicKeyJWK: jwk,
			JWKSUrl:      jwksUrl,
			Issuer:       v.GetString(field.LambdaServerAuthJWTExpectedIssuerField.GetName()),
			Subject:      v.GetString(field.LambdaServerAuthJWTExpectedSubjectField.GetName()),
			Audience:     v.GetString(field.LambdaServerAuthJWTExpectedAudienceField.GetName()),
		}

		authOpt, err := middleware.WithAuth(runCtx, authConfig)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to create auth middleware: %w", err)
		}

		// TODO(morgabra/kans): This seems to be OK in practice - just don't invoke the unimplemented methods.
		opts := &connector.RegisterOps{
			Ratelimiter:         nil,
			ProvisioningEnabled: true,
			TicketingEnabled:    true,
		}

		s := c1_lambda_grpc.NewServer(authOpt)
		connector.Register(ctx, s, c, opts)

		aws_lambda.Start(s.Handler)
		return nil
	}
	return nil
}
