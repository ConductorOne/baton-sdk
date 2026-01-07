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
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/go-jose/go-jose/v4"
	"github.com/maypok86/otter/v2"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/field"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	c1_lambda_config "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
	"github.com/conductorone/baton-sdk/pkg/lambda/grpc/middleware"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"google.golang.org/grpc"
)

func OptionallyAddLambdaCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc2[T],
	connectorSchema field.Configuration,
	mainCmd *cobra.Command,
	sessionStoreEnabled bool,
) error {
	lambdaSchema := field.NewConfiguration(field.LambdaServerFields(), field.WithConstraints(field.LambdaServerRelationships...))

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

		logLevel := v.GetString("log-level")
		// Downgrade log level to "info" if debug mode has expired
		debugModeExpiresAt := v.GetTime("log-level-debug-expires-at")
		if logLevel == "debug" && !debugModeExpiresAt.IsZero() && time.Now().After(debugModeExpiresAt) {
			logLevel = "info"
		}

		initialLogFields := map[string]interface{}{
			"tenant_id":          os.Getenv("tenant"),
			"connector_id":       os.Getenv("connector"),
			"app_id":             os.Getenv("app"),
			"release_version":    os.Getenv("version"),
			"installation":       os.Getenv("installation"),
			"catalog_id":         os.Getenv("catalog_id"),
			"catalog_name":       os.Getenv("catalog_name"),
			"tenant_name":        os.Getenv("tenant_name"),
			"tenant_is_internal": os.Getenv("tenant_is_internal"),
		}

		runCtx, err := initLogger(
			ctx,
			name,
			logging.WithLogFormat(v.GetString("log-format")),
			logging.WithLogLevel(logLevel),
			logging.WithInitialFields(initialLogFields),
		)
		if err != nil {
			return err
		}

		runCtx, otelShutdown, err := initOtel(runCtx, name, v, initialLogFields)
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

		// Create DPoP client with authentication
		grpcClient, webKey, _, err := c1_lambda_config.NewDPoPClient(
			runCtx,
			v.GetString(field.LambdaServerClientIDField.GetName()),
			v.GetString(field.LambdaServerClientSecretField.GetName()),
		)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to create DPoP client: %w", err)
		}

		// Create connector config service client using the DPoP client
		configClient := v1.NewConnectorConfigServiceClient(grpcClient)

		// Get configuration, convert it to viper flag values, then proceed.
		config, err := configClient.GetConnectorConfig(runCtx, &v1.GetConnectorConfigRequest{})
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

		// parse content directly for lambdas, don't read from file
		readFromPath := false
		decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))
		t, err := MakeGenericConfiguration[T](v, decodeOpts)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to make generic configuration: %w", err)
		}
		switch cfg := any(t).(type) {
		case *viper.Viper:
			for k, v := range configStruct.AsMap() {
				cfg.Set(k, v)
			}
		default:
			// Use mapstructure with decode hook for file upload fields
			decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
				DecodeHook: field.ComposeDecodeHookFunc(decodeOpts),
				Result:     cfg,
			})
			if err != nil {
				return fmt.Errorf("lambda-run: failed to create decoder: %w", err)
			}
			err = decoder.Decode(configStruct.AsMap())
			if err != nil {
				return fmt.Errorf("lambda-run: failed to decode config: %w", err)
			}
		}

		configStructMap := configStruct.AsMap()

		var (
			fieldOptions  []field.Option
			schemaFields  []field.SchemaField
			authMethodStr string
		)
		if authMethod, ok := configStructMap["auth-method"]; ok {
			if authMethodStr, ok = authMethod.(string); ok {
				fieldOptions = append(fieldOptions, field.WithAuthMethod(authMethodStr))
			}
		}
		schemaFieldsMap := connectorSchema.FieldGroupFields(authMethodStr)
		for _, field := range schemaFieldsMap {
			schemaFields = append(schemaFields, field)
		}

		if len(schemaFields) == 0 {
			schemaFields = connectorSchema.Fields
		}

		if err := field.Validate(connectorSchema, t, fieldOptions...); err != nil {
			return fmt.Errorf("lambda-run: failed to validate config: %w", err)
		}

		clientSecret := v.GetString("client-secret")
		if clientSecret != "" {
			secretJwk, err := crypto.ParseClientSecret([]byte(clientSecret), true)
			if err != nil {
				return err
			}
			runCtx = context.WithValue(runCtx, crypto.ContextClientSecretKey, secretJwk)
		}
		sessionStoreMaximumSize := v.GetInt(field.ServerSessionStoreMaximumSizeField.GetName())
		var sessionStoreConstructor sessions.SessionStoreConstructor
		if sessionStoreEnabled {
			sessionStoreConstructor = createSessionCacheConstructor(grpcClient)
		} else {
			sessionStoreConstructor = func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
				return &session.NoOpSessionStore{}, nil
			}
		}
		ops := RunTimeOpts{
			SessionStore: NewLazyCachingSessionStore(sessionStoreConstructor, func(otterOptions *otter.Options[string, []byte]) {
				if sessionStoreMaximumSize <= 0 {
					otterOptions.MaximumWeight = 0
				} else {
					otterOptions.MaximumWeight = uint64(sessionStoreMaximumSize)
				}
			}),
		}

		if hasOauthField(schemaFields) {
			ops.TokenSource = &lambdaTokenSource{
				ctx:    runCtx,
				webKey: webKey,
				client: configClient,
			}
		}
		c, err := getconnector(runCtx, t, ops)
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

		chain := ugrpc.ChainUnaryInterceptors(authOpt)

		s := c1_lambda_grpc.NewServer(chain)
		connector.Register(runCtx, s, c, opts)

		aws_lambda.StartWithOptions(s.Handler, aws_lambda.WithContext(runCtx))
		return nil
	}

	return nil
}

// createSessionCacheConstructor creates a session cache constructor function that uses the provided gRPC client.
func createSessionCacheConstructor(grpcClient grpc.ClientConnInterface) sessions.SessionStoreConstructor {
	return func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
		// Create the gRPC session client using the same gRPC connection
		client := v1.NewBatonSessionServiceClient(grpcClient)
		// Create and return the session cache
		return session.NewGRPCSessionStore(ctx, client, opt...)
	}
}

type lambdaTokenSource struct {
	ctx    context.Context
	webKey *jose.JSONWebKey
	client v1.ConnectorConfigServiceClient
}

func (s *lambdaTokenSource) Token() (*oauth2.Token, error) {
	resp, err := s.client.GetConnectorOauthToken(s.ctx, &v1.GetConnectorOauthTokenRequest{})
	if err != nil {
		return nil, err
	}

	ed25519PrivateKey, ok := s.webKey.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("lambda-run: failed to cast webkey to ed25519.PrivateKey")
	}

	decrypted, err := jwk.DecryptED25519(ed25519PrivateKey, resp.Token)
	if err != nil {
		return nil, fmt.Errorf("lambda-run: failed to decrypt config: %w", err)
	}

	t := oauth2.Token{}
	err = json.Unmarshal(decrypted, &t)
	if err != nil {
		return nil, fmt.Errorf("lambda-run: failed to unmarshal decrypted config: %w", err)
	}
	return &t, nil
}

func hasOauthField(fields []field.SchemaField) bool {
	for _, f := range fields {
		if f.ConnectorConfig.FieldType == field.OAuth2 {
			return true
		}
	}
	return false
}
