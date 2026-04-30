//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	aws_lambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/go-jose/go-jose/v4"
	"github.com/maypok86/otter/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

const (
	lambdaConnectorConfigVersionHeader = "x-baton-connector-config-version"
	lambdaConnectorDrainTimeout        = 30 * time.Second
	lambdaConnectorCloseTimeout        = 10 * time.Second
)

type lambdaConnectorGeneration struct {
	version   string
	connector types.ConnectorServer
	logging   lambdaLogLevelConfig
}

type lambdaLogLevelConfig struct {
	level              string
	debugModeExpiresAt time.Time
}

func (c lambdaLogLevelConfig) effective(now time.Time) string {
	if c.level == "debug" && !c.debugModeExpiresAt.IsZero() && now.After(c.debugModeExpiresAt) {
		return "info"
	}
	return c.level
}

type lambdaConnectorCloserWithContext interface {
	Close(context.Context) error
}

type lambdaConnectorCloser interface {
	Close() error
}

type lambdaConnectorReloader struct {
	mu      sync.Mutex
	server  *c1_lambda_grpc.Server
	current *lambdaConnectorGeneration
	build   func(context.Context, string) (*lambdaConnectorGeneration, error)
}

func (r *lambdaConnectorReloader) Handler(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	requestedVersion := ""
	if values := req.Headers().Get(lambdaConnectorConfigVersionHeader); len(values) > 0 {
		requestedVersion = values[0]
	}

	if requestedVersion != "" {
		if err := r.reloadIfNeeded(ctx, requestedVersion); err != nil {
			return c1_lambda_grpc.ErrorResponse(status.Errorf(codes.Unavailable, "lambda-run: failed to reload connector config: %v", err)), nil
		}
	}

	if err := r.applyCurrentLogLevel(time.Now()); err != nil {
		return c1_lambda_grpc.ErrorResponse(status.Errorf(codes.Unavailable, "lambda-run: failed to apply log level: %v", err)), nil
	}

	return r.server.Handler(ctx, req)
}

func (r *lambdaConnectorReloader) applyCurrentLogLevel(now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current == nil {
		return nil
	}
	return applyLambdaLogLevel(r.current.logging, now)
}

func (r *lambdaConnectorReloader) reloadIfNeeded(ctx context.Context, requestedVersion string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil && r.current.version == requestedVersion {
		return nil
	}
	if r.current == nil {
		return fmt.Errorf("no current connector generation is registered")
	}

	next, err := r.build(ctx, requestedVersion)
	if err != nil {
		return err
	}
	previous := r.current
	replaced, drained, err := r.server.ReplaceServiceImplementation(previous.connector, next.connector)
	if err != nil {
		return err
	}
	if replaced == 0 {
		return fmt.Errorf("no registered services matched the current connector generation")
	}

	r.current = next
	if err := applyLambdaLogLevel(next.logging, time.Now()); err != nil {
		return err
	}
	closeCtx := context.WithoutCancel(ctx)
	go closeConnectorGenerationAfterDrain(closeCtx, previous, drained, lambdaConnectorDrainTimeout, lambdaConnectorCloseTimeout)
	return nil
}

func closeConnectorGenerationAfterDrain(ctx context.Context, generation *lambdaConnectorGeneration, drained <-chan struct{}, drainTimeout time.Duration, closeTimeout time.Duration) {
	if generation == nil {
		return
	}

	drainTimer := time.NewTimer(drainTimeout)
	defer drainTimer.Stop()

	select {
	case <-drained:
	case <-drainTimer.C:
		zap.L().Warn("timed out waiting for stale lambda connector generation to drain", zap.String("config_version", generation.version), zap.Duration("timeout", drainTimeout))
	}

	closeCtx, cancel := context.WithTimeout(ctx, closeTimeout)
	defer cancel()

	if err := closeLambdaConnectorGeneration(closeCtx, generation.connector); err != nil {
		zap.L().Warn("error closing stale lambda connector generation", zap.String("config_version", generation.version), zap.Error(err))
	}
}

func closeLambdaConnectorGeneration(ctx context.Context, connector types.ConnectorServer) error {
	errCh := make(chan error, 1)
	if closer, ok := connector.(lambdaConnectorCloserWithContext); ok {
		go func() {
			errCh <- closer.Close(ctx)
		}()
	} else if closer, ok := connector.(lambdaConnectorCloser); ok {
		go func() {
			errCh <- closer.Close()
		}()
	} else {
		return nil
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

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

		startupLogLevel, err := lambdaLogLevelConfigFromViper(v)
		if err != nil {
			return err
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
			logging.WithLogLevel(startupLogLevel.effective(time.Now())),
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

		ed25519PrivateKey, ok := webKey.Key.(ed25519.PrivateKey)
		if !ok {
			return fmt.Errorf("lambda-run: failed to cast webkey to ed25519.PrivateKey")
		}

		clientSecret := v.GetString("lambda-client-secret")
		if clientSecret != "" {
			secretJwk, err := crypto.ParseClientSecret([]byte(clientSecret), true)
			if err != nil {
				return err
			}
			runCtx = context.WithValue(runCtx, crypto.ContextClientSecretKey, secretJwk)
		}

		// parse content directly for lambdas, don't read from file
		readFromPath := false
		decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))

		sessionStoreMaximumSize := v.GetInt(field.ServerSessionStoreMaximumSizeField.GetName())
		var sessionStoreConstructor sessions.SessionStoreConstructor
		if sessionStoreEnabled {
			sessionStoreConstructor = createSessionCacheConstructor(grpcClient)
		} else {
			sessionStoreConstructor = func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
				return &session.NoOpSessionStore{}, nil
			}
		}

		buildConnectorGeneration := func(ctx context.Context, requestedVersion string) (*lambdaConnectorGeneration, error) {
			// Get configuration, convert it to viper flag values, then proceed.
			config, err := configClient.GetConnectorConfig(ctx, &v1.GetConnectorConfigRequest{
				RequestedVersion: requestedVersion,
			})
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to get connector config: %w", err)
			}

			decrypted, err := jwk.DecryptED25519(ed25519PrivateKey, config.GetConfig())
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to decrypt config: %w", err)
			}

			configStruct := structpb.Struct{}
			err = json.Unmarshal(decrypted, &configStruct)
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to unmarshal decrypted config: %w", err)
			}

			effectiveConfig := effectiveLambdaConfig(v, configStruct.AsMap())
			logLevelConfig, err := lambdaLogLevelConfigFromViper(effectiveConfig)
			if err != nil {
				return nil, err
			}

			t, err := MakeGenericConfiguration[T](effectiveConfig, decodeOpts)
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to make generic configuration: %w", err)
			}

			var (
				fieldOptions  []field.Option
				schemaFields  []field.SchemaField
				authMethodStr string
			)
			authMethodStr = effectiveConfig.GetString("auth-method")
			if authMethodStr != "" {
				fieldOptions = append(fieldOptions, field.WithAuthMethod(authMethodStr))
			}
			schemaFieldsMap := connectorSchema.FieldGroupFields(authMethodStr)
			for _, field := range schemaFieldsMap {
				schemaFields = append(schemaFields, field)
			}

			if len(schemaFields) == 0 {
				schemaFields = connectorSchema.Fields
			}

			if err := field.Validate(connectorSchema, t, fieldOptions...); err != nil {
				return nil, fmt.Errorf("failed to validate config: %w", err)
			}

			ops := RunTimeOpts{
				SessionStore: NewLazyCachingSessionStore(sessionStoreConstructor, func(otterOptions *otter.Options[string, []byte]) {
					if sessionStoreMaximumSize <= 0 {
						otterOptions.MaximumWeight = 0
					} else {
						otterOptions.MaximumWeight = uint64(sessionStoreMaximumSize)
					}
				}),
				SelectedAuthMethod:  authMethodStr,
				SyncResourceTypeIDs: effectiveConfig.GetStringSlice("sync-resource-types"),
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
				return nil, fmt.Errorf("failed to get connector: %w", err)
			}

			version := requestedVersion
			if version == "" {
				version = lambdaConnectorConfigVersion(config)
			}
			return &lambdaConnectorGeneration{
				version:   version,
				connector: c,
				logging:   logLevelConfig,
			}, nil
		}

		initialGeneration, err := buildConnectorGeneration(runCtx, "")
		if err != nil {
			return err
		}
		if err := applyLambdaLogLevel(initialGeneration.logging, time.Now()); err != nil {
			return err
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
		connector.Register(runCtx, s, initialGeneration.connector, opts)

		reloader := &lambdaConnectorReloader{
			server:  s,
			current: initialGeneration,
			build:   buildConnectorGeneration,
		}

		aws_lambda.StartWithOptions(reloader.Handler, aws_lambda.WithContext(runCtx))
		return nil
	}

	return nil
}

func lambdaConnectorConfigVersion(config *v1.GetConnectorConfigResponse) string {
	if config == nil || config.GetLastUpdated() == nil {
		return ""
	}
	return config.GetLastUpdated().AsTime().UTC().Format(time.RFC3339Nano)
}

func lambdaLogLevelConfigFromViper(v *viper.Viper) (lambdaLogLevelConfig, error) {
	level, err := logging.NormalizeLogLevel(v.GetString("log-level"))
	if err != nil {
		return lambdaLogLevelConfig{}, fmt.Errorf("lambda-run: invalid log level: %w", err)
	}
	return lambdaLogLevelConfig{
		level:              level,
		debugModeExpiresAt: v.GetTime("log-level-debug-expires-at"),
	}, nil
}

func applyLambdaLogLevel(config lambdaLogLevelConfig, now time.Time) error {
	return logging.SetLogLevel(config.effective(now))
}

func cloneViperSettings(v *viper.Viper) *viper.Viper {
	cloned := viper.New()
	if v == nil {
		return cloned
	}
	for key, value := range v.AllSettings() {
		cloned.Set(key, value)
	}
	return cloned
}

func effectiveLambdaConfig(v *viper.Viper, connectorConfig map[string]any) *viper.Viper {
	effectiveConfig := cloneViperSettings(v)
	for key, value := range connectorConfig {
		effectiveConfig.Set(key, value)
	}
	return effectiveConfig
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
	token  *oauth2.Token
}

func (s *lambdaTokenSource) Token() (*oauth2.Token, error) {
	if s.token.Valid() {
		return s.token, nil
	}

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

	s.token = &t
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
