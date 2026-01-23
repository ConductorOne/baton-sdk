package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/maypok86/otter/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	baton_v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	utls2 "github.com/conductorone/baton-sdk/pkg/utls"
)

const (
	otelShutdownTimeout = 5 * time.Second
)

type ContrainstSetter func(*cobra.Command, field.Configuration) error

// In one shot & service mode, the child process uses this client to connect to the session store server...
//
//	which uses the C1Z for storage.  Unfortunately the C1Z is instantiated well after we fork the child process,
//	so there is quite a bit of pass through.
func getGRPCSessionStoreClient(ctx context.Context, serverCfg *v1.ServerConfig) func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
	return func(_ context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
		l := ctxzap.Extract(ctx)
		clientTLSConfig, err := utls2.ClientConfig(ctx, serverCfg.GetCredential())
		if err != nil {
			return nil, err
		}
		if serverCfg.GetSessionStoreListenPort() == 0 {
			return &session.NoOpSessionStore{}, nil
		}
		// connected, grpc will handle retries for us.
		dialCtx, canc := context.WithTimeout(ctx, 5*time.Second)
		defer canc()
		var dialErr error
		var conn *grpc.ClientConn
		for {
			conn, err = grpc.DialContext( //nolint:staticcheck // grpc.DialContext is deprecated but we are using it still.
				ctx,
				fmt.Sprintf("127.0.0.1:%d", serverCfg.GetSessionStoreListenPort()),
				grpc.WithTransportCredentials(credentials.NewTLS(clientTLSConfig)),
				grpc.WithBlock(), //nolint:staticcheck // grpc.WithBlock is deprecated but we are using it still.
			)
			if err != nil {
				dialErr = err
				select {
				case <-time.After(time.Millisecond * 500):
				case <-dialCtx.Done():
					return nil, dialErr
				}
				continue
			}
			break
		}

		client := baton_v1.NewBatonSessionServiceClient(conn)
		ss, err := session.NewGRPCSessionStore(ctx, client, opt...)
		if err != nil {
			err2 := conn.Close()
			if err2 != nil {
				l.Error("error closing connection", zap.Error(err2))
			}
			return nil, err
		}
		return ss, nil
	}
}

func MakeMainCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc2[T],
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

		runCtx, otelShutdown, err := initOtel(runCtx, name, v, nil)
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

		// NOTE: initOtel may do stuff with the logger
		l := ctxzap.Extract(runCtx)

		if isService() {
			l.Debug("running as service", zap.String("name", name))
			runCtx, err = runService(runCtx, name)
			if err != nil {
				l.Error("error running service", zap.Error(err))
				return err
			}
		}

		readFromPath := true
		decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))
		t, err := MakeGenericConfiguration[T](v, decodeOpts)
		if err != nil {
			return fmt.Errorf("failed to make configuration: %w", err)
		}
		// validate required fields and relationship constraints
		if err := field.Validate(confschema, t, field.WithAuthMethod(v.GetString("auth-method"))); err != nil {
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
				opts = append(opts, connectorrunner.WithOnDemandEventStream(v.GetString("event-feed-id"), v.GetTime("event-feed-start-at"), v.GetString("event-feed-cursor")))
			case v.GetString("create-account-profile") != "":
				profileMap := v.GetStringMap("create-account-profile")
				if profileMap == nil {
					return fmt.Errorf("create-account-profile is empty or incorrectly formatted: %v", v.GetString("create-account-profile"))
				}
				if v.GetString("create-account-login") != "" {
					if _, ok := profileMap["login"]; !ok {
						profileMap["login"] = v.GetString("create-account-login")
					}
				}
				if v.GetString("create-account-email") != "" {
					if _, ok := profileMap["email"]; !ok {
						profileMap["email"] = v.GetString("create-account-email")
					}
				}
				login, email := "", ""
				if l, ok := profileMap["login"]; ok {
					if l, ok := l.(string); ok {
						login = l
					}
				}
				if e, ok := profileMap["email"]; ok {
					if e, ok := e.(string); ok {
						email = e
					}
				}
				profile, err := structpb.NewStruct(profileMap)
				if err != nil {
					return err
				}
				opts = append(opts,
					connectorrunner.WithProvisioningEnabled(),
					connectorrunner.WithOnDemandCreateAccount(
						v.GetString("file"),
						login,
						email,
						profile,
						v.GetString("create-account-resource-type"),
					))
			case v.GetString("create-account-login") != "":
				// should only be here if no create-account-profile is provided, so lets make one.
				profile, err := structpb.NewStruct(map[string]any{
					"login": v.GetString("create-account-login"),
					"email": v.GetString("create-account-email"),
				})
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
						v.GetString("create-account-resource-type"),
					))
			case v.GetString("invoke-action") != "":
				invokeActionArgsStr := v.GetString("invoke-action-args")
				invokeActionArgs := map[string]any{}
				if invokeActionArgsStr != "" {
					err := json.Unmarshal([]byte(invokeActionArgsStr), &invokeActionArgs)
					if err != nil {
						return fmt.Errorf("failed to parse invoke-action-args: %w", err)
					}
				}
				invokeActionArgsStruct, err := structpb.NewStruct(invokeActionArgs)
				if err != nil {
					return fmt.Errorf("failed to parse invoke-action-args: %w", err)
				}
				opts = append(opts,
					connectorrunner.WithActionsEnabled(),
					connectorrunner.WithOnDemandInvokeAction(
						v.GetString("file"),
						v.GetString("invoke-action"),
						v.GetString("invoke-action-resource-type"), // Optional resource type for resource-scoped actions
						invokeActionArgsStruct,
					))
			case v.GetBool("list-action-schemas"):
				opts = append(opts,
					connectorrunner.WithActionsEnabled(),
					connectorrunner.WithOnDemandListActionSchemas(
						v.GetString("file"),
						v.GetString("list-action-schemas-resource-type"), // Optional resource type filter
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
			case v.GetBool("diff-syncs"):
				opts = append(opts,
					connectorrunner.WithDiffSyncs(
						v.GetString("file"),
						v.GetString("base-sync-id"),
						v.GetString("applied-sync-id"),
					),
				)
			case v.GetBool("compact-syncs"):
				opts = append(opts,
					connectorrunner.WithSyncCompactor(
						v.GetString("compact-output-path"),
						v.GetStringSlice("compact-file-paths"),
						v.GetStringSlice("compact-sync-ids"),
					),
				)
			default:
				if len(v.GetStringSlice("sync-resources")) > 0 {
					opts = append(opts,
						connectorrunner.WithTargetedSyncResources(v.GetStringSlice("sync-resources")))
				}
				if len(v.GetStringSlice("sync-resource-types")) > 0 {
					opts = append(opts,
						connectorrunner.WithSyncResourceTypeIDs(v.GetStringSlice("sync-resource-types")))
				}
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

		if v.GetString("external-resource-c1z") != "" {
			externalResourceC1ZPath := v.GetString("external-resource-c1z")
			_, err := os.Open(externalResourceC1ZPath)
			if err != nil {
				return fmt.Errorf("the specified external resource c1z file does not exist: %s", externalResourceC1ZPath)
			}
			opts = append(opts, connectorrunner.WithExternalResourceC1Z(externalResourceC1ZPath))
		}

		if v.GetString("external-resource-entitlement-id-filter") != "" {
			externalResourceEntitlementIdFilter := v.GetString("external-resource-entitlement-id-filter")
			opts = append(opts, connectorrunner.WithExternalResourceEntitlementFilter(externalResourceEntitlementIdFilter))
		}

		opts = append(opts, connectorrunner.WithSkipEntitlementsAndGrants(v.GetBool("skip-entitlements-and-grants")))

		if v.GetBool("skip-grants") {
			opts = append(opts, connectorrunner.WithSkipGrants(v.GetBool("skip-grants")))
		}

		c, err := getconnector(runCtx, t, RunTimeOpts{})
		if err != nil {
			return err
		}

		// NOTE(shackra): top-most in the execution flow for connectors
		r, err := connectorrunner.NewConnectorRunner(runCtx, c, opts...)
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

func initOtel(ctx context.Context, name string, v *viper.Viper, initialLogFields map[string]interface{}) (context.Context, func(context.Context) error, error) {
	otelEndpoint := v.GetString(field.OtelCollectorEndpointFieldName)
	if otelEndpoint == "" {
		return ctx, nil, nil
	}

	var otelOpts []uotel.Option
	otelOpts = append(otelOpts, uotel.WithServiceName(fmt.Sprintf("%s-server", name)))

	if len(initialLogFields) > 0 {
		otelOpts = append(otelOpts, uotel.WithInitialLogFields(initialLogFields))
	}

	if v.GetBool(field.OtelTracingDisabledFieldName) {
		otelOpts = append(otelOpts, uotel.WithTracingDisabled())
	}

	if v.GetBool(field.OtelLoggingDisabledFieldName) {
		otelOpts = append(otelOpts, uotel.WithLoggingDisabled())
	}

	otelTLSInsecure := v.GetBool(field.OtelCollectorEndpointTLSInsecureFieldName)
	if otelTLSInsecure {
		otelOpts = append(otelOpts, uotel.WithInsecureOtelEndpoint(otelEndpoint))
	} else {
		otelTLSCert := v.GetString(field.OtelCollectorEndpointTLSCertFieldName)
		otelTLSCertPath := v.GetString(field.OtelCollectorEndpointTLSCertPathFieldName)
		otelOpts = append(otelOpts, uotel.WithOtelEndpoint(otelEndpoint, otelTLSCertPath, otelTLSCert))
	}

	return uotel.InitOtel(ctx, otelOpts...)
}

func MakeGRPCServerCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc2[T],
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

		runCtx, otelShutdown, err := initOtel(runCtx, name, v, nil)
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

		l := ctxzap.Extract(runCtx)
		l.Debug("starting grpc server")

		readFromPath := true
		decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))
		t, err := MakeGenericConfiguration[T](v, decodeOpts)
		if err != nil {
			return fmt.Errorf("failed to make configuration: %w", err)
		}
		// validate required fields and relationship constraints
		if err := field.Validate(confschema, t, field.WithAuthMethod(v.GetString("auth-method"))); err != nil {
			return err
		}

		var cfgStr string
		scn := bufio.NewScanner(os.Stdin)
		for scn.Scan() {
			cfgStr = scn.Text()
			break
		}

		cfgBytes, err := base64.StdEncoding.DecodeString(cfgStr)
		if err != nil {
			return err
		}

		// Avoid zombie processes. If the parent dies, this
		// will cause Stdin on the child to close, and then
		// the child will exit itself.
		go func() {
			in := make([]byte, 1)
			_, err := os.Stdin.Read(in)
			if err != nil {
				os.Exit(0)
			}
		}()

		if len(cfgBytes) == 0 {
			return fmt.Errorf("unexpected empty input")
		}

		serverCfg := &v1.ServerConfig{}
		err = proto.Unmarshal(cfgBytes, serverCfg)
		if err != nil {
			return err
		}

		err = serverCfg.ValidateAll()
		if err != nil {
			return err
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
		sessionConstructor := getGRPCSessionStoreClient(runCtx, serverCfg)
		c, err := getconnector(runCtx, t, RunTimeOpts{
			SessionStore: NewLazyCachingSessionStore(sessionConstructor, func(otterOptions *otter.Options[string, []byte]) {
				if sessionStoreMaximumSize <= 0 {
					otterOptions.MaximumWeight = 0
				} else {
					otterOptions.MaximumWeight = uint64(sessionStoreMaximumSize)
				}
			}),
		})
		if err != nil {
			return err
		}

		var copts []connector.Option

		if v.GetBool("provisioning") {
			copts = append(copts, connector.WithProvisioningEnabled())
		}

		if v.GetBool("ticketing") {
			copts = append(copts, connector.WithTicketingEnabled())
		}

		if v.GetBool("skip-full-sync") {
			copts = append(copts, connector.WithFullSyncDisabled())
		}

		if len(v.GetStringSlice("sync-resources")) > 0 {
			copts = append(copts, connector.WithTargetedSyncResources(v.GetStringSlice("sync-resources")))
		}

		if len(v.GetStringSlice("sync-resource-types")) > 0 {
			copts = append(copts, connector.WithSyncResourceTypeIDs(v.GetStringSlice("sync-resource-types")))
		}

		switch {
		case v.GetString("grant-entitlement") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("revoke-grant") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("create-account-profile") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("create-account-login") != "" || v.GetString("create-account-email") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("delete-resource") != "" || v.GetString("delete-resource-type") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("rotate-credentials") != "" || v.GetString("rotate-credentials-type") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetBool("create-ticket"):
			copts = append(copts, connector.WithTicketingEnabled())
		case v.GetBool("bulk-create-ticket"):
			copts = append(copts, connector.WithTicketingEnabled())
		case v.GetBool("list-ticket-schemas"):
			copts = append(copts, connector.WithTicketingEnabled())
		case v.GetBool("get-ticket"):
			copts = append(copts, connector.WithTicketingEnabled())
		}

		cw, err := connector.NewWrapper(runCtx, c, copts...)
		if err != nil {
			return err
		}

		return cw.Run(runCtx, serverCfg)
	}
}

func MakeCapabilitiesCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc2[T],
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

		var c types.ConnectorServer

		c, err = defaultConnectorBuilder(ctx, opts...)
		if err != nil {
			return fmt.Errorf("failed to build default connector: %w", err)
		}

		if c == nil {
			readFromPath := true
			decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))
			t, err := MakeGenericConfiguration[T](v, decodeOpts)
			if err != nil {
				return fmt.Errorf("failed to make configuration: %w", err)
			}
			// validate required fields and relationship constraints
			if err := field.Validate(confschema, t, field.WithAuthMethod(v.GetString("auth-method"))); err != nil {
				return err
			}

			c, err = getconnector(runCtx, t, RunTimeOpts{})
			if err != nil {
				return err
			}
		}

		if c == nil {
			return fmt.Errorf("could not create connector %w", err)
		}

		type getter interface {
			GetCapabilities(ctx context.Context) (*v2.ConnectorCapabilities, error)
		}

		var capabilities *v2.ConnectorCapabilities

		if getCap, ok := c.(getter); ok {
			capabilities, err = getCap.GetCapabilities(runCtx)
			if err != nil {
				return err
			}
		}

		if capabilities == nil {
			md, err := c.GetMetadata(runCtx, &v2.ConnectorServiceGetMetadataRequest{})
			if err != nil {
				return err
			}

			if !md.GetMetadata().HasCapabilities() {
				return fmt.Errorf("connector does not support capabilities")
			}

			capabilities = md.GetMetadata().GetCapabilities()
		}

		protoMarshaller := protojson.MarshalOptions{
			Multiline: true,
			Indent:    "  ",
		}

		a := &anypb.Any{}
		err = anypb.MarshalFrom(a, capabilities, proto.MarshalOptions{Deterministic: true})
		if err != nil {
			return err
		}

		outBytes, err := protoMarshaller.Marshal(a)
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

func MakeConfigSchemaCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc2[T],
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		// Use MarshalIndent for pretty printing
		pb, err := json.MarshalIndent(&confschema, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprint(os.Stdout, string(pb))
		if err != nil {
			return err
		}
		return nil
	}
}

func defaultConnectorBuilder(ctx context.Context, opts ...connectorrunner.Option) (types.ConnectorServer, error) {
	defaultConnector, err := connectorrunner.ExtractDefaultConnector(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if defaultConnector == nil {
		return nil, nil
	}

	c, err := connectorbuilder.NewConnector(ctx, defaultConnector)
	if err != nil {
		return nil, err
	}
	return c, nil
}
