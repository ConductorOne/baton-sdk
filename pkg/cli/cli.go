package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	envPrefix        = "baton"
	defaultLogLevel  = "info"
	defaultLogFormat = logging.LogFormatJSON
)

// NewCmd returns a new cobra command that will populate the provided config object, validate it, and run the provided run function.
func NewCmd[T any, PtrT *T](
	ctx context.Context,
	name string,
	cfg PtrT,
	validateF func(ctx context.Context, cfg PtrT) error,
	getConnector func(ctx context.Context, cfg PtrT) (types.ConnectorServer, error),
	opts ...connectorrunner.Option,
) (*cobra.Command, error) {
	err := setupService(name)
	if err != nil {
		return nil, err
	}

	cmd := &cobra.Command{
		Use:           name,
		Short:         name,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
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

			err = validateF(ctx, cfg)
			if err != nil {
				return err
			}

			l := ctxzap.Extract(runCtx)

			if isService() {
				runCtx, err = runService(runCtx, name)
				if err != nil {
					l.Error("error running service", zap.Error(err))
					return err
				}
			}

			c, err := getConnector(runCtx, cfg)
			if err != nil {
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
				opts = append(opts, connectorrunner.WithClientCredentials(v.GetString("client-id"), v.GetString("client-secret")))
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
				case v.GetString("create-account-login") != "":
					opts = append(opts,
						connectorrunner.WithProvisioningEnabled(),
						connectorrunner.WithOnDemandCreateAccount(
							v.GetString("file"),
							v.GetString("create-account-login"),
							v.GetString("create-account-email"),
						))
				case v.GetString("delete-resource") != "":
					opts = append(opts,
						connectorrunner.WithProvisioningEnabled(),
						connectorrunner.WithOnDemandDeleteResource(
							v.GetString("file"),
							v.GetString("delete-resource"),
							v.GetString("delete-resource-type"),
						))
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
		},
	}

	grpcServerCmd := &cobra.Command{
		Use:    "_connector-service",
		Short:  "Start the connector service",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
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

			err = validateF(runCtx, cfg)
			if err != nil {
				return err
			}

			c, err := getConnector(runCtx, cfg)
			if err != nil {
				return err
			}

			var copts []connector.Option

			switch {
			case v.GetString("grant-entitlement") != "":
				copts = append(copts, connector.WithProvisioningEnabled())
			case v.GetString("revoke-grant") != "":
				copts = append(copts, connector.WithProvisioningEnabled())
			case v.GetString("create-account-login") != "" || v.GetString("create-account-email") != "":
				copts = append(copts, connector.WithProvisioningEnabled())
			case v.GetString("delete-resource") != "" || v.GetString("delete-resource-type") != "":
				copts = append(copts, connector.WithProvisioningEnabled())
			case v.GetBool("provisioning"):
				copts = append(copts, connector.WithProvisioningEnabled())
			}

			cw, err := connector.NewWrapper(runCtx, c, copts...)
			if err != nil {
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

			return cw.Run(runCtx, serverCfg)
		},
	}

	capabilitiesCmd := &cobra.Command{
		Use:   "capabilities",
		Short: "Get connector capabilities",
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(cmd, cfg)
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

			c, err := getConnector(runCtx, cfg)
			if err != nil {
				return err
			}

			md, err := c.GetMetadata(runCtx, &v2.ConnectorServiceGetMetadataRequest{})
			if err != nil {
				return err
			}

			if md.Metadata.Capabilities == nil {
				return fmt.Errorf("connector does not support capabilities")
			}

			outBytes, err := protojson.Marshal(md.Metadata.Capabilities)
			if err != nil {
				return err
			}

			_, err = fmt.Fprint(os.Stdout, string(outBytes))
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(grpcServerCmd)
	cmd.AddCommand(capabilitiesCmd)

	// Flags for file management
	cmd.PersistentFlags().String("c1z-temp-dir", "", "The directory to store temporary files in. It "+
		"must exist, and write access is required. Defaults to the OS temporary directory. ($BATON_C1Z_TEMP_DIR)")
	if err := cmd.PersistentFlags().MarkHidden("c1z-temp-dir"); err != nil {
		return nil, err
	}

	// Flags for logging configuration
	cmd.PersistentFlags().String("log-level", defaultLogLevel, "The log level: debug, info, warn, error ($BATON_LOG_LEVEL)")
	cmd.PersistentFlags().String("log-format", defaultLogFormat, "The output format for logs: json, console ($BATON_LOG_FORMAT)")

	// Flags for direct syncing and provisioning
	cmd.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to sync with ($BATON_FILE)")
	cmd.PersistentFlags().String("grant-entitlement", "", "The entitlement to grant to the supplied principal ($BATON_GRANT_ENTITLEMENT)")
	cmd.PersistentFlags().String("grant-principal", "", "The resource to grant the entitlement to ($BATON_GRANT_PRINCIPAL)")
	cmd.PersistentFlags().String("grant-principal-type", "", "The resource type of the principal to grant the entitlement to ($BATON_GRANT_PRINCIPAL_TYPE)")
	cmd.MarkFlagsRequiredTogether("grant-entitlement", "grant-principal", "grant-principal-type")
	cmd.PersistentFlags().String("revoke-grant", "", "The grant to revoke ($BATON_REVOKE_GRANT)")

	cmd.PersistentFlags().String("create-account-login", "", "The login of the account to create ($BATON_CREATE_ACCOUNT_LOGIN)")
	cmd.PersistentFlags().String("create-account-email", "", "The email of the account to create ($BATON_CREATE_ACCOUNT_EMAIL)")

	cmd.PersistentFlags().String("delete-resource", "", "The id of the resource to delete ($BATON_DELETE_RESOURCE)")
	cmd.PersistentFlags().String("delete-resource-type", "", "The type of the resource to delete ($BATON_DELETE_RESOURCE_TYPE)")

	cmd.MarkFlagsMutuallyExclusive("grant-entitlement", "revoke-grant", "create-account-login", "delete-resource")
	cmd.MarkFlagsMutuallyExclusive("grant-entitlement", "revoke-grant", "create-account-email", "delete-resource-type")
	err = cmd.PersistentFlags().MarkHidden("grant-entitlement")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("grant-principal")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("grant-principal-type")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("revoke-grant")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("create-account-login")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("create-account-email")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("delete-resource")
	if err != nil {
		return nil, err
	}
	err = cmd.PersistentFlags().MarkHidden("delete-resource-type")
	if err != nil {
		return nil, err
	}

	// Flags for daemon mode
	cmd.PersistentFlags().String("client-id", "", "The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)")
	cmd.PersistentFlags().String("client-secret", "", "The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)")
	cmd.PersistentFlags().BoolP("provisioning", "p", false, "This must be set in order for provisioning actions to be enabled. ($BATON_PROVISIONING)")
	cmd.MarkFlagsRequiredTogether("client-id", "client-secret")
	cmd.MarkFlagsMutuallyExclusive("file", "client-id")

	// Add a hook for additional commands to be added to the root command.
	// We use this for OS specific commands.
	cmd.AddCommand(additionalCommands(name, cfg)...)

	return cmd, nil
}
