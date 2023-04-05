package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	envPrefix        = "baton"
	defaultLogLevel  = "info"
	defaultLogFormat = logging.LogFormatJSON
)

func DaemonMode(ctx context.Context, enabled bool) bool {
	if enabled {
		return true
	}

	if IsService() {
		return true
	}

	return false
}

// NewCmd returns a new cobra command that will populate the provided config object, validate it, and run the provided run function.
func NewCmd[T any, PtrT *T](
	ctx context.Context,
	name string,
	cfg PtrT,
	validateF func(ctx context.Context, cfg PtrT) error,
	getConnector func(ctx context.Context, cfg PtrT) (types.ConnectorServer, error),
	opts ...connectorrunner.Option,
) (*cobra.Command, error) {
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

			loggerCtx, err := logging.Init(ctx, v.GetString("log-format"), v.GetString("log-level"))
			if err != nil {
				return err
			}

			err = validateF(ctx, cfg)
			if err != nil {
				return err
			}

			l := ctxzap.Extract(loggerCtx)

			c, err := getConnector(loggerCtx, cfg)
			if err != nil {
				return err
			}

			var opts []connectorrunner.Option
			daemonMode := DaemonMode(ctx, v.GetBool("daemon-mode"))
			if daemonMode {
				opts = append(opts, connectorrunner.WithClientCredentials(v.GetString("client-id"), v.GetString("client-secret")))
			} else {
				opts = append(opts, connectorrunner.WithOnDemandSync(v.GetString("file")))
			}

			if v.GetBool("provisioning") {
				opts = append(opts, connectorrunner.WithProvisioningEnabled())
			}

			r, err := connectorrunner.NewConnectorRunner(loggerCtx, c, opts...)
			if err != nil {
				l.Error("error creating connector runner", zap.Error(err))
				return err
			}
			defer r.Close(loggerCtx)

			err = r.Run(loggerCtx)
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

			loggerCtx, err := logging.Init(ctx, v.GetString("log-format"), v.GetString("log-level"))
			if err != nil {
				return err
			}

			err = validateF(loggerCtx, cfg)
			if err != nil {
				return err
			}

			c, err := getConnector(loggerCtx, cfg)
			if err != nil {
				return err
			}

			var copts []connector.Option
			if v.GetBool("provisioning") {
				copts = append(copts, connector.WithProvisioningEnabled())
			}

			cw, err := connector.NewWrapper(loggerCtx, c, copts...)
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

			return cw.Run(loggerCtx, serverCfg)
		},
	}

	cmd.AddCommand(grpcServerCmd)

	cmd.PersistentFlags().String("log-level", defaultLogLevel, "The log level: debug, info, warn, error ($BATON_LOG_LEVEL)")
	cmd.PersistentFlags().String("log-format", defaultLogFormat, "The output format for logs: json, console ($BATON_LOG_FORMAT)")
	cmd.PersistentFlags().StringP("file", "f", "sync.c1z", "The path to the c1z file to sync with ($BATON_FILE)")
	cmd.PersistentFlags().BoolP("daemon-mode", "d", false, "Run in daemon mode ($BATON_DAEMON_MODE)")
	cmd.PersistentFlags().String("client-id", "", "The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)")
	cmd.PersistentFlags().String("client-secret", "", "The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)")
	cmd.PersistentFlags().BoolP("provisioning", "p", false, "This must be set in order for provisioning actions to be enabled. ($BATON_PROVISIONING)")
	err := cmd.PersistentFlags().MarkHidden("daemon-mode")
	if err != nil {
		return nil, err
	}

	// Add a hook for additional commands to be added to the root command.
	// We use this for OS specific commands.
	xtraCmds, err := additionalCommands(ctx)
	if err != nil {
		return nil, err
	}
	cmd.AddCommand(xtraCmds...)

	return cmd, nil
}
