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
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type GetConnectorFunc func(context.Context, *viper.Viper) (types.ConnectorServer, error)

func MakeMainCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc,
	opts ...connectorrunner.Option,
) func(*cobra.Command, []string) error {
	return func(*cobra.Command, []string) error {
		// validate required fields and relationship constraints
		if err := field.Validate(confschema, v); err != nil {
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
			runCtx, err = runService(runCtx, name)
			if err != nil {
				l.Error("error running service", zap.Error(err))
				return err
			}
		}

		c, err := getconnector(runCtx, v)
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

func MakeGRPCServerCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	confschema field.Configuration,
	getconnector GetConnectorFunc,
) func(*cobra.Command, []string) error {
	return func(*cobra.Command, []string) error {
		// validate required fields and relationship constraints
		if err := field.Validate(confschema, v); err != nil {
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

		c, err := getconnector(runCtx, v)
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

		switch {
		case v.GetString("grant-entitlement") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("revoke-grant") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("create-account-login") != "" || v.GetString("create-account-email") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("delete-resource") != "" || v.GetString("delete-resource-type") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetString("rotate-credentials") != "" || v.GetString("rotate-credentials-type") != "":
			copts = append(copts, connector.WithProvisioningEnabled())
		case v.GetBool("create-ticket"):
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

		// NOTE (shackra): I don't understand this goroutine
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
	}
}

func MakeCapabilitiesCommand(
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc,
) func(*cobra.Command, []string) error {
	return func(*cobra.Command, []string) error {
		runCtx, err := initLogger(
			ctx,
			name,
			logging.WithLogFormat(v.GetString("log-format")),
			logging.WithLogLevel(v.GetString("log-level")),
		)
		if err != nil {
			return err
		}

		c, err := getconnector(runCtx, v)
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

		protoMarshaller := protojson.MarshalOptions{
			Multiline: true,
			Indent:    "  ",
		}

		a := &anypb.Any{}
		err = anypb.MarshalFrom(a, md.Metadata.Capabilities, proto.MarshalOptions{Deterministic: true})
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
