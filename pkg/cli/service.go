//go:build !windows

package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"google.golang.org/protobuf/proto"
)

func IsService() bool {
	return !term.IsTerminal(int(os.Stdin.Fd()))
}

func additionalCommands[T any, PtrT *T](ctx context.Context, cfg PtrT) ([]*cobra.Command, error) {
	var ret []*cobra.Command
	testCmd := &cobra.Command{
		Use:    "test",
		Short:  "test additional commands",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig("", cmd, cfg)
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
	ret = append(ret, testCmd)

	return ret, nil
}
