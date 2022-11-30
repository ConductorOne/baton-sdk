package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

const (
	defaultConfigFilename = ".baton-%s"
	envPrefix             = "baton"
	defaultLogLevel       = "info"
	defaultLogFormat      = logging.LogFormatJSON
)

type BaseConfig struct {
	LogLevel  string `mapstructure:"log-level"`
	LogFormat string `mapstructure:"log-format"`
	C1zPath   string `mapstructure:"file"`
}

// NewCmd returns a new cobra command that will populate the provided config object, validate it, and run the provided run function.
func NewCmd[T any, PtrT *T](
	ctx context.Context,
	name string,
	cfg PtrT,
	validateF func(ctx context.Context, cfg PtrT) error,
	getConnector func(ctx context.Context, cfg PtrT) (types.ConnectorServer, error),
	runF func(ctx context.Context, cfg PtrT) error,
) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   name,
		Short: name,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(name, cmd, cfg)
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

			return runF(loggerCtx, cfg)
		},
	}

	grpcServerCmd := &cobra.Command{
		Use:    "_connector-service",
		Short:  "Start the connector service",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			v, err := loadConfig(name, cmd, cfg)
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
			cw, err := connector.NewWrapper(ctx, c)
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

	return cmd, nil
}

// loadConfig sets viper up to parse the config into the provided configuration object.
func loadConfig[T any, PtrT *T](name string, cmd *cobra.Command, cfg PtrT) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	v.SetConfigName(fmt.Sprintf(defaultConfigFilename, name))
	v.AddConfigPath(".")

	if err := v.ReadInConfig(); err != nil {
		if ok := !errors.Is(err, viper.ConfigFileNotFoundError{}); !ok {
			return nil, err
		}
	}

	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	if err := v.BindPFlags(cmd.PersistentFlags()); err != nil {
		return nil, err
	}
	if err := v.BindPFlags(cmd.Flags()); err != nil {
		return nil, err
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return v, nil
}
