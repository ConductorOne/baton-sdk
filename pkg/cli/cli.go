package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	envPrefix        = "baton"
	defaultLogLevel  = "info"
	defaultLogFormat = logging.LogFormatJSON
)

type BaseConfig struct {
	LogLevel           string `mapstructure:"log-level"`
	LogFormat          string `mapstructure:"log-format"`
	C1zPath            string `mapstructure:"file"`
	GrantEntitlementID string `mapstructure:"grant-entitlement"`
	GrantPrincipalID   string `mapstructure:"grant-principal"`
	GrantPrincipalType string `mapstructure:"grant-principal-type"`
	RevokeGrantID      string `mapstructure:"revoke-grant"`
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

			return runF(loggerCtx, cfg)
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
	cmd.PersistentFlags().String("grant-entitlement", "", "The entitlement to grant to the supplied principal ($BATON_GRANT_ENTITLEMENT)")
	cmd.PersistentFlags().String("grant-principal", "", "The resource to grant the entitlement to ($BATON_GRANT_PRINCIPAL)")
	cmd.PersistentFlags().String("grant-principal-type", "", "The resource type of the principal to grant the entitlement to ($BATON_GRANT_PRINCIPAL_TYPE)")
	cmd.MarkFlagsRequiredTogether("grant-entitlement", "grant-principal", "grant-principal-type")

	cmd.PersistentFlags().String("revoke-grant", "", "The grant to revoke ($BATON_REVOKE_GRANT)")
	cmd.MarkFlagsMutuallyExclusive("grant-entitlement", "revoke-grant")
	return cmd, nil
}

func getConfigPath(customPath string) (string, string, error) {
	if customPath != "" {
		cfgDir, cfgFile := filepath.Split(filepath.Clean(customPath))
		if cfgDir == "" {
			cfgDir = "."
		}

		ext := filepath.Ext(cfgFile)
		if ext != ".yaml" && ext != ".yml" {
			return "", "", errors.New("expected config file to have .yaml or .yml extension")
		}

		return strings.TrimSuffix(cfgDir, string(filepath.Separator)), strings.TrimSuffix(cfgFile, ext), nil
	}

	return ".", ".baton", nil
}

// loadConfig sets viper up to parse the config into the provided configuration object.
func loadConfig[T any, PtrT *T](cmd *cobra.Command, cfg PtrT) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	cfgPath, cfgName, err := getConfigPath(os.Getenv("BATON_CONFIG_PATH"))
	if err != nil {
		return nil, err
	}

	v.SetConfigName(cfgName)
	v.AddConfigPath(cfgPath)

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
