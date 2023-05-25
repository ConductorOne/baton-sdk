package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type BaseConfig struct {
	LogLevel           string `mapstructure:"log-level"`
	LogFormat          string `mapstructure:"log-format"`
	C1zPath            string `mapstructure:"file"`
	DaemonMode         bool   `mapstructure:"daemonize"`
	ClientID           string `mapstructure:"client-id"`
	ClientSecret       string `mapstructure:"client-secret"`
	GrantEntitlementID string `mapstructure:"grant-entitlement"`
	GrantPrincipalID   string `mapstructure:"grant-principal"`
	GrantPrincipalType string `mapstructure:"grant-principal-type"`
	RevokeGrantID      string `mapstructure:"revoke-grant"`
}

func getConfigPath(customPath string) (string, string, error) {
	if customPath != "" {
		cfgDir, cfgFile := filepath.Split(filepath.Clean(customPath))
		if cfgDir == "" {
			cfgDir = "."
		}

		ext := filepath.Ext(cfgFile)
		if ext == "" || (ext != ".yaml" && ext != ".yml") {
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
