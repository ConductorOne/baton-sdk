package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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
