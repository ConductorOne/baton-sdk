package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type BaseConfig struct {
	LogLevel           string `mapstructure:"log-level"`
	LogFormat          string `mapstructure:"log-format"`
	C1zPath            string `mapstructure:"file"`
	ClientID           string `mapstructure:"client-id"`
	ClientSecret       string `mapstructure:"client-secret"`
	GrantEntitlementID string `mapstructure:"grant-entitlement"`
	GrantPrincipalID   string `mapstructure:"grant-principal"`
	GrantPrincipalType string `mapstructure:"grant-principal-type"`
	RevokeGrantID      string `mapstructure:"revoke-grant"`
	C1zTempDir         string `mapstructure:"c1z-temp-dir"`
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

func configToCmdFlags[T any, PtrT *T](cmd *cobra.Command, cfg PtrT) error {
	baseConfigFields := reflect.VisibleFields(reflect.TypeOf(BaseConfig{}))
	baseConfigFieldsMap := make(map[string]bool)
	for _, field := range baseConfigFields {
		baseConfigFieldsMap[field.Name] = true
	}

	fields := reflect.VisibleFields(reflect.TypeOf(*cfg))
	for _, field := range fields {
		// ignore BaseConfig fields
		if _, ok := baseConfigFieldsMap[field.Name]; ok {
			continue
		}
		if field.Name == "BaseConfig" {
			continue
		}

		cfgField := field.Tag.Get("mapstructure")
		if cfgField == "" {
			return fmt.Errorf("mapstructure tag is required on config field %s", field.Name)
		}
		description := field.Tag.Get("description")
		if description == "" {
			// Skip fields without descriptions for backwards compatibility
			continue
		}
		defaultValueStr := field.Tag.Get("defaultValue")

		envVarName := strings.ReplaceAll(strings.ToUpper(cfgField), "-", "_")
		description = fmt.Sprintf("%s ($BATON_%s)", description, envVarName)
		switch field.Type.Kind() {
		case reflect.String:
			cmd.PersistentFlags().String(cfgField, defaultValueStr, description)
		case reflect.Bool:
			defaultValue, err := strconv.ParseBool(defaultValueStr)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Bool(cfgField, defaultValue, description)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			defaultValue, err := strconv.ParseInt(defaultValueStr, 10, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Int64(cfgField, defaultValue, description)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			defaultValue, err := strconv.ParseUint(defaultValueStr, 10, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Uint64(cfgField, defaultValue, description)
		case reflect.Float32, reflect.Float64:
			defaultValue, err := strconv.ParseFloat(defaultValueStr, 64)
			if defaultValueStr != "" && err != nil {
				return fmt.Errorf("invalid default value for config field %s: %w", field.Name, err)
			}
			cmd.PersistentFlags().Float64(cfgField, defaultValue, description)
		default:
			return fmt.Errorf("unsupported type %s for config field %s", field.Type.Kind(), field.Name)
		}
	}

	return nil
}
