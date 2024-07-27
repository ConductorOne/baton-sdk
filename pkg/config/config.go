package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func DefineConfiguration(
	ctx context.Context,
	connectorName string,
	connector cli.GetConnectorFunc,
	schema field.Configuration,
	options ...connectorrunner.Option,
) (*viper.Viper, *cobra.Command, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	path, name, err := cleanOrGetConfigPath(os.Getenv("BATON_CONFIG_PATH"))
	if err != nil {
		return nil, nil, err
	}

	v.SetConfigName(name)
	v.AddConfigPath(path)
	if err := v.ReadInConfig(); err != nil {
		if errors.Is(err, viper.ConfigFileNotFoundError{}) {
			return nil, nil, err
		}
	}
	v.SetEnvPrefix("baton")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	// add default fields and constrains
	schema.Fields = field.EnsureDefaultFieldsExists(schema.Fields)
	schema.Constraints = field.EnsureDefaultRelationships(schema.Constraints)

	// setup CLI with cobra
	mainCMD := &cobra.Command{
		Use:           connectorName,
		Short:         connectorName,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          cli.MakeMainCommand(ctx, connectorName, v, schema, connector, options...),
	}

	// add options to the main command
	for _, field := range schema.Fields {
		switch field.FieldType {
		case reflect.Bool:
			value, err := field.Bool()
			if err != nil {
				return nil, nil, fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			mainCMD.PersistentFlags().
				BoolP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
		case reflect.Int:
			value, err := field.Int()
			if err != nil {
				return nil, nil, fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			mainCMD.PersistentFlags().
				IntP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
		case reflect.String:
			value, err := field.String()
			if err != nil {
				return nil, nil, fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			mainCMD.PersistentFlags().
				StringP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
		case reflect.Slice:
			value, err := field.StringSlice()
			if err != nil {
				return nil, nil, fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			mainCMD.PersistentFlags().
				StringSliceP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
		default:
			return nil, nil, fmt.Errorf(
				"field %s, %s is not yet supported",
				field.FieldName,
				field.FieldType,
			)
		}

		// mark hidden
		if field.Hidden {
			err := mainCMD.PersistentFlags().MarkHidden(field.FieldName)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"cannot hide field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
		}

		// mark required
		if field.Required {
			if field.FieldType == reflect.Bool {
				return nil, nil, fmt.Errorf("requiring %s of type %s does not make sense", field.FieldName, field.FieldType)
			}

			err := mainCMD.MarkPersistentFlagRequired(field.FieldName)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"cannot require field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
		}
	}

	// apply constrains
	for _, constrain := range schema.Constraints {
		switch constrain.Kind {
		case field.MutuallyExclusive:
			mainCMD.MarkFlagsMutuallyExclusive(listFieldConstrainsAsStrings(constrain)...)
		case field.RequiredTogether:
			mainCMD.MarkFlagsRequiredTogether(listFieldConstrainsAsStrings(constrain)...)
		case field.AtLeastOne:
			mainCMD.MarkFlagsOneRequired(listFieldConstrainsAsStrings(constrain)...)
		case field.Dependents:
			// do nothing
		}
	}

	if err := v.BindPFlags(mainCMD.PersistentFlags()); err != nil {
		return nil, nil, err
	}
	if err := v.BindPFlags(mainCMD.Flags()); err != nil {
		return nil, nil, err
	}

	grpcServerCmd := &cobra.Command{
		Use:    "_connector-service",
		Short:  "Start the connector service",
		Hidden: true,
		RunE:   cli.MakeGRPCServerCommand(ctx, connectorName, v, schema, connector),
	}
	mainCMD.AddCommand(grpcServerCmd)

	capabilitiesCmd := &cobra.Command{
		Use:   "capabilities",
		Short: "Get connector capabilities",
		RunE:  cli.MakeCapabilitiesCommand(ctx, connectorName, v, connector),
	}
	mainCMD.AddCommand(capabilitiesCmd)

	mainCMD.AddCommand(cli.AdditionalCommands(name, schema.Fields)...)

	// NOTE (shackra): we don't check subcommands (i.e.: grpcServerCmd and capabilitiesCmd)
	mainCMD.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = mainCMD.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})

	return v, mainCMD, nil
}

func listFieldConstrainsAsStrings(constrains field.SchemaFieldRelationship) []string {
	var fields []string
	for _, v := range constrains.Fields {
		fields = append(fields, v.FieldName)
	}

	return fields
}

func cleanOrGetConfigPath(customPath string) (string, string, error) {
	if customPath != "" {
		cfgDir, cfgFile := filepath.Split(filepath.Clean(customPath))
		if cfgDir == "" {
			cfgDir = "."
		}

		ext := filepath.Ext(cfgFile)
		if ext == "" || (ext != ".yaml" && ext != ".yml") {
			return "", "", errors.New("expected config file to have .yaml or .yml extension")
		}

		return strings.TrimSuffix(
				cfgDir,
				string(filepath.Separator),
			), strings.TrimSuffix(
				cfgFile,
				ext,
			), nil
	}

	return ".", ".baton", nil
}
