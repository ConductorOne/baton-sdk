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

	confschema := schema
	confschema.Fields = append(field.DefaultFields, confschema.Fields...)
	// Ensure unique fields
	uniqueFields := make(map[string]field.SchemaField)
	for _, f := range confschema.Fields {
		uniqueFields[f.FieldName] = f
	}
	confschema.Fields = make([]field.SchemaField, 0, len(uniqueFields))
	for _, f := range uniqueFields {
		confschema.Fields = append(confschema.Fields, f)
	}
	// setup CLI with cobra
	mainCMD := &cobra.Command{
		Use:           connectorName,
		Short:         connectorName,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          cli.MakeMainCommand(ctx, connectorName, v, confschema, connector, options...),
	}
	// set persistent flags only on the main subcommand
	err = setFlagsAndConstraints(mainCMD, field.NewConfiguration(field.DefaultFields, field.DefaultRelationships...))
	if err != nil {
		return nil, nil, err
	}

	// set the rest of flags
	err = setFlagsAndConstraints(mainCMD, schema)
	if err != nil {
		return nil, nil, err
	}

	grpcServerCmd := &cobra.Command{
		Use:    "_connector-service",
		Short:  "Start the connector service",
		Hidden: true,
		RunE:   cli.MakeGRPCServerCommand(ctx, connectorName, v, confschema, connector),
	}
	err = setFlagsAndConstraints(grpcServerCmd, schema)
	if err != nil {
		return nil, nil, err
	}
	mainCMD.AddCommand(grpcServerCmd)

	capabilitiesCmd := &cobra.Command{
		Use:   "capabilities",
		Short: "Get connector capabilities",
		RunE:  cli.MakeCapabilitiesCommand(ctx, connectorName, v, confschema, connector),
	}
	err = setFlagsAndConstraints(capabilitiesCmd, schema)
	if err != nil {
		return nil, nil, err
	}
	mainCMD.AddCommand(capabilitiesCmd)

	mainCMD.AddCommand(cli.AdditionalCommands(connectorName, schema.Fields)...)

	// NOTE(shackra): Set all values from Viper to the flags so
	// that Cobra won't complain that a flag is missing in case we
	// pass values through environment variables

	// main subcommand
	mainCMD.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = mainCMD.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})

	// children process subcommand
	grpcServerCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = grpcServerCmd.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})

	// capabilities subcommand
	capabilitiesCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = capabilitiesCmd.Flags().Set(f.Name, v.GetString(f.Name))
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

func setFlagsAndConstraints(command *cobra.Command, schema field.Configuration) error {
	// add options
	for _, field := range schema.Fields {
		switch field.FieldType {
		case reflect.Bool:
			value, err := field.Bool()
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			if field.Persistent {
				command.PersistentFlags().
					BoolP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			} else {
				command.Flags().
					BoolP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			}
		case reflect.Int:
			value, err := field.Int()
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			if field.Persistent {
				command.PersistentFlags().
					IntP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			} else {
				command.Flags().
					IntP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			}
		case reflect.String:
			value, err := field.String()
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			if field.Persistent {
				command.PersistentFlags().
					StringP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			} else {
				command.Flags().
					StringP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			}
		case reflect.Slice:
			value, err := field.StringSlice()
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					field.FieldName,
					field.FieldType,
					err,
				)
			}
			if field.Persistent {
				command.PersistentFlags().
					StringSliceP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			} else {
				command.Flags().
					StringSliceP(field.FieldName, field.CLIShortHand, value, field.GetDescription())
			}
		default:
			return fmt.Errorf(
				"field %s, %s is not yet supported",
				field.FieldName,
				field.FieldType,
			)
		}

		// mark hidden
		if field.Hidden {
			if field.Persistent {
				err := command.PersistentFlags().MarkHidden(field.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot hide persistent field %s, %s: %w",
						field.FieldName,
						field.FieldType,
						err,
					)
				}
			} else {
				err := command.Flags().MarkHidden(field.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot hide field %s, %s: %w",
						field.FieldName,
						field.FieldType,
						err,
					)
				}
			}
		}

		// mark required
		if field.Required {
			if field.FieldType == reflect.Bool {
				return fmt.Errorf("requiring %s of type %s does not make sense", field.FieldName, field.FieldType)
			}

			if field.Persistent {
				err := command.MarkPersistentFlagRequired(field.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot require persistent field %s, %s: %w",
						field.FieldName,
						field.FieldType,
						err,
					)
				}
			} else {
				err := command.MarkFlagRequired(field.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot require field %s, %s: %w",
						field.FieldName,
						field.FieldType,
						err,
					)
				}
			}
		}
	}

	// apply constrains
	for _, constrain := range schema.Constraints {
		switch constrain.Kind {
		case field.MutuallyExclusive:
			command.MarkFlagsMutuallyExclusive(listFieldConstrainsAsStrings(constrain)...)
		case field.RequiredTogether:
			command.MarkFlagsRequiredTogether(listFieldConstrainsAsStrings(constrain)...)
		case field.AtLeastOne:
			command.MarkFlagsOneRequired(listFieldConstrainsAsStrings(constrain)...)
		case field.Dependents:
			// do nothing
		default:
			return fmt.Errorf("invalid config")
		}
	}

	return nil
}
