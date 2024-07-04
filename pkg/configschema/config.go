package configschema

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/commands"
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func DefineConfiguration(connectorName string, connector commands.GetConnectorFunc, fields []SchemaField, constrains ...SchemaFieldRelationship) (*viper.Viper, *cobra.Command, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	path, name, err := config.CleanOrGetConfigPath(os.Getenv("BATON_CONFIG_PATH"))
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
	fields = ensureDefaultFieldsExists(fields)
	constrains = ensureDefaultRelationships(constrains)

	// setup CLI with cobra
	mainCMD := &cobra.Command{
		Use:           connectorName,
		Short:         connectorName,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          commands.MakeMainCommand(context.Background(), connectorName, v, nil, nil),
	}

	// add options to the main command
	for _, field := range fields {
		switch field.FieldType {
		case reflect.Bool:
			value, err := field.Bool()
			if err != nil {
				return nil, nil, fmt.Errorf("field %s, %s: %w", field.FieldName, field.FieldType, err)
			}
			mainCMD.PersistentFlags().BoolP(field.FieldName, field.CLIShortHand, value, field.Description)
		case reflect.Int:
			value, err := field.Int()
			if err != nil {
				return nil, nil, fmt.Errorf("field %s, %s: %w", field.FieldName, field.FieldType, err)
			}
			mainCMD.PersistentFlags().IntP(field.FieldName, field.CLIShortHand, value, field.Description)
		case reflect.String:
			value, err := field.String()
			if err != nil {
				return nil, nil, fmt.Errorf("field %s, %s: %w", field.FieldName, field.FieldType, err)
			}
			mainCMD.PersistentFlags().StringP(field.FieldName, field.CLIShortHand, value, field.Description)
		default:
			return nil, nil, fmt.Errorf("field %s, %s is not yet supported", field.FieldName, field.FieldType)
		}

		// mark hidden
		if field.Hidden {
			err := mainCMD.PersistentFlags().MarkHidden(field.FieldName)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot hide field %s, %s: %w", field.FieldName, field.FieldType, err)
			}
		}
	}

	// apply constrains
	for _, constrain := range constrains {
		switch constrain.Kind {
		case MutuallyExclusive:
			mainCMD.MarkFlagsMutuallyExclusive(listFieldConstrainsAsStrings(constrain)...)
		case RequiredTogether:
			mainCMD.MarkFlagsRequiredTogether(listFieldConstrainsAsStrings(constrain)...)
		}
	}

	// TODO (shackra): add the gRPC command and the capabilities command

	return v, mainCMD, nil
}

func listFieldConstrainsAsStrings(constrains SchemaFieldRelationship) []string {
	var fields []string
	for _, v := range constrains.Fields {
		fields = append(fields, v.FieldName)
	}

	return fields
}
