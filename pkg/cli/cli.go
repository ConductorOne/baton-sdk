package cli

import (
	"context"
	"fmt"
	"reflect"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type GetConnectorFunc[T field.Configurable] func(context.Context, T) (types.ConnectorServer, error)

func MakeGenericConfiguration[T field.Configurable](v *viper.Viper) (T, error) {
	// Create an instance of the struct type T using reflection
	var config T // Create a zero-value instance of T

	// Is it a *Viper?
	if reflect.TypeOf(config) == reflect.TypeOf((*viper.Viper)(nil)) {
		if t, ok := any(v).(T); ok {
			return t, nil
		}
		return config, fmt.Errorf("cannot convert *viper.Viper to %T", config)
	}

	// Unmarshal into the config struct
	err := v.Unmarshal(&config)
	if err != nil {
		return config, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	return config, nil
}

// NOTE(shackra): Set all values from Viper to the flags so...
// that Cobra won't complain that a flag is missing in case we...
// pass values through environment variables.
func VisitFlags(cmd *cobra.Command, v *viper.Viper) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = cmd.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})
}

func AddCommand(mainCMD *cobra.Command, v *viper.Viper, schema *field.Configuration, subCMD *cobra.Command) (*cobra.Command, error) {
	mainCMD.AddCommand(subCMD)
	if schema != nil {
		err := SetFlagsAndConstraints(subCMD, *schema)
		if err != nil {
			return nil, err
		}
	}
	VisitFlags(subCMD, v)

	return subCMD, nil
}
func SetFlagsAndConstraints(command *cobra.Command, schema field.Configuration) error {
	// add options
	for _, f := range schema.Fields {
		switch f.Variant {
		case field.BoolVariant:
			value, err := field.GetDefaultValue[bool](f)
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					f.FieldName,
					f.Variant,
					err,
				)
			}
			if f.IsPersistent() {
				command.PersistentFlags().
					BoolP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			} else {
				command.Flags().
					BoolP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			}
		case field.IntVariant:
			value, err := field.GetDefaultValue[int](f)
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					f.FieldName,
					f.Variant,
					err,
				)
			}
			if f.IsPersistent() {
				command.PersistentFlags().
					IntP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			} else {
				command.Flags().
					IntP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			}
		case field.StringVariant:
			value, err := field.GetDefaultValue[string](f)
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					f.FieldName,
					f.Variant,
					err,
				)
			}
			if f.IsPersistent() {
				command.PersistentFlags().
					StringP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			} else {
				command.Flags().
					StringP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			}

		case field.StringSliceVariant:
			value, err := field.GetDefaultValue[[]string](f)
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					f.FieldName,
					f.Variant,
					err,
				)
			}
			if f.IsPersistent() {
				command.PersistentFlags().
					StringSliceP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			} else {
				command.Flags().
					StringSliceP(f.FieldName, f.GetCLIShortHand(), *value, f.GetDescription())
			}
		case field.StringMapVariant:
			value, err := field.GetDefaultValue[map[string]any](f)
			if err != nil {
				return fmt.Errorf(
					"field %s, %s: %w",
					f.FieldName,
					f.Variant,
					err,
				)
			}
			strMap := make(map[string]string)
			for k, v := range *value {
				switch val := v.(type) {
				case string:
					strMap[k] = val
				case int:
					strMap[k] = fmt.Sprintf("%d", val)
				case bool:
					strMap[k] = fmt.Sprintf("%v", val)
				case float64:
					strMap[k] = fmt.Sprintf("%g", val)
				default:
					strMap[k] = fmt.Sprintf("%v", val)
				}
			}
			if f.IsPersistent() {
				command.PersistentFlags().
					StringToStringP(f.FieldName, f.GetCLIShortHand(), strMap, f.GetDescription())
			} else {
				command.Flags().
					StringToStringP(f.FieldName, f.GetCLIShortHand(), strMap, f.GetDescription())
			}
		default:
			return fmt.Errorf(
				"field %s, %s is not yet supported",
				f.FieldName,
				f.Variant,
			)
		}

		// mark hidden
		if f.IsHidden() {
			if f.IsPersistent() {
				err := command.PersistentFlags().MarkHidden(f.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot hide persistent field %s, %s: %w",
						f.FieldName,
						f.Variant,
						err,
					)
				}
			} else {
				err := command.Flags().MarkHidden(f.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot hide field %s, %s: %w",
						f.FieldName,
						f.Variant,
						err,
					)
				}
			}
		}

		// mark required
		if f.Required {
			if f.Variant == field.BoolVariant {
				return fmt.Errorf("requiring %s of type %s does not make sense", f.FieldName, f.Variant)
			}

			if f.IsPersistent() {
				err := command.MarkPersistentFlagRequired(f.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot require persistent field %s, %s: %w",
						f.FieldName,
						f.Variant,
						err,
					)
				}
			} else {
				err := command.MarkFlagRequired(f.FieldName)
				if err != nil {
					return fmt.Errorf(
						"cannot require field %s, %s: %w",
						f.FieldName,
						f.Variant,
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

func listFieldConstrainsAsStrings(constrains field.SchemaFieldRelationship) []string {
	var fields []string
	for _, v := range constrains.Fields {
		fields = append(fields, v.FieldName)
	}

	return fields
}
