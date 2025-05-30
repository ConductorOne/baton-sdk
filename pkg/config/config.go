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
	"github.com/spf13/viper"
)

func DefineConfiguration[T field.Configurable](
	ctx context.Context,
	connectorName string,
	connector cli.GetConnectorFunc[T],
	schema field.Configuration,
	options ...connectorrunner.Option,
) (*viper.Viper, *cobra.Command, error) {
	if err := verifyStructFields[T](schema); err != nil {
		return nil, nil, fmt.Errorf("VerifyStructFields failed: %w", err)
	}

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
		if s, ok := uniqueFields[f.FieldName]; ok {
			if !(f.WasReExported || s.WasReExported) {
				return nil, nil, fmt.Errorf("multiple fields with the same name: %s.If you want to use a default field in the SDK, use ExportAs on the connector schema field", f.FieldName)
			}
		}

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

	relationships := []field.SchemaFieldRelationship{}
	// set persistent flags only on the main subcommand
	relationships = append(relationships, field.DefaultRelationships...)
	relationships = append(relationships, confschema.Constraints...)

	err = cli.SetFlagsAndConstraints(mainCMD, field.NewConfiguration(confschema.Fields, field.WithConstraints(relationships...)))
	if err != nil {
		return nil, nil, err
	}

	mainCMD.AddCommand(cli.AdditionalCommands(connectorName, confschema.Fields)...)
	cli.VisitFlags(mainCMD, v)

	err = cli.OptionallyAddLambdaCommand(ctx, connectorName, v, connector, confschema, mainCMD)

	if err != nil {
		return nil, nil, err
	}

	_, err = cli.AddCommand(mainCMD, v, &schema, &cobra.Command{
		Use:    "_connector-service",
		Short:  "Start the connector service",
		Hidden: true,
		RunE:   cli.MakeGRPCServerCommand(ctx, connectorName, v, confschema, connector),
	})

	if err != nil {
		return nil, nil, err
	}

	_, err = cli.AddCommand(mainCMD, v, &schema, &cobra.Command{
		Use:   "capabilities",
		Short: "Get connector capabilities",
		RunE:  cli.MakeCapabilitiesCommand(ctx, connectorName, v, confschema, connector),
	})

	if err != nil {
		return nil, nil, err
	}

	_, err = cli.AddCommand(mainCMD, v, nil, &cobra.Command{
		Use:   "config",
		Short: "Get the connector config schema",
		RunE:  cli.MakeConfigSchemaCommand(ctx, connectorName, v, confschema, connector),
	})

	if err != nil {
		return nil, nil, err
	}

	return v, mainCMD, nil
}

func verifyStructFields[T field.Configurable](schema field.Configuration) error {
	// Verify that every field in the confschema has a corresponding struct tag in the struct defined in getconnector of type T
	//  or that it obeys the old interface, a *viper.Viper
	var config T // Create a zero-value instance of T
	tType := reflect.TypeOf(config)
	// Viper doesn't do struct fields
	if tType == reflect.TypeOf(&viper.Viper{}) {
		return nil
	}
	configType := reflect.TypeOf(config)
	if configType.Kind() == reflect.Ptr {
		configType = configType.Elem()
	}
	if configType.Kind() != reflect.Struct {
		return fmt.Errorf("T must be a struct type, got %v", configType.Kind())
	}
	for _, field := range schema.Fields {
		fieldFound := false
		for i := 0; i < configType.NumField(); i++ {
			structField := configType.Field(i)
			if structField.Tag.Get("mapstructure") == field.FieldName {
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return fmt.Errorf("field %s in confschema does not have a corresponding struct tag in the configuration struct", field.FieldName)
		}
	}
	return nil
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
