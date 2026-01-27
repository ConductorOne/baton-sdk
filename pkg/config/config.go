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
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func RunConnector[T field.Configurable](
	ctx context.Context,
	connectorName string,
	version string,
	schema field.Configuration,
	cf cli.NewConnector[T],
	options ...connectorrunner.Option,
) {
	f := func(ctx context.Context, cfg T, runTimeOpts cli.RunTimeOpts) (types.ConnectorServer, error) {
		l := ctxzap.Extract(ctx)
		connector, builderOpts, err := cf(ctx, cfg, &cli.ConnectorOpts{TokenSource: runTimeOpts.TokenSource})
		if err != nil {
			return nil, err
		}

		builderOpts = append(builderOpts, connectorbuilder.WithSessionStore(runTimeOpts.SessionStore))

		c, err := connectorbuilder.NewConnector(ctx, connector, builderOpts...)
		if err != nil {
			l.Error("error creating connector", zap.Error(err))
			return nil, err
		}
		return c, nil
	}

	_, cmd, err := DefineConfigurationV2(ctx, connectorName, f, schema, options...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
		return
	}

	cmd.Version = version

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var ErrDuplicateField = errors.New("multiple fields with the same name")

// GetConnectorFunc is a function type that creates a connector instance.
// It takes a context and configuration. The session cache constructor is retrieved from the context.
// deprecated - prefer RunConnector.
func DefineConfiguration[T field.Configurable](
	ctx context.Context,
	connectorName string,
	connector cli.GetConnectorFunc[T],
	schema field.Configuration,
	options ...connectorrunner.Option,
) (*viper.Viper, *cobra.Command, error) {
	f := func(ctx context.Context, cfg T, runTimeOpts cli.RunTimeOpts) (types.ConnectorServer, error) {
		connector, err := connector(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return connector, nil
	}
	return DefineConfigurationV2(ctx, connectorName, f, schema, options...)
}

// deprecated - prefer RunConnector.
func DefineConfigurationV2[T field.Configurable](
	ctx context.Context,
	connectorName string,
	connector cli.GetConnectorFunc2[T],
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

	defaultFieldsByName := make(map[string]field.SchemaField)
	for _, f := range field.DefaultFields {
		if _, ok := defaultFieldsByName[f.FieldName]; ok {
			return nil, nil, fmt.Errorf("multiple default fields with the same name: %s", f.FieldName)
		}
		defaultFieldsByName[f.FieldName] = f
	}

	confschema := schema
	confschema.Fields = append(field.DefaultFields, confschema.Fields...)
	// Ensure unique fields
	uniqueFields := make(map[string]field.SchemaField)
	fieldsToDelete := make(map[string]bool)
	for _, f := range confschema.Fields {
		if existingField, ok := uniqueFields[f.FieldName]; ok {
			// If the duplicate field is not a default field, error.
			if _, ok := defaultFieldsByName[f.FieldName]; !ok {
				return nil, nil, fmt.Errorf("%w: %s", ErrDuplicateField, f.FieldName)
			}
			// If redeclaring a default field and not reexporting it, error.
			if !f.WasReExported {
				return nil, nil, fmt.Errorf("%w: %s. If you want to use a default field in the SDK, use ExportAs on the connector schema field", ErrDuplicateField, f.FieldName)
			}
			if existingField.WasReExported {
				return nil, nil, fmt.Errorf("%w: %s. If you want to use a default field in the SDK, use ExportAs on the connector schema field", ErrDuplicateField, f.FieldName)
			}

			fieldsToDelete[existingField.FieldName] = true
		}

		uniqueFields[f.FieldName] = f
	}

	// Filter out fields that were not reexported and were in the fieldsToDelete list.
	fields := make([]field.SchemaField, 0, len(confschema.Fields))
	for _, f := range confschema.Fields {
		if !f.WasReExported && fieldsToDelete[f.FieldName] {
			continue
		}
		fields = append(fields, f)
	}
	confschema.Fields = fields

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

	err = cli.SetFlagsAndConstraints(
		mainCMD,
		field.NewConfiguration(
			confschema.Fields,
			field.WithConstraints(relationships...),
			field.WithFieldGroups(confschema.FieldGroups),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	mainCMD.AddCommand(cli.AdditionalCommands(connectorName, confschema.Fields)...)
	cli.VisitFlags(mainCMD, v)

	sessionStoreEnabled, err := connectorrunner.IsSessionStoreEnabled(ctx, options...)
	if err != nil {
		return nil, nil, err
	}

	err = cli.OptionallyAddLambdaCommand(ctx, connectorName, v, connector, confschema, mainCMD, sessionStoreEnabled)

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

	defaultConnector, err := connectorrunner.ExtractDefaultConnector(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	if defaultConnector == nil {
		_, err = cli.AddCommand(mainCMD, v, &schema, &cobra.Command{
			Use:   "capabilities",
			Short: "Get connector capabilities",
			RunE:  cli.MakeCapabilitiesCommand(ctx, connectorName, v, confschema, connector),
		})
		if err != nil {
			return nil, nil, err
		}
	} else {
		// We don't want to use cli.AddCommand here because we don't want to validate config flags
		// So we can call capabilities even with incomplete config
		mainCMD.AddCommand(&cobra.Command{
			Use:   "capabilities",
			Short: "Get connector capabilities",
			RunE:  cli.MakeCapabilitiesCommand(ctx, connectorName, v, confschema, connector, options...),
		})
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
		return fmt.Errorf("T must be a struct type, got %v", configType.Kind()) //nolint:staticcheck // we want to capital letter here
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
