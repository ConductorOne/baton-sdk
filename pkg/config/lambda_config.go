package config

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
)

func DefineLambdaServerConfiguration(
	ctx context.Context,
	connectorName string,
	connector cli.GetConnectorFunc,
	connectorConfSchema field.Configuration,
	options ...connectorrunner.Option,
) (*viper.Viper, *cobra.Command, error) {
	v := viper.New()
	v.SetEnvPrefix("BATON")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	lambdaConfSchema := field.NewConfiguration(field.LambdaServerFields(), field.LambdaServerRelationships...)

	// setup CLI with cobra
	mainCMD := &cobra.Command{
		Use:           connectorName,
		Short:         connectorName,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          cli.MakeLambdaServerCommand(ctx, connectorName, v, lambdaConfSchema, connector, connectorConfSchema),
	}

	// set persistent flags only on the main subcommand
	err := setFlagsAndConstraints(mainCMD, lambdaConfSchema)
	if err != nil {
		return nil, nil, err
	}

	// main subcommand
	mainCMD.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = mainCMD.PersistentFlags().Set(f.Name, v.GetString(f.Name))
		}
	})

	mainCMD.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = mainCMD.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})

	return v, mainCMD, nil
}

func DefineLambdaClientConfiguration(
	ctx context.Context,
	connectorName string,
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
	confschema.Fields = append(confschema.Fields, field.LambdaClientFields()...)
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
		RunE:          cli.MakeLambdaMainCommand(ctx, connectorName, v, confschema, options...),
	}
	// set persistent flags only on the main subcommand
	defaultFields := append(field.DefaultFields, field.LambdaClientFields()...)
	err = setFlagsAndConstraints(mainCMD, field.NewConfiguration(defaultFields, field.DefaultRelationships...))
	if err != nil {
		return nil, nil, err
	}

	// set the rest of flags
	err = setFlagsAndConstraints(mainCMD, schema)
	if err != nil {
		return nil, nil, err
	}

	metadataCmd := &cobra.Command{
		Use:   "metadata",
		Short: "Get connector metadata",
		RunE:  cli.MakeLambdaMetadataCommand(ctx, connectorName, v, confschema),
	}
	err = setFlagsAndConstraints(metadataCmd, schema)
	if err != nil {
		return nil, nil, err
	}
	mainCMD.AddCommand(metadataCmd)

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

	metadataCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if v.IsSet(f.Name) {
			_ = metadataCmd.Flags().Set(f.Name, v.GetString(f.Name))
		}
	})

	return v, mainCMD, nil
}
