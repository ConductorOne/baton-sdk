package configschema

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

var renderConfigExpected = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package main

import (
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Configuration struct {
	Field1String string ` + "`yaml:\"field-1-string\"`" + `
	// this is a description
	Field2Int int ` + "`yaml:\"field-2-int\"`" + `
}

func NewConfiguration(cmd *cobra.Command) (*Configuration, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	cfgPath, cfgName, err := config.CleanOrGetConfigPath(os.Getenv("BATON_CONFIG_PATH"))
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

	var cfg *Configuration
	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
`

var renderCLIExpected = `// THIS FILE WAS AUTO GENERATED. DO NOT EDIT
package main

import (
	"github.com/spf13/cobra"
)

func NewCLI(cmd *cobra.Command) {
	cmd.PersistentFlags().String("field-1-string",
		"value",
		"($BATON_FIELD_1_STRING)")
	cmd.PersistentFlags().Int("field-2-int",
		0,
		"this is a description ($BATON_FIELD_2_INT)")

}
`

func TestRenderConfig(t *testing.T) {
	fields := []SchemaField{
		StringField("field-1-string", WithDefaultValue("value")),
		IntField("field-2-int", WithDescription("this is a description")),
	}

	b := bytes.NewBuffer(nil)

	err := renderConfig(TemplateData{PackageName: "main", Fields: fields}, b)
	require.NoError(t, err)

	require.Equal(t, renderConfigExpected, b.String())
}

func TestRenderCobra(t *testing.T) {
	fields := []SchemaField{
		StringField("field-1-string", WithDefaultValue("value")),
		IntField("field-2-int", WithDescription("this is a description")),
	}

	b := bytes.NewBuffer(nil)

	err := renderCLI(TemplateData{PackageName: "main", Fields: fields}, b)
	require.NoError(t, err)

	require.Equal(t, renderCLIExpected, b.String())
}