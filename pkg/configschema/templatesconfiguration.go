package configschema

var configurationStructTemplate = `package {{.PackageName}}

import (
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Configuration struct {
{{range .Fields}}{{ if .Description }}	// {{.Description}}
{{end}}	{{ToCamelCase .FieldName}} {{.FieldType}}` + " `yaml:" + `"{{.FieldName}}"` + "`" + `
{{end}}
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
