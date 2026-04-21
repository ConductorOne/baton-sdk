package cli

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type testLambdaConfig struct {
	ConfigPath string   `mapstructure:"config-path"`
	Name       string   `mapstructure:"name"`
	Tags       []string `mapstructure:"tags"`
	Payload    []byte   `mapstructure:"payload"`
}

func (c *testLambdaConfig) GetString(key string) string {
	switch key {
	case "config-path":
		return c.ConfigPath
	case "name":
		return c.Name
	default:
		return ""
	}
}

func (c *testLambdaConfig) GetBool(key string) bool {
	return false
}

func (c *testLambdaConfig) GetInt(key string) int {
	return 0
}

func (c *testLambdaConfig) GetStringSlice(key string) []string {
	switch key {
	case "tags":
		return append([]string(nil), c.Tags...)
	default:
		return nil
	}
}

func (c *testLambdaConfig) GetStringMap(key string) map[string]any {
	return nil
}

// setupCommand creates a cobra command with registered flags from the given schema,
// then calls VisitFlags to transfer viper values into pflag.
func setupCommand(t *testing.T, schema field.Configuration, v *viper.Viper) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "test"}
	err := SetFlagsAndConstraints(cmd, schema)
	require.NoError(t, err)
	VisitFlags(cmd, v)
	return cmd
}

func writeYAML(t *testing.T, dir, content string) string {
	t.Helper()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestVisitFlags_StringSlice_YAMLArray(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `custom-user-fields:
  - u_type
  - u_dept
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("custom-user-fields"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Equal(t, []string{"u_type", "u_dept"}, got)
}

func TestVisitFlags_StringSlice_YAMLInline(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `custom-user-fields: [u_type, u_dept]
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("custom-user-fields"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Equal(t, []string{"u_type", "u_dept"}, got)
}

func TestVisitFlags_StringSlice_SingleElement(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `tags:
  - only-one
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("tags"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("tags")
	require.NoError(t, err)
	require.Equal(t, []string{"only-one"}, got)
}

func TestVisitFlags_StringSlice_ValuesWithCommas(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `skip-ous:
  - "OU=Users,DC=example,DC=com"
  - "OU=Groups,DC=example,DC=com"
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("skip-ous"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("skip-ous")
	require.NoError(t, err)
	require.Equal(t, []string{"OU=Users,DC=example,DC=com", "OU=Groups,DC=example,DC=com"}, got)
}

func TestVisitFlags_StringSlice_CLICommaSeparated(t *testing.T) {
	// Simulates: --custom-user-fields u_type,u_dept
	// When passed via CLI, pflag parses the comma-separated value directly.
	// VisitFlags is not involved in this path. Verify pflag handles it correctly.
	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("custom-user-fields"),
		},
	}
	cmd := &cobra.Command{Use: "test"}
	err := SetFlagsAndConstraints(cmd, schema)
	require.NoError(t, err)

	// Simulate what pflag does when it receives --custom-user-fields u_type,u_dept
	require.NoError(t, cmd.Flags().Set("custom-user-fields", "u_type,u_dept"))

	got, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Equal(t, []string{"u_type", "u_dept"}, got)
}

func TestVisitFlags_StringToString_YAMLMap(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `labels:
  env: prod
  team: platform
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringMapField("labels"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringToString("labels")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod", "team": "platform"}, got)
}

func TestHydrateConnectorConfig_ViperConfigPreservesBootstrapFields(t *testing.T) {
	base := viper.New()
	base.Set("config-path", "/tmp/runtime/config.yaml")
	base.Set("bootstrap-js-path", "/tmp/runtime/bootstrap.js")
	base.Set("connector-js-path", "/tmp/runtime/connector.js")
	base.Set("jira-url", "https://old.example.com")

	snapshotConfig, err := structpb.NewStruct(map[string]any{
		"jira-url":       "https://new.example.com",
		"jira-api-token": "secret",
	})
	require.NoError(t, err)

	effective := HydrateConnectorConfig(CloneConfigSettings(base), &ConnectorConfigSnapshot{
		Config: snapshotConfig,
	})

	require.Equal(t, "/tmp/runtime/config.yaml", effective.GetString("config-path"))
	require.Equal(t, "/tmp/runtime/bootstrap.js", effective.GetString("bootstrap-js-path"))
	require.Equal(t, "/tmp/runtime/connector.js", effective.GetString("connector-js-path"))
	require.Equal(t, "https://new.example.com", effective.GetString("jira-url"))
	require.Equal(t, "secret", effective.GetString("jira-api-token"))
}

func TestHydrateConnectorConfig_TypedConfigRetainsBootstrapAndDecodeHooks(t *testing.T) {
	base := viper.New()
	base.Set("config-path", "/tmp/runtime/config.yaml")
	base.Set("name", "bootstrap-name")

	payload := []byte(`{"hello":"world"}`)
	snapshotConfig, err := structpb.NewStruct(map[string]any{
		"name":    "c1-name",
		"tags":    "alpha,beta",
		"payload": base64.StdEncoding.EncodeToString(payload),
	})
	require.NoError(t, err)

	effective := HydrateConnectorConfig(CloneConfigSettings(base), &ConnectorConfigSnapshot{
		Config: snapshotConfig,
	})

	cfg, err := MakeGenericConfiguration[*testLambdaConfig](
		effective,
		field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(false)),
	)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "/tmp/runtime/config.yaml", cfg.ConfigPath)
	require.Equal(t, "c1-name", cfg.Name)
	require.Equal(t, []string{"alpha", "beta"}, cfg.Tags)
	require.Equal(t, payload, cfg.Payload)
}

func TestVisitFlags_StringToString_SingleEntry(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `meta:
  version: "1"
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringMapField("meta"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringToString("meta")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"version": "1"}, got)
}

func TestVisitFlags_String_Unchanged(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `deployment: dev209168
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringField("deployment"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetString("deployment")
	require.NoError(t, err)
	require.Equal(t, "dev209168", got)
}

func TestVisitFlags_StringSlice_EnvVar(t *testing.T) {
	// Env vars are stored by viper as raw strings. GetStringSlice returns
	// a single-element slice, which is passed through as-is to pflag.
	// pflag then splits on commas, producing the expected multiple elements.
	v := viper.New()
	v.SetEnvPrefix("baton")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	t.Setenv("BATON_CUSTOM_USER_FIELDS", "u_type,u_dept")

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("custom-user-fields"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Equal(t, []string{"u_type", "u_dept"}, got)
}

func TestVisitFlags_StringSlice_EnvVar_CommaSeparatedWithSpaces(t *testing.T) {
	// Real-world case: env var with comma-separated values that contain spaces.
	// e.g. BATON_USER_BASED_SECURITY_GROUP_FILTER='Service Center Administrator,Alternate Approver'
	v := viper.New()
	v.SetEnvPrefix("baton")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	t.Setenv("BATON_USER_BASED_SECURITY_GROUP_FILTER", "Service Center Administrator,Alternate Approver")

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("user-based-security-group-filter"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("user-based-security-group-filter")
	require.NoError(t, err)
	require.Equal(t, []string{"Service Center Administrator", "Alternate Approver"}, got)
}

func TestVisitFlags_String_EnvVar(t *testing.T) {
	v := viper.New()
	v.SetEnvPrefix("baton")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()
	t.Setenv("BATON_DEPLOYMENT", "dev209168")

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringField("deployment"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetString("deployment")
	require.NoError(t, err)
	require.Equal(t, "dev209168", got)
}

func TestVisitFlags_UnsetFlagNotTouched(t *testing.T) {
	v := viper.New()
	// Don't set anything in viper

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("items"),
		},
	}
	cmd := setupCommand(t, schema, v)

	// Flag should still have its default (empty slice)
	got, err := cmd.Flags().GetStringSlice("items")
	require.NoError(t, err)
	require.Empty(t, got)
	require.False(t, cmd.Flags().Lookup("items").Changed)
}

func TestVisitFlags_MixedFlagTypes(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `deployment: dev209168
custom-user-fields:
  - u_type
  - u_dept
labels:
  env: prod
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringField("deployment"),
			field.StringSliceField("custom-user-fields"),
			field.StringMapField("labels"),
		},
	}
	cmd := setupCommand(t, schema, v)

	str, err := cmd.Flags().GetString("deployment")
	require.NoError(t, err)
	require.Equal(t, "dev209168", str)

	ss, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Equal(t, []string{"u_type", "u_dept"}, ss)

	sm, err := cmd.Flags().GetStringToString("labels")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"env": "prod"}, sm)
}

func TestVisitFlags_StringToString_DeterministicOrder(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `labels:
  alpha: a
  beta: b
  gamma: g
  delta: d
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringMapField("labels"),
		},
	}

	// Run multiple times to check map values survive regardless of iteration order
	for i := 0; i < 10; i++ {
		cmd := setupCommand(t, schema, v)
		got, err := cmd.Flags().GetStringToString("labels")
		require.NoError(t, err)

		keys := make([]string, 0, len(got))
		for k := range got {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		require.Equal(t, []string{"alpha", "beta", "delta", "gamma"}, keys)
		require.Equal(t, "a", got["alpha"])
		require.Equal(t, "b", got["beta"])
		require.Equal(t, "g", got["gamma"])
		require.Equal(t, "d", got["delta"])
	}
}

func TestVisitFlags_EmptyStringSlice_NotSet(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `custom-user-fields: []
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringSliceField("custom-user-fields"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringSlice("custom-user-fields")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestVisitFlags_EmptyStringMap_NotSet(t *testing.T) {
	dir := t.TempDir()
	writeYAML(t, dir, `labels: {}
`)
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(dir)
	require.NoError(t, v.ReadInConfig())

	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.StringMapField("labels"),
		},
	}
	cmd := setupCommand(t, schema, v)

	got, err := cmd.Flags().GetStringToString("labels")
	require.NoError(t, err)
	require.Empty(t, got)
}
