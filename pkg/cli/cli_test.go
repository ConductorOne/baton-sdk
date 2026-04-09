package cli

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

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
	require.NoError(t, os.WriteFile(p, []byte(content), 0o644))
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
