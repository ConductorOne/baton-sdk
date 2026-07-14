package cli

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/field"
)

// TestWithConnectorDefault_WorkerCount pins the per-connector default
// mechanism end to end at the flag layer: the connector's default value
// lands on the cobra flag (so --help shows it and viper resolves it when
// nothing else is set), while explicit values — including the documented
// zero sentinel for sequential sync — still win.
func TestWithConnectorDefault_WorkerCount(t *testing.T) {
	newCmdAndViper := func(t *testing.T) (*viper.Viper, *field.SchemaField) {
		t.Helper()
		f := field.WithConnectorDefault(field.WorkerCountField, 16)
		return viper.New(), &f
	}

	t.Run("default flows to flag, help, and viper", func(t *testing.T) {
		v, f := newCmdAndViper(t)
		cmd := setupCommand(t, field.Configuration{Fields: []field.SchemaField{*f}}, v)

		flag := cmd.PersistentFlags().Lookup("workers")
		require.NotNil(t, flag)
		require.Equal(t, "16", flag.DefValue, "connector default must be the flag default (what --help renders)")
		require.False(t, flag.Changed)

		require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
		require.Equal(t, 16, v.GetInt("workers"), "unset resolves to the connector default")
	})

	t.Run("explicit zero beats the connector default (sequential stays reachable)", func(t *testing.T) {
		v, f := newCmdAndViper(t)
		cmd := setupCommand(t, field.Configuration{Fields: []field.SchemaField{*f}}, v)

		require.NoError(t, cmd.PersistentFlags().Set("workers", "0"))
		require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
		require.Equal(t, 0, v.GetInt("workers"))
	})

	t.Run("explicit value beats the connector default", func(t *testing.T) {
		v, f := newCmdAndViper(t)
		cmd := setupCommand(t, field.Configuration{Fields: []field.SchemaField{*f}}, v)

		require.NoError(t, cmd.PersistentFlags().Set("workers", "4"))
		require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
		require.Equal(t, 4, v.GetInt("workers"))
	})

	t.Run("config file beats the connector default", func(t *testing.T) {
		dir := t.TempDir()
		writeYAML(t, dir, "workers: 8\n")
		v := viper.New()
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(dir)
		require.NoError(t, v.ReadInConfig())

		f := field.WithConnectorDefault(field.WorkerCountField, 16)
		cmd := setupCommand(t, field.Configuration{Fields: []field.SchemaField{f}}, v)
		require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
		require.Equal(t, 8, v.GetInt("workers"))
	})

	t.Run("original shared field is untouched", func(t *testing.T) {
		require.Equal(t, 0, field.WorkerCountField.DefaultValue)
		require.False(t, field.WorkerCountField.WasReExported)
		f := field.WithConnectorDefault(field.WorkerCountField, 16)
		require.Equal(t, 16, f.DefaultValue)
		require.True(t, f.WasReExported, "the copy must be re-exported so DefineConfiguration replaces the SDK copy instead of erroring on the duplicate")
	})
}
