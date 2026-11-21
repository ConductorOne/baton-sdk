package cli

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestBoolField_BareFlagParses is a regression guard for the --event-feed
// fix: a StringField read via v.GetBool failed to parse a bare flag (pflag
// requires string flags to take an argument), while a BoolField parses a
// bare flag as true.
func TestBoolField_BareFlagParses(t *testing.T) {
	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.BoolField("event-feed"),
		},
	}
	cmd := &cobra.Command{Use: "test"}
	require.NoError(t, SetFlagsAndConstraints(cmd, schema))

	require.NoError(t, cmd.ParseFlags([]string{"--event-feed"}))

	got, err := cmd.Flags().GetBool("event-feed")
	require.NoError(t, err)
	require.True(t, got)
}

func TestBoolField_ExplicitTrueStillParses(t *testing.T) {
	schema := field.Configuration{
		Fields: []field.SchemaField{
			field.BoolField("event-feed"),
		},
	}
	cmd := &cobra.Command{Use: "test"}
	require.NoError(t, SetFlagsAndConstraints(cmd, schema))

	require.NoError(t, cmd.ParseFlags([]string{"--event-feed=true"}))

	got, err := cmd.Flags().GetBool("event-feed")
	require.NoError(t, err)
	require.True(t, got)
}
