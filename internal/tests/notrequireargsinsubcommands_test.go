package tests

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestCallSubCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	requiredField := field.StringField("name", field.WithRequired(true))
	carrier := field.NewConfiguration(
		[]field.SchemaField{
			requiredField,
		},
	)

	t.Run("should run «help» sub-command successfully", func(t *testing.T) {
		_, err := entrypoint(ctx, carrier, "help")
		require.NoError(t, err)
	})

	t.Run("should run «capabilities» sub-command without success", func(t *testing.T) {
		_, err := entrypoint(ctx, carrier, "capabilities")
		require.EqualError(t, err, "(Cobra) Execute failed: required flag(s) \"name\" not set")
	})

	t.Run("should run «completion zsh» sub-command successfully", func(t *testing.T) {
		_, err := entrypoint(ctx, carrier, "completion", "zsh")
		require.NoError(t, err)
	})
}
