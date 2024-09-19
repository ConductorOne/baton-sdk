package tests

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestCallSubCommandHelp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeoutIn)
	defer cancel()

	requiredField := field.StringField("name", field.WithRequired(true))

	t.Run("should run help sub-command successfully", func(t *testing.T) {
		carrier := field.NewConfiguration(
			[]field.SchemaField{
				requiredField,
			},
		)
		_, err := entrypoint(ctx, carrier, "help")
		require.NoError(t, err)
	})
}
