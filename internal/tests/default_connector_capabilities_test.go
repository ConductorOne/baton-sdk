package tests

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

func TestDefaultConnectorCapabilities(t *testing.T) {
	ctx := t.Context()

	requiredField := field.StringField("name", field.WithRequired(true))
	carrier := field.NewConfiguration(
		[]field.SchemaField{
			requiredField,
		},
	)

	t.Run("should run «capabilities» sub-command successfully", func(t *testing.T) {
		_, err := entrypoint(ctx, carrier, []connectorrunner.Option{
			connectorrunner.WithDefaultCapabilitiesConnectorBuilder(&Dummy{}),
		}, "capabilities")
		require.NoError(t, err)
	})
}
