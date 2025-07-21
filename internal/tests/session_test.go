package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func sessionGetConnector(ctx context.Context, v *viper.Viper, sessionCacheConstructor types.SessionCacheConstructor) (types.ConnectorServer, error) {
	sessionDummyConnector := NewDummy()

	_, err := sessionCacheConstructor(ctx)
	if err != nil {
		return nil, err
	}

	connector, err := connectorbuilder.NewConnector(ctx, sessionDummyConnector)
	if err != nil {
		return nil, err
	}

	return connector, nil
}

func sessionEntrypoint(ctx context.Context, cfg field.Configuration, t *testing.T) (*viper.Viper, error) {
	v, cmd, err := config.DefineConfiguration(ctx, "baton-dummy", sessionGetConnector, cfg)
	if err != nil {
		return nil, fmt.Errorf("DefineConfiguration failed: %w", err)
	}

	cmd.Version = "testing"
	err = cmd.ExecuteContext(ctx)
	if err != nil {
		// we don't want a full execution
		if errors.Is(err, context.DeadlineExceeded) {
			return v, nil
		}
		return nil, fmt.Errorf("(Cobra) Execute failed: %w", err)
	}
	return v, nil
}

func TestSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	carrier := field.NewConfiguration(
		[]field.SchemaField{},
	)

	_, err := sessionEntrypoint(
		ctx,
		carrier,
		t,
	)

	require.NoError(t, err)
}
