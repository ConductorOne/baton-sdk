package tests

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/viper"
)

func entrypoint(cfg field.Configuration) (*viper.Viper, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()

	v, cmd, err := config.DefineConfiguration(ctx, "baton-dummy", getConnector, cfg)
	if err != nil {
		return nil, fmt.Errorf("DefineConfiguration failed: %w", err)
	}

	cmd.Version = "testing"

	err = cmd.Execute()
	if err != nil {
		// we don't want a full execution
		if errors.Is(err, context.DeadlineExceeded) {
			return v, nil
		}
		return nil, fmt.Errorf("(Cobra) Execute failed: %w", err)
	}
	return v, nil
}

func getConnector(ctx context.Context, v *viper.Viper) (types.ConnectorServer, error) {
	dummyConnector := NewDummy()

	c, err := connectorbuilder.NewConnector(ctx, dummyConnector)
	if err != nil {
		return nil, err
	}

	return c, nil
}
