package tests

import (
	"context"
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
)

func entrypoint(ctx context.Context, cfg field.Configuration, args ...string) (cli.ConnectorConfig, error) {
	var _cc cli.ConnectorConfig
	var getConnector = func(ctx context.Context, cc cli.ConnectorConfig) (types.ConnectorServer, error) {
		_cc = cc
		dummyConnector := NewDummy()

		c, err := connectorbuilder.NewConnector(ctx, dummyConnector)
		if err != nil {
			return nil, err
		}

		return c, nil
	}

	cmd, err := config.DefineConfiguration(ctx, "baton-dummy", getConnector, cfg)
	if err != nil {
		return nil, fmt.Errorf("DefineConfiguration failed: %w", err)
	}

	cmd.Version = "testing"

	cmd.SetArgs(args)
	err = cmd.ExecuteContext(ctx)
	if err != nil {
		// we don't want a full execution
		if errors.Is(err, context.DeadlineExceeded) {
			return _cc, nil
		}
		return nil, fmt.Errorf("(Cobra) Execute failed: %w", err)
	}
	return _cc, nil
}
