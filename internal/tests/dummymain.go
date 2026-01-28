package tests

import (
	"context"
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/viper"
)

func entrypoint(ctx context.Context, cfg field.Configuration, options []connectorrunner.Option, args ...string) (*viper.Viper, error) {
	v, cmd, err := config.DefineConfigurationV2(ctx, "baton-dummy", getConnector, cfg, options...)
	if err != nil {
		return nil, fmt.Errorf("DefineConfiguration failed: %w", err)
	}

	cmd.Version = "testing"

	cmd.SetArgs(args)
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

func getConnector(ctx context.Context, v *viper.Viper, runTimeOpts cli.RunTimeOpts) (types.ConnectorServer, error) {
	dummyConnector := NewDummy()
	if v.GetString("auth-method") != runTimeOpts.SelectedAuthMethod {
		return nil, fmt.Errorf("incorrect authentication method passed")
	}

	c, err := connectorbuilder.NewConnector(ctx, dummyConnector)
	if err != nil {
		return nil, err
	}

	return c, nil
}
