package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	cfg "github.com/conductorone/baton-appstoreconnect/pkg/config"
	"github.com/conductorone/baton-appstoreconnect/pkg/connector"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
)

var version = "dev"

func main() {
	ctx := context.Background()

	config.RunConnector(
		ctx,
		"baton-appstoreconnect",
		version,
		cfg.Config,
		getConnector,
		connectorrunner.WithDefaultCapabilitiesConnectorBuilderV2(&connector.AppStoreConnect{}),
		connectorrunner.WithSessionStoreEnabled(),
	)
}

func getConnector(ctx context.Context, ascConfig *cfg.AppStoreConnect, _ *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	privateKey, err := resolvePrivateKey(ascConfig)
	if err != nil {
		return nil, nil, err
	}

	c, err := connector.New(ctx, connector.Config{
		KeyID:         ascConfig.KeyId,
		IssuerID:      ascConfig.IssuerId,
		PrivateKeyPEM: privateKey,
		BaseURL:       ascConfig.BaseUrl,
	})
	if err != nil {
		return nil, nil, err
	}

	return c, nil, nil
}

// resolvePrivateKey returns the PEM key material, whether it was pasted into the config or left on
// disk for the CLI to read.
func resolvePrivateKey(ascConfig *cfg.AppStoreConnect) (string, error) {
	if key := strings.TrimSpace(string(ascConfig.PrivateKey)); key != "" {
		return key, nil
	}

	path := strings.TrimSpace(ascConfig.PrivateKeyPath)
	if path == "" {
		return "", fmt.Errorf("baton-appstoreconnect: one of --private-key or --private-key-path is required")
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("baton-appstoreconnect: failed to read private key from %s: %w", path, err)
	}

	return strings.TrimSpace(string(contents)), nil
}
