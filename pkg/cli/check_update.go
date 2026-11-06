package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
)

const defaultRegistryURL = "https://dist.conductorone.com"

// NewCheckUpdateCommand checks the installed connector against the registry's stable channel.
func NewCheckUpdateCommand(ctx context.Context, connectorName, version string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check-update",
		Short: "Check whether a newer stable connector release is available",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			org, err := cmd.Flags().GetString("org")
			if err != nil {
				return err
			}
			registryURL, err := cmd.Flags().GetString("registry-url")
			if err != nil {
				return err
			}
			return checkUpdate(ctx, cmd, registryURL, org, connectorName, version)
		},
	}
	cmd.Flags().String("org", "ConductorOne", "Registry organization that publishes this connector")
	cmd.Flags().String("registry-url", defaultRegistryURL, "Connector registry URL")
	return cmd
}

func checkUpdate(ctx context.Context, cmd *cobra.Command, registryURL, org, connectorName, version string) error {
	installed, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("cannot determine update status: installed version %q is not semantic versioning", version)
	}
	if org == "" || connectorName == "" {
		return fmt.Errorf("cannot determine update status: registry organization and connector name are required")
	}

	base, err := url.Parse(registryURL)
	if err != nil || base.Scheme == "" || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") {
		return fmt.Errorf("invalid registry URL %q", registryURL)
	}
	base.Path = strings.TrimSuffix(base.Path, "/") + "/api/v1/connectors/" + url.PathEscape(org) + "/" + url.PathEscape(connectorName) + "/summary"
	base.RawQuery = ""
	base.Fragment = ""

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return fmt.Errorf("create registry request: %w", err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("cannot determine update status: query registry: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("cannot determine update status: registry returned HTTP %d", response.StatusCode)
	}

	var summary struct {
		Connector *struct {
			Stable string `json:"stable"`
		} `json:"connector"`
	}
	if err := json.NewDecoder(response.Body).Decode(&summary); err != nil {
		return fmt.Errorf("cannot determine update status: decode registry response: %w", err)
	}
	if summary.Connector == nil || summary.Connector.Stable == "" {
		return fmt.Errorf("cannot determine update status: connector has no stable release")
	}
	stable, err := semver.NewVersion(summary.Connector.Stable)
	if err != nil {
		return fmt.Errorf("cannot determine update status: stable version %q is not semantic versioning", summary.Connector.Stable)
	}

	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Installed: %s\nStable: %s\n", version, summary.Connector.Stable)
	if err != nil {
		return err
	}
	status := "up to date"
	if installed.LessThan(stable) {
		status = "update available"
	} else if stable.LessThan(installed) {
		status = "newer than stable"
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Status: %s\n", status)
	return err
}
