package config

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

type windowSmokeServer struct {
	types.ConnectorServer
}

func (*windowSmokeServer) Close(context.Context) error { return nil }
func (*windowSmokeServer) GetMetadata(context.Context, *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return v2.ConnectorServiceGetMetadataResponse_builder{Metadata: v2.ConnectorMetadata_builder{DisplayName: "window-smoke"}.Build()}.Build(), nil
}
func (*windowSmokeServer) Validate(context.Context, *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	return &v2.ConnectorServiceValidateResponse{}, nil
}
func (*windowSmokeServer) Cleanup(context.Context, *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
	return &v2.ConnectorServiceCleanupResponse{}, nil
}
func (*windowSmokeServer) ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	rt := v2.ResourceType_builder{Id: "group", DisplayName: "Group", Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{})}.Build()
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: []*v2.ResourceType{rt}}.Build(), nil
}
func (s *windowSmokeServer) ListResources(_ context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	page, _ := strconv.Atoi(req.GetPageToken())
	r := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: strconv.Itoa(page)}.Build(), DisplayName: "Group"}.Build()
	next := ""
	if page < 4 {
		next = strconv.Itoa(page + 1)
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: []*v2.Resource{r}, NextPageToken: next}.Build(), nil
}
func (*windowSmokeServer) ListStaticEntitlements(context.Context, *v2.EntitlementsServiceListStaticEntitlementsRequest) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return &v2.EntitlementsServiceListStaticEntitlementsResponse{}, nil
}

func TestMain(m *testing.M) {
	if os.Getenv("BATON_SDK_WINDOW_SMOKE_CHILD") == "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		getConnector := func(context.Context, *viper.Viper, cli.RunTimeOpts) (types.ConnectorServer, error) {
			return &windowSmokeServer{}, nil
		}
		_, cmd, err := DefineConfigurationV2(ctx, "window-smoke", getConnector, field.NewConfiguration(nil))
		if err == nil {
			args := []string{"--file", os.Getenv("BATON_SDK_WINDOW_SMOKE_FILE"), "--list-page-window", "2", "--log-level", "error"}
			if os.Args[len(os.Args)-1] == "_connector-service" {
				args = []string{"_connector-service"}
			}
			cmd.SetArgs(args)
			err = cmd.ExecuteContext(ctx)
		}
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

func TestListPageWindowCLIToFinishedSyncStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	path := filepath.Join(dir, "window.c1z")
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, executable) // #nosec G702 -- this test re-executes its own fixture binary.
	cmd.Env = append(os.Environ(), "BATON_SDK_WINDOW_SMOKE_CHILD=1", "BATON_SDK_WINDOW_SMOKE_FILE="+path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(context.Background())) }()
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.NotNil(t, run.Stats)
	require.EqualValues(t, 2, run.Stats.GetResources())
	require.EqualValues(t, 1, run.Stats.GetCounters()["list_page_window.sampled"])
	require.EqualValues(t, 1, run.Stats.GetCounters()["list_page_window.sampled:group"])
}

func TestListPageWindowCLIRejectsOutOfRangeWindows(t *testing.T) {
	for _, value := range []string{"-1", "4294967296"} {
		t.Run(value, func(t *testing.T) {
			getConnector := func(context.Context, *viper.Viper, cli.RunTimeOpts) (types.ConnectorServer, error) {
				t.Error("invalid window reached connector construction")
				return &windowSmokeServer{}, nil
			}
			_, cmd, err := DefineConfigurationV2(t.Context(), "window-smoke", getConnector, field.NewConfiguration(nil))
			require.NoError(t, err)
			cmd.SetArgs([]string{"--file", filepath.Join(t.TempDir(), "invalid.c1z"), "--list-page-window", value, "--log-level", "error"})
			require.ErrorContains(t, cmd.ExecuteContext(t.Context()), "list-page-window")
		})
	}
}
