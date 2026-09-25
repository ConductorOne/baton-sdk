package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
)

func TestLedgerLegacySDKArtifact(t *testing.T) {
	source := os.Getenv("BATON_LEGACY_SDK_ARTIFACT")
	if source == "" {
		t.Skip("requires the interrupted four-page artifact produced by the baseline SDK")
	}
	root, err := os.OpenRoot(filepath.Dir(source))
	require.NoError(t, err)
	data, err := root.ReadFile(filepath.Base(source))
	require.NoError(t, err)
	require.NoError(t, root.Close())
	path := filepath.Join(t.TempDir(), "legacy.c1z")
	require.NoError(t, writeLedgerTestFile(path, data, 0600))
	f := openLedgerFixtureAt(t, path, false)
	ctx := t.Context()
	oldToken, err := f.store.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, oldToken)
	before, err := f.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	require.Len(t, before.GetList(), 10)
	connector := &ledgerCostConnector{mockConnector: newMockConnector(), pages: 4, records: 10, streams: 1}
	connector.rtDB = []*v2.ResourceType{v2.ResourceType_builder{
		Id: "cost-0", DisplayName: "Cost resources", Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()}
	created, err := NewSyncer(ctx, connector, WithConnectorStore(f.store), WithWorkerCount(1), WithDontExpandGrants())
	require.NoError(t, err)
	s := created.(*syncer)
	s.testHooks.checkpointHook = func(string) { t.Error("ledger migration wrote a checkpoint token") }
	observed := false
	var snapshot []ledgerKV
	s.testHooks.ledgerWalk = func(entering bool) {
		if !entering {
			f.audit.enter(ledgerLifecycle)
			require.Equal(t, snapshot, ledgerRawSnapshot(t, f.engine))
			return
		}
		observed = true
		snapshot = ledgerRawSnapshot(t, f.engine)
		f.audit.enter(ledgerWalk)
		state, err := f.store.CurrentSyncStep(ctx)
		require.NoError(t, err)
		require.Empty(t, state)
		frontier, found, err := f.ledger.LedgerFrontier(ctx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, oldToken, frontier.State)
		_, initialized, err := f.ledger.PendingWork(ctx, 0, 100)
		require.NoError(t, err)
		require.True(t, initialized)
	}
	require.NoError(t, s.Sync(ctx))
	require.True(t, observed)
	require.EqualValues(t, 3, connector.calls.Load())
	require.NoError(t, f.store.Close(ctx))
	f = openLedgerFixtureAt(t, path, false)
	resources, err := f.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	require.Len(t, resources.GetList(), 40)
	ids := make(map[string]bool)
	for _, resource := range resources.GetList() {
		require.Equal(t, "cost-0", resource.GetId().GetResourceType())
		require.Equal(t, "fixed-resource-payload", resource.GetDisplayName())
		ids[resource.GetId().GetResource()] = true
	}
	for i := range 40 {
		require.True(t, ids[fmt.Sprintf("%012d", i)])
	}
}
