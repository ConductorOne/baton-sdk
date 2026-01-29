package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/require"
)

func newTempC1Z(t *testing.T) (*C1File, func()) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "edges_test.c1z")
	f, err := NewC1ZFile(context.Background(), path, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	cleanup := func() {
		_ = f.Close(context.Background())
		_ = os.Remove(path)
	}
	return f, cleanup
}

func countEdges(ctx context.Context, f *C1File, syncID string) (int, error) {
	q := f.db.From(entitlementEdges.Name()).Prepared(true)
	q = q.Select(goqu.COUNT("*"))
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	query, args, err := q.ToSQL()
	if err != nil {
		return 0, err
	}
	var n int
	err = f.db.QueryRowContext(ctx, query, args...).Scan(&n)
	return n, err
}

func TestEntitlementEdges_BuildAndApplyDiff(t *testing.T) {
	ctx := context.Background()

	f, cleanup := newTempC1Z(t)
	defer cleanup()

	// Base sync with one relationship-defining grant.
	baseSyncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Minimal objects (dotc1z doesn't enforce foreign keys here).
	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "rt"}.Build())
	require.NoError(t, err)

	resA := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "rt", Resource: "a"}.Build()}.Build()
	resB := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "rt", Resource: "b"}.Build()}.Build()
	require.NoError(t, f.PutResources(ctx, resA, resB))

	entA := v2.Entitlement_builder{Id: "ent_a", Resource: resA, Slug: "a"}.Build()
	entB := v2.Entitlement_builder{Id: "ent_b", Resource: resB, Slug: "b"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, entA, entB))

	edgeAnno := v2.GrantExpansionEdges_builder{
		EntitlementIds:  []string{"ent_a"},
		Shallow:         false,
		ResourceTypeIds: []string{"rt"},
	}.Build()
	g1 := v2.Grant_builder{
		Id:          "g1",
		Entitlement: entB,
		Principal:   resA,
		Annotations: annotations.New(edgeAnno),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, g1))
	require.NoError(t, f.EndSync(ctx))

	// Build edges for base sync.
	require.NoError(t, f.BuildEntitlementEdgesForSync(ctx, baseSyncID))
	n, err := countEdges(ctx, f, baseSyncID)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Create a diff: delete g1.
	delSyncID := "diff_del_1"
	now := time.Now().Format("2006-01-02 15:04:05.999999999")
	q := f.db.Insert(syncRuns.Name()).Rows(goqu.Record{
		"sync_id":        delSyncID,
		"started_at":     now,
		"sync_token":     "",
		"sync_type":      connectorstore.SyncTypePartialDeletions,
		"parent_sync_id": baseSyncID,
		"linked_sync_id": "",
	}).Prepared(true)
	query, args, err := q.ToSQL()
	require.NoError(t, err)
	_, err = f.db.ExecContext(ctx, query, args...)
	require.NoError(t, err)
	require.NoError(t, f.SetSyncID(ctx, delSyncID))
	require.NoError(t, f.PutGrants(ctx, g1)) // deletions sync contains the OLD row
	require.NoError(t, f.EndSync(ctx))

	upSyncID := "diff_up_1"
	q = f.db.Insert(syncRuns.Name()).Rows(goqu.Record{
		"sync_id":        upSyncID,
		"started_at":     now,
		"sync_token":     "",
		"sync_type":      connectorstore.SyncTypePartialUpserts,
		"parent_sync_id": baseSyncID,
		"linked_sync_id": "",
	}).Prepared(true)
	query, args, err = q.ToSQL()
	require.NoError(t, err)
	_, err = f.db.ExecContext(ctx, query, args...)
	require.NoError(t, err)
	require.NoError(t, f.SetSyncID(ctx, upSyncID))
	require.NoError(t, f.EndSync(ctx))

	require.NoError(t, f.ApplyEntitlementEdgesDiff(ctx, baseSyncID, upSyncID, delSyncID))

	n, err = countEdges(ctx, f, baseSyncID)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}
