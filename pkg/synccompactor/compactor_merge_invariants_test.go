package synccompactor

// Wiring pin for WithCompactionMergedStore (review round 2): the
// compactor's expand pass runs the syncer's post-collection ingestion
// invariants, and a keep-newer merge of two INDIVIDUALLY VALID syncs
// can manufacture an exclusion-group conflict no input contained (the
// default moved from E1 to E2 between syncs; the union keeps both
// is_default rows). The merge signal is what downgrades that conflict
// from a seal-blocking failure to an aggregated warning — deleting
// sync.WithCompactionMergedStore() from expandGrants makes THIS test
// fail with "ingest invariant I5 violated". Policy-level behavior is
// pinned in pkg/sync; this pins the compactor's wiring end to end.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func TestCompactionExpandToleratesMergeManufacturedExclusionConflicts(t *testing.T) {
	ctx := t.Context()

	inputDir, err := os.MkdirTemp("", "compactor-merge-inv-in")
	require.NoError(t, err)
	defer os.RemoveAll(inputDir)
	outputDir, err := os.MkdirTemp("", "compactor-merge-inv-out")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)
	tmpDir, err := os.MkdirTemp("", "compactor-merge-inv-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	appRT := v2.ResourceType_builder{Id: "app", DisplayName: "App"}.Build()
	appResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()

	defaultEnt := func(t *testing.T, entID string) *v2.Entitlement {
		t.Helper()
		eg, err := anypb.New(v2.EntitlementExclusionGroup_builder{
			ExclusionGroupId: "role",
			IsDefault:        true,
		}.Build())
		require.NoError(t, err)
		return v2.Entitlement_builder{
			Id:          entID,
			Resource:    appResource,
			Annotations: []*anypb.Any{eg},
		}.Build()
	}

	// Each input is INTERNALLY VALID: exactly one is_default in group
	// "role". The default moved between syncs (admin → viewer), so the
	// keep-newer union carries BOTH default rows — the conflict only
	// the merge could produce.
	buildInput := func(t *testing.T, name, entID string) *CompactableSync {
		t.Helper()
		path := filepath.Join(inputDir, name)
		f, err := dotc1z.NewC1ZFile(ctx, path, dotc1z.WithTmpDir(tmpDir))
		require.NoError(t, err)
		syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		require.NoError(t, f.PutResourceTypes(ctx, appRT))
		require.NoError(t, f.PutResources(ctx, appResource))
		require.NoError(t, f.PutEntitlements(ctx, defaultEnt(t, entID)))
		require.NoError(t, f.EndSync(ctx))
		require.NoError(t, f.Close(ctx))
		return &CompactableSync{FilePath: path, SyncID: syncID}
	}

	older := buildInput(t, "older.c1z", "app:github:admin")
	newer := buildInput(t, "newer.c1z", "app:github:viewer")

	compactor, cleanup, err := NewCompactor(ctx, outputDir,
		[]*CompactableSync{older, newer},
		WithTmpDir(tmpDir),
		WithCompactorType(CompactorTypeAttached),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()

	// The whole point: compaction (including its expand pass, which
	// runs the invariant seam over the merged store) must SUCCEED —
	// the two-defaults conflict is merge-manufactured and gets one
	// aggregated warning, never a seal failure.
	out, err := compactor.Compact(ctx)
	require.NoError(t, err,
		"the compaction expand pass must tolerate merge-manufactured exclusion-group conflicts (WithCompactionMergedStore wiring)")
	require.NotNil(t, out)

	// The merged artifact holds both generations' rows, conflict intact.
	merged, err := dotc1z.NewC1ZFile(ctx, out.FilePath, dotc1z.WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, merged.Close(ctx)) }()
	resp, err := merged.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.GetList()))
	for _, e := range resp.GetList() {
		ids = append(ids, e.GetId())
	}
	require.ElementsMatch(t, []string{"app:github:admin", "app:github:viewer"}, ids,
		"keep-newer union keeps both entitlement generations (distinct ids never collide)")
}
