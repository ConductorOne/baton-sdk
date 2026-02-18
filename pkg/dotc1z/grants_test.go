package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestExtractAndStripExpansion_WhitespaceOnlyEntitlementIDs(t *testing.T) {
	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"  ", "\t", "   \n  "},
	}.Build()

	expandableAny, err := anypb.New(expandable)
	require.NoError(t, err)

	grant := v2.Grant_builder{
		Id: "test-grant",
		Entitlement: v2.Entitlement_builder{
			Id: "test-entitlement",
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user1",
			}.Build(),
		}.Build(),
		Annotations: []*anypb.Any{expandableAny},
	}.Build()

	expansionBytes, isExpandable := extractAndStripExpansion(grant)
	require.False(t, isExpandable, "grant with only whitespace entitlement IDs should not be expandable")
	require.Nil(t, expansionBytes, "expansion bytes should be nil for non-expandable grant")

	// The annotation should still be stripped from the grant even though the IDs were invalid.
	require.Empty(t, grant.GetAnnotations(), "GrantExpandable annotation should be stripped even with invalid IDs")
}

func TestExtractAndStripExpansion_MixedWhitespaceAndValidIDs(t *testing.T) {
	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"  ", "valid-entitlement-id", "\t"},
		Shallow:        true,
	}.Build()

	expandableAny, err := anypb.New(expandable)
	require.NoError(t, err)

	grant := v2.Grant_builder{
		Id: "test-grant",
		Entitlement: v2.Entitlement_builder{
			Id: "test-entitlement",
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user1",
			}.Build(),
		}.Build(),
		Annotations: []*anypb.Any{expandableAny},
	}.Build()

	expansionBytes, isExpandable := extractAndStripExpansion(grant)
	require.True(t, isExpandable, "grant with valid entitlement ID should be expandable")
	require.NotNil(t, expansionBytes, "expansion bytes should not be nil for expandable grant")

	// Verify that the annotation was stripped from the grant.
	require.Len(t, grant.GetAnnotations(), 0, "GrantExpandable annotation should be stripped from grant")
}

// TestGrantExpandableRoundTrip verifies that storing a grant with GrantExpandable strips the
// annotation from the data blob, stores it in the expansion column, and that
// ListExpandableGrants returns the expansion data from SQL columns.
// ListGrants does NOT re-attach the annotation — expansion data is only
// accessible via ListExpandableGrants.
func TestGrantExpandableRoundTrip(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1}.Build()

	// Grant WITHOUT expandable annotation.
	normalGrant := v2.Grant_builder{
		Id:          "grant-normal",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()

	// Grant WITH expandable annotation.
	expandableGrant := v2.Grant_builder{
		Id:          "grant-expandable",
		Entitlement: ent2,
		Principal:   g1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{"ent1"},
			Shallow:         true,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	require.NoError(t, c1f.PutGrants(ctx, normalGrant, expandableGrant))

	// Verify PutGrants did NOT mutate the caller's grant (annotation still present).
	callerAnnotations := annotations.Annotations(expandableGrant.GetAnnotations())
	require.True(t, callerAnnotations.Contains(&v2.GrantExpandable{}),
		"PutGrants should not mutate caller's grant object")

	// ListGrants should return grants WITHOUT the GrantExpandable annotation.
	// Expansion data is only accessible via ListExpandableGrants.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2)

	for _, g := range resp.GetList() {
		readAnnos := annotations.Annotations(g.GetAnnotations())
		require.False(t, readAnnos.Contains(&v2.GrantExpandable{}),
			"ListGrants should not return GrantExpandable annotation (grant %s)", g.GetId())
	}

	// Internal expansion list should return the expandable grant with correct expansion data.
	defs, _ := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{})
	require.Len(t, defs, 1)
	require.Equal(t, "grant-expandable", defs[0].GrantExternalID)
	require.Equal(t, "ent2", defs[0].TargetEntitlementID)
	require.Equal(t, []string{"ent1"}, defs[0].SourceEntitlementIDs)
	require.True(t, defs[0].Shallow)
	require.Equal(t, []string{"user"}, defs[0].ResourceTypeIDs)
}

// TestGrantExpandableSurvivesCloseReopen verifies that expansion data persists
// through a close/reopen cycle of the c1z file.
func TestGrantExpandableSurvivesCloseReopen(t *testing.T) {
	ctx := context.Background()

	tmpFile, err := os.CreateTemp("", "test-expansion-reopen-*.c1z")
	require.NoError(t, err)
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Phase 1: Create store, write expandable grant, close.
	func() {
		c1f, err := NewC1ZFile(ctx, tmpPath)
		require.NoError(t, err)

		_, err = c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		groupRT := v2.ResourceType_builder{Id: "group"}.Build()
		userRT := v2.ResourceType_builder{Id: "user"}.Build()
		require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

		g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
		u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
		require.NoError(t, c1f.PutResources(ctx, g1, u1))

		ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
		require.NoError(t, c1f.PutEntitlements(ctx, ent1))

		expandableGrant := v2.Grant_builder{
			Id:          "grant-expandable",
			Entitlement: ent1,
			Principal:   u1,
			Annotations: annotations.New(v2.GrantExpandable_builder{
				EntitlementIds:  []string{"ent1"},
				Shallow:         true,
				ResourceTypeIds: []string{"user"},
			}.Build()),
		}.Build()
		require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

		// Verify expansion is set before close.
		raw := getRawGrantRow(ctx, t, c1f, "grant-expandable")
		require.NotNil(t, raw.expansion, "expansion should be set before close")

		require.NoError(t, c1f.EndSync(ctx))
		require.NoError(t, c1f.Close(ctx))
	}()

	// Phase 2: Reopen and verify expansion survived.
	c1f2, err := NewC1ZFile(ctx, tmpPath)
	require.NoError(t, err)
	defer c1f2.Close(ctx)

	defs, _ := listExpansionDefs(ctx, t, c1f2, connectorstore.GrantListOptions{})
	require.Len(t, defs, 1, "expansion should survive close/reopen")
	require.Equal(t, "grant-expandable", defs[0].GrantExternalID)
	require.Equal(t, []string{"ent1"}, defs[0].SourceEntitlementIDs)
	require.True(t, defs[0].Shallow)
	require.Equal(t, []string{"user"}, defs[0].ResourceTypeIDs)
}

// TestGrantExpandableStrippedFromDataBlob verifies that the GrantExpandable annotation
// is stripped from the data blob on write and only stored in the expansion column.
func TestGrantExpandableStrippedFromDataBlob(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	expandableGrant := v2.Grant_builder{
		Id:          "grant-blob-check",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	// Read the raw data blob and verify GrantExpandable is NOT in it.
	var rawData []byte
	err := c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id=?", "grant-blob-check",
	).Scan(&rawData)
	require.NoError(t, err)

	rawGrant := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(rawData, rawGrant))
	rawAnnos := annotations.Annotations(rawGrant.GetAnnotations())
	require.False(t, rawAnnos.Contains(&v2.GrantExpandable{}),
		"data blob should NOT contain GrantExpandable -- it should only be in the expansion column")

	// Verify the expansion column has the data.
	raw := getRawGrantRow(ctx, t, c1f, "grant-blob-check")
	require.NotNil(t, raw.expansion, "expansion column should be populated")

	ge := &v2.GrantExpandable{}
	require.NoError(t, proto.Unmarshal(raw.expansion, ge))
	require.Equal(t, []string{"ent2"}, ge.GetEntitlementIds())
	require.True(t, ge.GetShallow())
}

// TestDiffDetectsExpansionAnnotationChange verifies that when only the expansion annotation
// changes (not the rest of the grant data), the diff correctly detects the grant as modified.
func TestDiffDetectsExpansionAnnotationChange(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_expansion_change_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_expansion_change_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1}.Build()

	// OLD: grant with expansion annotation pointing to ent1.
	// Use normal locking mode so the file can be attached later.
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, ent1, ent2))

	oldGrant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        false,
		}.Build()),
	}.Build()
	require.NoError(t, oldFile.PutGrants(ctx, oldGrant))
	require.NoError(t, oldFile.EndSync(ctx))

	// NEW: same grant but expansion annotation now has Shallow=true (different expansion).
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, ent1, ent2))

	newGrant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true, // Changed from false to true.
		}.Build()),
	}.Build()
	require.NoError(t, newFile.PutGrants(ctx, newGrant))
	require.NoError(t, newFile.EndSync(ctx))

	// Generate diff.
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// The grant should appear in upserts because the expansion column changed.
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	grantsResp, err := newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1, "diff should detect expansion annotation change as a modification")
	require.Equal(t, "grant-1", grantsResp.GetList()[0].GetId())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestDiffDetectsDataOnlyChange verifies that when the grant data changes but the expansion
// annotation stays the same, the diff correctly detects the grant as modified.
func TestDiffDetectsDataOnlyChange(t *testing.T) {
	ctx := context.Background()

	oldPath := filepath.Join(c1zTests.workingDir, "diff_data_change_old.c1z")
	newPath := filepath.Join(c1zTests.workingDir, "diff_data_change_new.c1z")
	defer os.Remove(oldPath)
	defer os.Remove(newPath)

	opts := []C1ZOption{WithPragma("journal_mode", "WAL")}

	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "G1"}.Build()
	g2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(), DisplayName: "G2"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "U1"}.Build()

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g2}.Build()

	sharedExpandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"ent1"},
		Shallow:        true,
	}.Build()

	// OLD: grant with sources={}.
	// Use normal locking mode so the file can be attached later.
	oldOpts := append(slices.Clone(opts), WithPragma("locking_mode", "normal"))
	oldFile, err := NewC1ZFile(ctx, oldPath, oldOpts...)
	require.NoError(t, err)

	oldSyncID, err := oldFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, oldFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, oldFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, oldFile.PutEntitlements(ctx, ent1, ent2))

	oldGrant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent2,
		Principal:   u1,
		Annotations: annotations.New(sharedExpandable),
	}.Build()
	require.NoError(t, oldFile.PutGrants(ctx, oldGrant))
	require.NoError(t, oldFile.EndSync(ctx))

	// NEW: same expansion, but grant now has sources (data blob changes).
	newFile, err := NewC1ZFile(ctx, newPath, opts...)
	require.NoError(t, err)

	newSyncID, err := newFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, newFile.PutResourceTypes(ctx, groupRT, userRT))
	require.NoError(t, newFile.PutResources(ctx, g1, g2, u1))
	require.NoError(t, newFile.PutEntitlements(ctx, ent1, ent2))

	newGrant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent2,
		Principal:   u1,
		Annotations: annotations.New(sharedExpandable),
		Sources: v2.GrantSources_builder{
			Sources: map[string]*v2.GrantSources_GrantSource{
				"ent1": {},
			},
		}.Build(),
	}.Build()
	require.NoError(t, newFile.PutGrants(ctx, newGrant))
	require.NoError(t, newFile.EndSync(ctx))

	// Generate diff.
	attached, err := newFile.AttachFile(oldFile, "attached")
	require.NoError(t, err)

	upsertsSyncID, _, err := attached.GenerateSyncDiffFromFile(ctx, oldSyncID, newSyncID)
	require.NoError(t, err)

	_, err = attached.DetachFile("attached")
	require.NoError(t, err)

	// The grant should appear in upserts because the data blob changed.
	err = newFile.ViewSync(ctx, upsertsSyncID)
	require.NoError(t, err)

	grantsResp, err := newFile.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1, "diff should detect data-only change as a modification")
	require.Equal(t, "grant-1", grantsResp.GetList()[0].GetId())

	_ = oldFile.Close(ctx)
	_ = newFile.Close(ctx)
}

// TestBackfillMigration_OldSyncGetsExpansionColumn verifies that opening a c1z file where
// grants were stored with GrantExpandable in the data blob (old format) correctly backfills
// the expansion column and strips the annotation from data.
//
// It creates a small file, manually reverts a grant row to old format (annotation in data,
// expansion=NULL, supports_diff=0), then re-opens the file and verifies the migration ran.
func TestBackfillMigration_OldSyncGetsExpansionColumn(t *testing.T) {
	ctx := context.Background()

	tmpFile, err := os.CreateTemp("", "test-backfill-*.c1z")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Step 1: Create a c1z and write grants the "new" way (expansion column populated).
	c1f, err := NewC1ZFile(ctx, tmpFile.Name())
	require.NoError(t, err)

	defer c1f.Close(ctx)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1, u1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1))

	expandableGrant := v2.Grant_builder{
		Id:          "grant-expandable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{"ent1"},
			Shallow:         true,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()
	normalGrant := v2.Grant_builder{
		Id:          "grant-normal",
		Entitlement: ent1,
		Principal:   g1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant, normalGrant))
	require.NoError(t, c1f.EndSync(ctx))

	// Step 2: Simulate "old format" by manually moving GrantExpandable back into data blob
	// and clearing the expansion column + supports_diff. This mimics what an old c1z would look like.

	// Re-serialize expandableGrant WITH the annotation in data blob.
	origExpandable := v2.GrantExpandable_builder{
		EntitlementIds:  []string{"ent1"},
		Shallow:         true,
		ResourceTypeIds: []string{"user"},
	}.Build()
	grantWithAnno := v2.Grant_builder{
		Id:          "grant-expandable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(origExpandable),
	}.Build()
	oldFormatData, err := proto.MarshalOptions{Deterministic: true}.Marshal(grantWithAnno)
	require.NoError(t, err)

	// Overwrite the row to simulate old format: annotation in data, expansion=NULL.
	_, err = c1f.db.ExecContext(ctx, "UPDATE "+grants.Name()+" SET data=?, expansion=NULL WHERE external_id='grant-expandable' AND sync_id=?", oldFormatData, syncID)
	require.NoError(t, err)

	// Mark the sync as NOT supporting diff (old sync).
	_, err = c1f.db.ExecContext(ctx, "UPDATE "+syncRuns.Name()+" SET supports_diff=0 WHERE sync_id=?", syncID)
	require.NoError(t, err)

	// Step 3: Run the backfill explicitly (as the migration would on re-open).
	err = backfillGrantExpansionColumn(ctx, c1f.db, grants.Name())
	require.NoError(t, err)

	// Clear current sync so we can use ViewSync.
	c1f.currentSyncID = ""
	require.NoError(t, c1f.ViewSync(ctx, syncID))

	// Verify backfill populated the expansion column via ListExpandableGrants.
	defs, _ := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{SyncID: syncID})
	require.Len(t, defs, 1, "backfill should produce exactly one expandable grant")
	require.Equal(t, "grant-expandable", defs[0].GrantExternalID)
	require.Equal(t, []string{"ent1"}, defs[0].SourceEntitlementIDs)
	require.True(t, defs[0].Shallow)
	require.Equal(t, []string{"user"}, defs[0].ResourceTypeIDs)

	// Verify normal grant is NOT in the expandable list.
	// (ListExpandableGrants filters by expansion IS NOT NULL.)

	// Verify that ListGrants returns both grants WITHOUT the GrantExpandable annotation.
	// Expansion data is only accessible via ListExpandableGrants.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2)

	for _, g := range resp.GetList() {
		readAnnos := annotations.Annotations(g.GetAnnotations())
		require.False(t, readAnnos.Contains(&v2.GrantExpandable{}),
			"ListGrants should not return GrantExpandable annotation after backfill (grant %s)", g.GetId())
	}
}

// grantRawRow holds the raw SQL column values for a grant row.
type grantRawRow struct {
	expansion      []byte
	needsExpansion int
}

// getRawGrantRow reads the raw expansion and needs_expansion columns for a grant by external_id.
func getRawGrantRow(ctx context.Context, t *testing.T, c1f *C1File, externalID string) grantRawRow {
	t.Helper()
	var r grantRawRow
	err := c1f.db.QueryRowContext(ctx,
		"SELECT expansion, needs_expansion FROM "+grants.Name()+" WHERE external_id=?", externalID,
	).Scan(&r.expansion, &r.needsExpansion)
	require.NoError(t, err)
	// Normalize empty blob to nil so callers can use require.Nil uniformly
	// across drivers that may return []byte{} vs nil for SQL NULL.
	if len(r.expansion) == 0 {
		r.expansion = nil
	}
	return r
}

func getRawGrantRowForSync(ctx context.Context, t *testing.T, c1f *C1File, externalID string, syncID string) grantRawRow {
	t.Helper()
	var r grantRawRow
	err := c1f.db.QueryRowContext(ctx,
		"SELECT expansion, needs_expansion FROM "+grants.Name()+" WHERE external_id=? AND sync_id=?",
		externalID,
		syncID,
	).Scan(&r.expansion, &r.needsExpansion)
	require.NoError(t, err)
	// Normalize empty blob to nil so callers can use require.Nil uniformly
	// across drivers that may return []byte{} vs nil for SQL NULL.
	if len(r.expansion) == 0 {
		r.expansion = nil
	}
	return r
}

func listExpansionDefs(
	ctx context.Context,
	t *testing.T,
	c1f *C1File,
	opts connectorstore.GrantListOptions,
) ([]*connectorstore.ExpandableGrantDef, string) {
	t.Helper()
	if opts.Mode != connectorstore.GrantListModeExpansionNeedsOnly {
		opts.Mode = connectorstore.GrantListModeExpansion
	}

	resp, err := c1f.ListGrantsInternal(ctx, opts)
	require.NoError(t, err)

	defs := make([]*connectorstore.ExpandableGrantDef, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		if row.Expansion != nil {
			defs = append(defs, row.Expansion)
		}
	}
	return defs, resp.NextPageToken
}

func requireExpansionSQLNullForSync(ctx context.Context, t *testing.T, c1f *C1File, externalID string, syncID string) {
	t.Helper()
	var isNull int
	err := c1f.db.QueryRowContext(
		ctx,
		"SELECT expansion IS NULL FROM "+grants.Name()+" WHERE external_id=? AND sync_id=?",
		externalID,
		syncID,
	).Scan(&isNull)
	require.NoError(t, err)
	require.Equal(t, 1, isNull, "expansion must be SQL NULL, not empty blob")
}

func requireExpansionSQLNull(ctx context.Context, t *testing.T, c1f *C1File, externalID string) {
	t.Helper()
	var isNull int
	err := c1f.db.QueryRowContext(
		ctx,
		"SELECT expansion IS NULL FROM "+grants.Name()+" WHERE external_id=?",
		externalID,
	).Scan(&isNull)
	require.NoError(t, err)
	require.Equal(t, 1, isNull, "expansion must be SQL NULL, not empty blob")
}

// setupTestC1Z creates a fresh c1z with a started full sync and common resource types,
// resources, and entitlements. Returns the c1z, sync ID, and a cleanup function.
func setupTestC1Z(ctx context.Context, t *testing.T) (*C1File, string, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "test-grants-*.c1z")
	require.NoError(t, err)
	tmpFile.Close()

	c1f, err := NewC1ZFile(ctx, tmpFile.Name())
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1, u1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1, ent2))

	cleanup := func() {
		_ = c1f.Close(ctx)
		// #nosec G703 -- tmpFile.Name() is created by os.CreateTemp in this test.
		os.Remove(tmpFile.Name())
	}
	return c1f, syncID, cleanup
}

// TestNeedsExpansion_ExpandableToNonExpandable verifies that when a grant transitions
// from expandable to non-expandable by being re-synced with a GrantExpandable annotation
// that has no valid entitlement IDs, expansion is cleared to SQL NULL.
func TestNeedsExpansion_ExpandableToNonExpandable(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant WITH expansion.
	expandableGrant := v2.Grant_builder{
		Id:          "grant-transition",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	raw := getRawGrantRowForSync(ctx, t, c1f, "grant-transition", syncID)
	require.NotNil(t, raw.expansion, "initial insert should have non-nil expansion")
	require.Equal(t, 1, raw.needsExpansion, "initial insert should have needs_expansion=1")

	// Step 2: Upsert same grant with an empty GrantExpandable (no valid entitlement IDs).
	// This simulates a connector that previously provided expansion but now provides
	// an empty/invalid annotation — expansion should be cleared.
	clearedGrant := v2.Grant_builder{
		Id:          "grant-transition",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, clearedGrant))

	raw = getRawGrantRowForSync(ctx, t, c1f, "grant-transition", syncID)
	require.Nil(t, raw.expansion, "after clearing expansion, column should be NULL")
	requireExpansionSQLNullForSync(ctx, t, c1f, "grant-transition", syncID)
	require.Equal(t, 0, raw.needsExpansion, "needs_expansion must be 0 when expansion is cleared")

	// Step 3: Verify ListExpandableGrants returns nothing.
	defs, _ := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{SyncID: syncID})
	require.Len(t, defs, 0, "no expandable grants should remain")
}

// TestExpansionClearedOnUpsertWithoutAnnotation verifies that upserting a grant
// WITHOUT the GrantExpandable annotation clears the existing expansion column.
// If a connector previously returned GrantExpandable but now omits it, the grant
// is no longer expandable.
func TestExpansionClearedOnUpsertWithoutAnnotation(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant WITH expansion.
	expandableGrant := v2.Grant_builder{
		Id:          "grant-loses-expansion",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	raw := getRawGrantRow(ctx, t, c1f, "grant-loses-expansion")
	require.NotNil(t, raw.expansion, "initial insert should have non-nil expansion")
	require.Equal(t, 1, raw.needsExpansion, "initial insert should have needs_expansion=1")

	// Step 2: Upsert same grant WITHOUT the annotation (no GrantExpandable at all).
	// This simulates a connector that previously provided expansion but now omits it.
	// Expansion should be CLEARED.
	noAnnotationGrant := v2.Grant_builder{
		Id:          "grant-loses-expansion",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, noAnnotationGrant))

	raw = getRawGrantRow(ctx, t, c1f, "grant-loses-expansion")
	require.Nil(t, raw.expansion,
		"expansion column should be cleared when grant is upserted without GrantExpandable annotation")
	requireExpansionSQLNull(ctx, t, c1f, "grant-loses-expansion")
	require.Equal(t, 0, raw.needsExpansion,
		"needs_expansion should be 0 when expansion is cleared")

	// Step 3: Verify ListExpandableGrants returns nothing for this grant.
	defs, _ := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{})
	for _, d := range defs {
		require.NotEqual(t, "grant-loses-expansion", d.GrantExternalID,
			"grant should not appear in expandable grants after losing annotation")
	}
}

// TestNeedsExpansion_IdenticalExpansionKeepsExistingFlag verifies that upserting
// a grant with the same expansion annotation does not flip needs_expansion back to 1
// if it was already 0 (i.e. the ELSE branch of the CASE expression keeps the existing value).
func TestNeedsExpansion_IdenticalExpansionKeepsExistingFlag(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	expandable := v2.GrantExpandable_builder{
		EntitlementIds: []string{"ent2"},
		Shallow:        true,
	}.Build()

	grant := v2.Grant_builder{
		Id:          "grant-stable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(expandable),
	}.Build()

	// Step 1: Insert the grant. needs_expansion starts at 1.
	require.NoError(t, c1f.PutGrants(ctx, grant))
	raw := getRawGrantRow(ctx, t, c1f, "grant-stable")
	require.Equal(t, 1, raw.needsExpansion, "initial insert should have needs_expansion=1")

	// Step 2: Manually set needs_expansion to 0 (simulating post-expansion state).
	_, err := c1f.db.ExecContext(ctx,
		"UPDATE "+grants.Name()+" SET needs_expansion=0 WHERE external_id='grant-stable'",
	)
	require.NoError(t, err)
	raw = getRawGrantRow(ctx, t, c1f, "grant-stable")
	require.Equal(t, 0, raw.needsExpansion, "sanity check: needs_expansion should be 0 after manual update")

	// Step 3: Upsert with identical expansion. needs_expansion should stay 0.
	identicalGrant := v2.Grant_builder{
		Id:          "grant-stable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(expandable),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, identicalGrant))

	raw = getRawGrantRow(ctx, t, c1f, "grant-stable")
	require.Equal(t, 0, raw.needsExpansion,
		"upsert with identical expansion should not flip needs_expansion back to 1")
}

// TestNeedsExpansion_ExpansionChangeSetsFlag verifies that when the expansion annotation
// changes (different entitlement IDs, shallow flag, etc.), needs_expansion is set to 1.
func TestNeedsExpansion_ExpansionChangeSetsFlag(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert with one expansion annotation.
	grant := v2.Grant_builder{
		Id:          "grant-changing",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        false,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant))

	// Step 2: Set needs_expansion to 0 (simulating post-expansion state).
	_, err := c1f.db.ExecContext(ctx,
		"UPDATE "+grants.Name()+" SET needs_expansion=0 WHERE external_id='grant-changing'",
	)
	require.NoError(t, err)

	// Step 3: Upsert with DIFFERENT expansion (Shallow changed to true).
	changedGrant := v2.Grant_builder{
		Id:          "grant-changing",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true, // Changed!
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, changedGrant))

	raw := getRawGrantRow(ctx, t, c1f, "grant-changing")
	require.Equal(t, 1, raw.needsExpansion,
		"upsert with changed expansion should set needs_expansion=1")
}

// TestNeedsExpansion_NonExpandableToExpandable verifies that when a grant transitions
// from non-expandable to expandable, needs_expansion is set to 1.
func TestNeedsExpansion_NonExpandableToExpandable(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant WITHOUT expansion.
	normalGrant := v2.Grant_builder{
		Id:          "grant-becoming-expandable",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, normalGrant))

	raw := getRawGrantRow(ctx, t, c1f, "grant-becoming-expandable")
	require.Nil(t, raw.expansion, "initial insert should have NULL expansion")
	require.Equal(t, 0, raw.needsExpansion, "non-expandable grant should have needs_expansion=0")

	// Step 2: Upsert with expansion annotation.
	expandableGrant := v2.Grant_builder{
		Id:          "grant-becoming-expandable",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	raw = getRawGrantRow(ctx, t, c1f, "grant-becoming-expandable")
	require.NotNil(t, raw.expansion, "after gaining expansion, column should be non-NULL")
	require.Equal(t, 1, raw.needsExpansion,
		"needs_expansion must be 1 when grant transitions from non-expandable to expandable")
}

// TestPutGrantsIfNewer_ExpansionColumnsUpdated verifies that PutGrantsIfNewer correctly
// handles expansion columns when the newer grant has a different expansion annotation.
func TestPutGrantsIfNewer_ExpansionColumnsUpdated(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant with expansion via PutGrants (the normal path).
	oldGrant := v2.Grant_builder{
		Id:          "grant-ifnewer",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        false,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, oldGrant))

	raw := getRawGrantRow(ctx, t, c1f, "grant-ifnewer")
	require.NotNil(t, raw.expansion)
	require.Equal(t, 1, raw.needsExpansion)
	oldExpansion := make([]byte, len(raw.expansion))
	copy(oldExpansion, raw.expansion)

	// Step 2: Upsert with different expansion via PutGrantsIfNewer.
	// Since this is the same sync, discovered_at should be the same or newer.
	newGrant := v2.Grant_builder{
		Id:          "grant-ifnewer",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent1", "ent2"}, // Changed!
			Shallow:        true,                     // Changed!
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrantsIfNewer(ctx, newGrant))

	raw = getRawGrantRow(ctx, t, c1f, "grant-ifnewer")
	require.NotNil(t, raw.expansion)

	// Verify the expansion was updated by checking the proto content.
	ge := &v2.GrantExpandable{}
	require.NoError(t, proto.Unmarshal(raw.expansion, ge))
	require.Equal(t, []string{"ent1", "ent2"}, ge.GetEntitlementIds())
	require.True(t, ge.GetShallow())

	// Step 3: Also verify via ListExpandableGrants.
	defs, _ := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{})
	require.Len(t, defs, 1)
	require.Equal(t, "grant-ifnewer", defs[0].GrantExternalID)
	require.Equal(t, []string{"ent1", "ent2"}, defs[0].SourceEntitlementIDs)
	require.True(t, defs[0].Shallow)
}

func TestListGrantsInternal_ModeValidation(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	tests := []struct {
		name    string
		opts    connectorstore.GrantListOptions
		errText string
	}{
		{
			name: "sync id not supported with payload mode",
			opts: connectorstore.GrantListOptions{
				Mode:   connectorstore.GrantListModePayload,
				SyncID: "sync-1",
			},
			errText: "SyncID is not supported for payload modes",
		},
		{
			name: "sync id not supported with payload+expansion mode",
			opts: connectorstore.GrantListOptions{
				Mode:   connectorstore.GrantListModePayloadWithExpansion,
				SyncID: "sync-1",
			},
			errText: "SyncID is not supported for payload modes",
		},
		{
			name: "options needs-expansion-only not supported in payload mode",
			opts: connectorstore.GrantListOptions{
				Mode:               connectorstore.GrantListModePayloadWithExpansion,
				NeedsExpansionOnly: true,
			},
			errText: "NeedsExpansionOnly does not support payload modes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c1f.ListGrantsInternal(ctx, tc.opts)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.errText)
		})
	}
}

func TestListGrantsInternal_WithPayloadAndExpansionIncludesAllGrantsWhenNeedsNotSet(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	expandableGrant := v2.Grant_builder{
		Id:          "grant-with-expansion",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
		}.Build()),
	}.Build()
	normalGrant := v2.Grant_builder{
		Id:          "grant-without-expansion",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant, normalGrant))

	resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayloadWithExpansion,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 2)

	byID := map[string]*connectorstore.InternalGrantRow{}
	for _, r := range resp.Rows {
		byID[r.Grant.GetId()] = r
	}
	require.NotNil(t, byID["grant-with-expansion"])
	require.NotNil(t, byID["grant-without-expansion"])
	require.NotNil(t, byID["grant-with-expansion"].Expansion)
	require.Nil(t, byID["grant-without-expansion"].Expansion)
}

func TestListGrantsInternal_RequestExpandableOnlyWithPayloadFiltersToExpandable(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	expandableGrant := v2.Grant_builder{
		Id:          "grant-expandable-only-filter",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
		}.Build()),
	}.Build()
	normalGrant := v2.Grant_builder{
		Id:          "grant-normal-filtered-out",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant, normalGrant))

	resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:           connectorstore.GrantListModePayloadWithExpansion,
		ExpandableOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	require.Equal(t, "grant-expandable-only-filter", resp.Rows[0].Grant.GetId())
	require.NotNil(t, resp.Rows[0].Expansion)
}

func TestListGrantsInternal_ExpansionOnlyPageSizeAndToken(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(
		ctx,
		v2.Grant_builder{
			Id:          "grant-expandable-paged-a",
			Entitlement: ent1,
			Principal:   u1,
			Annotations: annotations.New(v2.GrantExpandable_builder{EntitlementIds: []string{"ent2"}}.Build()),
		}.Build(),
		v2.Grant_builder{
			Id:          "grant-expandable-paged-b",
			Entitlement: ent1,
			Principal:   u1,
			Annotations: annotations.New(v2.GrantExpandable_builder{EntitlementIds: []string{"ent2"}}.Build()),
		}.Build(),
	))

	page1, next := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{
		Mode:     connectorstore.GrantListModeExpansion,
		SyncID:   syncID,
		PageSize: 1,
	})
	require.Len(t, page1, 1)
	require.NotEmpty(t, next)

	page2, next2 := listExpansionDefs(ctx, t, c1f, connectorstore.GrantListOptions{
		Mode:      connectorstore.GrantListModeExpansion,
		SyncID:    syncID,
		PageSize:  1,
		PageToken: next,
	})
	require.Len(t, page2, 1)
	require.Equal(t, "", next2)
}

func TestListGrantsInternal_PayloadWithExpansionPageSizeAndTokenFromOptions(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(
		ctx,
		v2.Grant_builder{Id: "grant-payload-page-a", Entitlement: ent1, Principal: u1}.Build(),
		v2.Grant_builder{Id: "grant-payload-page-b", Entitlement: ent1, Principal: u1}.Build(),
	))

	page1, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:     connectorstore.GrantListModePayloadWithExpansion,
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, page1.Rows, 1)
	require.NotEmpty(t, page1.NextPageToken)

	page2, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:      connectorstore.GrantListModePayloadWithExpansion,
		PageSize:  1,
		PageToken: page1.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Rows, 1)
	require.Equal(t, "", page2.NextPageToken)
}

// TestUpsertGrants_PreserveExpansion verifies that upserting a grant with
// GrantUpsertModePreserveExpansion keeps existing expansion and needs_expansion
// columns unchanged, even when the incoming grant carries different data.
func TestUpsertGrants_PreserveExpansion(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert a grant WITH expansion via normal PutGrants (Replace mode).
	expandableGrant := v2.Grant_builder{
		Id:          "grant-preserve",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	// Verify initial state: expansion is set and needs_expansion is 1.
	rawBefore := getRawGrantRowForSync(ctx, t, c1f, "grant-preserve", syncID)
	require.NotNil(t, rawBefore.expansion, "initial insert should have non-nil expansion")
	require.Equal(t, 1, rawBefore.needsExpansion, "initial insert should have needs_expansion=1")

	// Step 2: Upsert the same grant via PreserveExpansion with NO expansion annotation.
	// The grant data itself is different (no annotations), but expansion columns must not change.
	plainGrant := v2.Grant_builder{
		Id:          "grant-preserve",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
		Mode: connectorstore.GrantUpsertModePreserveExpansion,
	}, plainGrant))

	// Verify: expansion and needs_expansion are unchanged.
	rawAfter := getRawGrantRowForSync(ctx, t, c1f, "grant-preserve", syncID)
	require.NotNil(t, rawAfter.expansion, "expansion should be preserved after PreserveExpansion upsert")
	require.Equal(t, rawBefore.expansion, rawAfter.expansion, "expansion bytes should be identical")
	require.Equal(t, rawBefore.needsExpansion, rawAfter.needsExpansion, "needs_expansion should be identical")

	// Step 3: Verify the reverse case — a non-expandable grant stays non-expandable.
	plainGrant2 := v2.Grant_builder{
		Id:          "grant-preserve-plain",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, plainGrant2))

	rawPlainBefore := getRawGrantRowForSync(ctx, t, c1f, "grant-preserve-plain", syncID)
	require.Nil(t, rawPlainBefore.expansion, "plain grant should have nil expansion")
	require.Equal(t, 0, rawPlainBefore.needsExpansion, "plain grant should have needs_expansion=0")

	// Upsert the plain grant via PreserveExpansion — columns stay nil/0.
	require.NoError(t, c1f.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
		Mode: connectorstore.GrantUpsertModePreserveExpansion,
	}, plainGrant2))

	rawPlainAfter := getRawGrantRowForSync(ctx, t, c1f, "grant-preserve-plain", syncID)
	require.Nil(t, rawPlainAfter.expansion, "expansion should remain nil after PreserveExpansion upsert")
	require.Equal(t, 0, rawPlainAfter.needsExpansion, "needs_expansion should remain 0 after PreserveExpansion upsert")
}

// TestPutGrantsIfNewer_StaleUpsertIsNoOp verifies that IfNewer mode rejects an
// upsert when the existing row has a newer discovered_at timestamp. Both data
// and expansion columns must remain unchanged.
func TestPutGrantsIfNewer_StaleUpsertIsNoOp(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant with expansion.
	original := v2.Grant_builder{
		Id:          "grant-stale",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, original))

	rawBefore := getRawGrantRowForSync(ctx, t, c1f, "grant-stale", syncID)
	require.NotNil(t, rawBefore.expansion)
	require.Equal(t, 1, rawBefore.needsExpansion)

	// Push discovered_at far into the future so any new upsert is "stale".
	_, err := c1f.db.ExecContext(ctx,
		"UPDATE "+grants.Name()+" SET discovered_at = '2099-01-01 00:00:00' WHERE external_id = ?",
		"grant-stale",
	)
	require.NoError(t, err)

	// Capture data blob before stale upsert.
	var dataBefore []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-stale", syncID,
	).Scan(&dataBefore))

	// Step 2: Attempt IfNewer upsert with different expansion. It should be rejected.
	stale := v2.Grant_builder{
		Id:          "grant-stale",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent1", "ent2"},
			Shallow:        false,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrantsIfNewer(ctx, stale))

	// Verify nothing changed.
	rawAfter := getRawGrantRowForSync(ctx, t, c1f, "grant-stale", syncID)
	require.Equal(t, rawBefore.expansion, rawAfter.expansion, "expansion should not change for stale upsert")
	require.Equal(t, rawBefore.needsExpansion, rawAfter.needsExpansion, "needs_expansion should not change for stale upsert")

	// Verify data blob is also unchanged.
	var dataAfter []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-stale", syncID,
	).Scan(&dataAfter))
	require.Equal(t, dataBefore, dataAfter, "data blob should not change for stale upsert")
}

// TestPutGrantsIfNewer_PlainGrant verifies IfNewer mode works correctly for
// grants without expansion annotations — both the accept and reject paths.
func TestPutGrantsIfNewer_PlainGrant(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()
	require.NoError(t, c1f.PutResources(ctx, u2))

	// Step 1: Insert a plain grant (no expansion) via Replace.
	original := v2.Grant_builder{
		Id:          "grant-ifnewer-plain",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, original))

	rawBefore := getRawGrantRowForSync(ctx, t, c1f, "grant-ifnewer-plain", syncID)
	require.Nil(t, rawBefore.expansion)
	require.Equal(t, 0, rawBefore.needsExpansion)
	var dataBeforeAccepted []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-ifnewer-plain", syncID,
	).Scan(&dataBeforeAccepted))

	// Step 2: IfNewer upsert with changed payload (different principal).
	// This should be accepted and update the row data.
	updated := v2.Grant_builder{
		Id:          "grant-ifnewer-plain",
		Entitlement: ent1,
		Principal:   u2,
	}.Build()
	require.NoError(t, c1f.PutGrantsIfNewer(ctx, updated))

	rawAfter := getRawGrantRowForSync(ctx, t, c1f, "grant-ifnewer-plain", syncID)
	require.Nil(t, rawAfter.expansion, "plain grant should still have nil expansion after IfNewer upsert")
	require.Equal(t, 0, rawAfter.needsExpansion, "plain grant should still have needs_expansion=0 after IfNewer upsert")
	var dataAfterAccepted []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-ifnewer-plain", syncID,
	).Scan(&dataAfterAccepted))
	require.NotEqual(t, dataBeforeAccepted, dataAfterAccepted, "IfNewer accepted path should update data blob")

	// Step 3: Push discovered_at into the future and verify stale IfNewer is rejected.
	_, err := c1f.db.ExecContext(ctx,
		"UPDATE "+grants.Name()+" SET discovered_at = '2099-01-01 00:00:00' WHERE external_id = ?",
		"grant-ifnewer-plain",
	)
	require.NoError(t, err)

	// Capture data blob before stale upsert.
	var dataBefore []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-ifnewer-plain", syncID,
	).Scan(&dataBefore))

	stale := v2.Grant_builder{
		Id:          "grant-ifnewer-plain",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrantsIfNewer(ctx, stale))

	var dataAfter []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		"SELECT data FROM "+grants.Name()+" WHERE external_id = ? AND sync_id = ?",
		"grant-ifnewer-plain", syncID,
	).Scan(&dataAfter))
	require.Equal(t, dataBefore, dataAfter, "data blob should not change for stale plain IfNewer upsert")
}

// TestPutGrants_ReplaceOverwritesExpansion verifies that Replace mode unconditionally
// overwrites expansion columns when the new grant has different expansion metadata.
func TestPutGrants_ReplaceOverwritesExpansion(t *testing.T) {
	ctx := context.Background()
	c1f, syncID, cleanup := setupTestC1Z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	// Step 1: Insert grant with expansion {ent2, shallow=true}.
	grant1 := v2.Grant_builder{
		Id:          "grant-replace",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent2"},
			Shallow:        true,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant1))

	rawBefore := getRawGrantRowForSync(ctx, t, c1f, "grant-replace", syncID)
	require.NotNil(t, rawBefore.expansion)
	geBefore := &v2.GrantExpandable{}
	require.NoError(t, proto.Unmarshal(rawBefore.expansion, geBefore))
	require.Equal(t, []string{"ent2"}, geBefore.GetEntitlementIds())
	require.True(t, geBefore.GetShallow())

	// Step 2: Replace with different expansion {ent1, ent2, shallow=false}.
	grant2 := v2.Grant_builder{
		Id:          "grant-replace",
		Entitlement: ent1,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds: []string{"ent1", "ent2"},
			Shallow:        false,
		}.Build()),
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant2))

	rawAfter := getRawGrantRowForSync(ctx, t, c1f, "grant-replace", syncID)
	require.NotNil(t, rawAfter.expansion)
	geAfter := &v2.GrantExpandable{}
	require.NoError(t, proto.Unmarshal(rawAfter.expansion, geAfter))
	require.Equal(t, []string{"ent1", "ent2"}, geAfter.GetEntitlementIds(), "Replace should overwrite expansion entitlement IDs")
	require.False(t, geAfter.GetShallow(), "Replace should overwrite expansion shallow flag")

	// Step 3: Replace with NO expansion — should clear expansion to NULL.
	grant3 := v2.Grant_builder{
		Id:          "grant-replace",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant3))

	requireExpansionSQLNullForSync(ctx, t, c1f, "grant-replace", syncID)
	rawCleared := getRawGrantRowForSync(ctx, t, c1f, "grant-replace", syncID)
	require.Equal(t, 0, rawCleared.needsExpansion, "needs_expansion should be 0 after clearing expansion via Replace")
}
