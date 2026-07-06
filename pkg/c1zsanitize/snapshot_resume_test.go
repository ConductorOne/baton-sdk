package c1zsanitize

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// buildManyGrantsFixture writes a single sync with nGrants grants on one
// entitlement, drawing principals from a tiny pool so the fixture stays cheap
// while still exceeding listPageSize. With nGrants > listPageSize the sanitizer
// reads grants in multiple pages, so an interruption on the second page leaves a
// NON-empty grant page token in the destination checkpoint.
func buildManyGrantsFixture(t *testing.T, ctx context.Context, path string, nGrants int) {
	t.Helper()
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)

	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
	))

	role := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: "admin"}.Build(), DisplayName: "Admin"}.Build()
	users := make([]*v2.Resource, 4)
	for i := range users {
		users[i] = v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
			DisplayName: "User " + strconv.Itoa(i),
		}.Build()
	}
	require.NoError(t, f.PutResources(ctx, append([]*v2.Resource{role}, users...)...))

	ent := v2.Entitlement_builder{Id: "ent-admin", Resource: role, DisplayName: "admin", Slug: "admin"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, ent))

	grants := make([]*v2.Grant, nGrants)
	for i := 0; i < nGrants; i++ {
		grants[i] = v2.Grant_builder{
			Id:          "grant-" + strconv.Itoa(i),
			Entitlement: ent,
			Principal:   users[i%len(users)],
		}.Build()
	}
	require.NoError(t, f.PutGrants(ctx, grants...))
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))
}

func listAllRuns(t *testing.T, ctx context.Context, f *dotc1z.C1File) []*c1zstore.SyncRun {
	t.Helper()
	var out []*c1zstore.SyncRun
	pageToken := ""
	for {
		runs, next, err := f.ListSyncRuns(ctx, pageToken, 1000)
		require.NoError(t, err)
		out = append(out, runs...)
		if next == "" {
			return out
		}
		pageToken = next
	}
}

// TestSnapshotResumeRecordIdentical is the headline test: a mid-sanitize
// destination is materialized with SnapshotTo (no Close, no re-decompress),
// reopened, and a resumable Sanitize is run against the snapshot. The
// reopened-and-resumed output must be record-identical to a single uninterrupted
// run with the same secret and anchor. Two interrupt points are covered: the
// resources/entitlements boundary (empty grant page token) and mid-grants (a
// non-empty grant page token in the snapshot's sync_token).
func TestSnapshotResumeRecordIdentical(t *testing.T) {
	t.Run("boundary", func(t *testing.T) {
		ctx := context.Background()
		tmp := t.TempDir()
		srcPath := filepath.Join(tmp, "src.c1z")
		cleanDst := filepath.Join(tmp, "clean.c1z")
		workDst := filepath.Join(tmp, "work.c1z")
		snapPath := filepath.Join(tmp, "snap.c1z")
		secret := bytes32("snapshot-resume-boundary")
		buildSyncFixture(t, ctx, srcPath, 1, 40)

		// Reference: one uninterrupted resumable run.
		sanitizeToFile(t, ctx, srcPath, cleanDst, secret, Options{Resumable: true})

		// Interrupt at the first PutGrants — resources + entitlements written and
		// checkpointed at phaseGrants with an empty page token. Snapshot the LIVE
		// destination at that checkpoint, then resume from the snapshot.
		func() {
			src := mustOpen(t, ctx, srcPath, true)
			defer src.Close(ctx)
			w := &interruptingWriter{C1File: mustOpen(t, ctx, workDst, false), failOnPutGrantsCall: 1}
			err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
			require.Error(t, err, "run must be interrupted at the grant phase")
			require.NoError(t, w.SnapshotTo(ctx, snapPath, dotc1z.WithC1FBulkLoad(true), dotc1z.WithC1FSkipVacuum(true)))
			require.NoError(t, w.Close(ctx))
		}()

		// Resume into the snapshot.
		func() {
			src := mustOpen(t, ctx, srcPath, true)
			defer src.Close(ctx)
			dst := mustOpen(t, ctx, snapPath, false)
			require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
			require.NoError(t, dst.Close(ctx))
		}()

		cleanRO := mustOpen(t, ctx, cleanDst, true)
		defer cleanRO.Close(ctx)
		snapRO := mustOpen(t, ctx, snapPath, true)
		defer snapRO.Close(ctx)
		clean := collectRecords(t, ctx, cleanRO)
		resumed := collectRecords(t, ctx, snapRO)
		require.NotZero(t, len(clean.grants))
		require.Equal(t, clean.idOccurrences, resumed.idOccurrences,
			"resume-from-snapshot output must be record-identical to the uninterrupted run")
	})

	t.Run("mid-grants", func(t *testing.T) {
		ctx := context.Background()
		tmp := t.TempDir()
		srcPath := filepath.Join(tmp, "src.c1z")
		cleanDst := filepath.Join(tmp, "clean.c1z")
		workDst := filepath.Join(tmp, "work.c1z")
		snapPath := filepath.Join(tmp, "snap.c1z")
		secret := bytes32("snapshot-resume-mid-grants")
		// One sync with more than listPageSize (10000) grants → multiple grant
		// pages, so a second-page interruption produces a non-empty page token.
		buildManyGrantsFixture(t, ctx, srcPath, 10001)

		sanitizeToFile(t, ctx, srcPath, cleanDst, secret, Options{Resumable: true})

		func() {
			src := mustOpen(t, ctx, srcPath, true)
			defer src.Close(ctx)
			// Fail the SECOND grant page: page one (10000 grants) is written and a
			// non-empty grant page token is checkpointed before page two fails.
			w := &interruptingWriter{C1File: mustOpen(t, ctx, workDst, false), failOnPutGrantsCall: 2}
			err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
			require.Error(t, err, "run must be interrupted on the second grant page")

			// The live checkpoint must carry a non-empty grant page token.
			runs := listAllRuns(t, ctx, w.C1File)
			require.Len(t, runs, 1)
			phase, gpt, _, ok := ResumePhase(runs[0].SyncToken)
			require.True(t, ok)
			require.Equal(t, "grants", phase)
			require.NotEmpty(t, gpt, "mid-grants interruption must checkpoint a non-empty grant page token")

			require.NoError(t, w.SnapshotTo(ctx, snapPath, dotc1z.WithC1FBulkLoad(true), dotc1z.WithC1FSkipVacuum(true)))
			require.NoError(t, w.Close(ctx))
		}()

		func() {
			src := mustOpen(t, ctx, srcPath, true)
			defer src.Close(ctx)
			dst := mustOpen(t, ctx, snapPath, false)
			require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
			require.NoError(t, dst.Close(ctx))
		}()

		cleanRO := mustOpen(t, ctx, cleanDst, true)
		defer cleanRO.Close(ctx)
		snapRO := mustOpen(t, ctx, snapPath, true)
		defer snapRO.Close(ctx)
		clean := collectRecords(t, ctx, cleanRO)
		resumed := collectRecords(t, ctx, snapRO)
		require.Equal(t, len(clean.grants), len(resumed.grants))
		require.Equal(t, clean.idOccurrences, resumed.idOccurrences,
			"resume-from-mid-grants-snapshot output must be record-identical to the uninterrupted run")
	})
}

// TestSnapshotPreservesSyncTokenByteForByte covers Test 3: the snapshot
// reproduces each sync's sync_token bytes exactly, and ResumePhase decodes the
// same phase/grant-page from the snapshot as from the live source.
func TestSnapshotPreservesSyncTokenByteForByte(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	workDst := filepath.Join(tmp, "work.c1z")
	snapPath := filepath.Join(tmp, "snap.c1z")
	secret := bytes32("snapshot-token-bytes")
	buildManyGrantsFixture(t, ctx, srcPath, 10001)

	var liveTokens map[string]string
	var livePhase, liveGPT, liveSrc string
	var liveOK bool

	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &interruptingWriter{C1File: mustOpen(t, ctx, workDst, false), failOnPutGrantsCall: 2}
		err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
		require.Error(t, err)

		liveTokens = map[string]string{}
		for _, r := range listAllRuns(t, ctx, w.C1File) {
			liveTokens[r.ID] = r.SyncToken
		}
		require.NotEmpty(t, liveTokens)
		// Capture the decoded phase of the (single) destination sync for the
		// cross-check below.
		for _, tok := range liveTokens {
			livePhase, liveGPT, liveSrc, liveOK = ResumePhase(tok)
		}
		require.True(t, liveOK)

		require.NoError(t, w.SnapshotTo(ctx, snapPath))
		require.NoError(t, w.Close(ctx))
	}()

	snap := mustOpen(t, ctx, snapPath, true)
	defer snap.Close(ctx)
	snapTokens := map[string]string{}
	for _, r := range listAllRuns(t, ctx, snap) {
		snapTokens[r.ID] = r.SyncToken
	}

	require.Equal(t, liveTokens, snapTokens, "snapshot sync_token bytes must match the source exactly")

	// ResumePhase decodes identically from the snapshot.
	for _, tok := range snapTokens {
		phase, gpt, src, ok := ResumePhase(tok)
		require.True(t, ok)
		require.Equal(t, livePhase, phase)
		require.Equal(t, liveGPT, gpt)
		require.Equal(t, liveSrc, src)
	}
}
