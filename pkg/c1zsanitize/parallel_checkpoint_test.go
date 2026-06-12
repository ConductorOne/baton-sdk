package c1zsanitize

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

var fixedAnchor = time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

// anyTB packs a proto into an Any, failing the test/benchmark on error.
func anyTB(tb testing.TB, m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		tb.Fatal(err)
	}
	return a
}

// buildSyncFixture writes a c1z with nSyncs independent syncs, each carrying a
// couple resource types, resources (with trait annotations), entitlements, and
// grants embedding a full entitlement + principal. Enough shape to exercise
// every copy* phase and the per-page transform fan-out.
func buildSyncFixture(t testing.TB, ctx context.Context, path string, nSyncs, grantsPerSync int) {
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	for sIdx := 0; sIdx < nSyncs; sIdx++ {
		_, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		p := fmt.Sprintf("s%d-", sIdx)

		require.NoError(t, f.PutResourceTypes(ctx,
			v2.ResourceType_builder{Id: "user", DisplayName: "User", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build(),
			v2.ResourceType_builder{Id: "role", DisplayName: "Role", Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE}}.Build(),
		))

		// Each user carries a UserTrait Icon AssetRef, so the resource phase is
		// the source of resource-trait asset refs. copyAssets must reproduce
		// these on every run — including a resume that skips the resource WRITE —
		// or the sanitized output holds dangling AssetRefs. iconID below is the
		// source asset id; the corresponding asset row is written after.
		var resources []*v2.Resource
		var iconIDs []string
		for i := 0; i < grantsPerSync; i++ {
			iconID := fmt.Sprintf("%sicon-%d", p, i)
			iconIDs = append(iconIDs, iconID)
			resources = append(resources, v2.Resource_builder{
				Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("%su%d", p, i)}.Build(),
				DisplayName: fmt.Sprintf("User %d", i),
				Annotations: []*anypb.Any{anyTB(t, v2.UserTrait_builder{
					Login:  fmt.Sprintf("user%d@acme.com", i),
					Emails: []*v2.UserTrait_Email{v2.UserTrait_Email_builder{Address: fmt.Sprintf("user%d@acme.com", i), IsPrimary: true}.Build()},
					Icon:   v2.AssetRef_builder{Id: iconID}.Build(),
				}.Build())},
			}.Build())
		}
		role := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: p + "admin"}.Build(), DisplayName: "Admin"}.Build()
		resources = append(resources, role)
		require.NoError(t, f.PutResources(ctx, resources...))

		// The asset rows the resource icons reference. Without these the source
		// would carry dangling refs; with them, the sanitizer's copyAssets has a
		// real asset to fetch, placeholder, and re-write per icon.
		for _, iconID := range iconIDs {
			require.NoError(t, f.PutAsset(ctx, v2.AssetRef_builder{Id: iconID}.Build(), "image/png", []byte{0x89, 0x50, 0x4e, 0x47}))
		}

		ent := v2.Entitlement_builder{Id: p + "ent-admin", Resource: role, DisplayName: "admin", Slug: "admin"}.Build()
		require.NoError(t, f.PutEntitlements(ctx, ent))

		var grants []*v2.Grant
		for i := 0; i < grantsPerSync; i++ {
			principal := resources[i]
			grants = append(grants, v2.Grant_builder{
				Id:          fmt.Sprintf("%sgrant-%d", p, i),
				Entitlement: ent,
				Principal:   principal,
				Annotations: []*anypb.Any{anyTB(t, v2.ETag_builder{Value: fmt.Sprintf("etag-%d", i), EntitlementId: p + "ent-admin"}.Build())},
			}.Build())
		}
		require.NoError(t, f.PutGrants(ctx, grants...))
		require.NoError(t, f.EndSync(ctx))
	}
	require.NoError(t, f.Close(ctx))
}

func sanitizeToFile(t *testing.T, ctx context.Context, srcPath, dstPath string, secret []byte, opts Options) {
	src := mustOpen(t, ctx, srcPath, true)
	defer src.Close(ctx)
	dst := mustOpen(t, ctx, dstPath, false)
	opts.Secret = secret
	opts.TimestampAnchor = fixedAnchor
	require.NoError(t, Sanitize(ctx, src, dst, opts))
	require.NoError(t, dst.Close(ctx))
}

// TestSanitizeParallelMultiSync exercises the per-page transform fan-out across
// a multi-sync fixture. Run under -race, it is the concurrency guard for the
// parallel transform stage; it also confirms output is correct (record counts
// preserved) under fan-out.
func TestSanitizeParallelMultiSync(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	dstPath := filepath.Join(tmp, "dst.c1z")
	buildSyncFixture(t, ctx, srcPath, 2, 50)

	sanitizeToFile(t, ctx, srcPath, dstPath, bytes32("parallel-multisync"), Options{})

	srcRO := mustOpen(t, ctx, srcPath, true)
	defer srcRO.Close(ctx)
	dstRO := mustOpen(t, ctx, dstPath, true)
	defer dstRO.Close(ctx)
	srcRec := collectRecords(t, ctx, srcRO)
	dstRec := collectRecords(t, ctx, dstRO)
	require.Equal(t, len(srcRec.grants), len(dstRec.grants), "grant count preserved across fan-out")
	require.Equal(t, len(srcRec.resources), len(dstRec.resources))
	require.Equal(t, len(srcRec.entitlements), len(dstRec.entitlements))
	require.NotZero(t, len(dstRec.grants))
}

// interruptingWriter wraps a real C1File and fails the Nth PutGrants call,
// simulating a process kill at the start of the grant phase. Embedding the
// concrete *C1File promotes every method (including the optional sync-link /
// supports-diff capabilities) so Sanitize behaves as against a real writer.
type interruptingWriter struct {
	*dotc1z.C1File
	failOnPutGrantsCall int
	putGrantsCalls      int
}

func (w *interruptingWriter) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	w.putGrantsCalls++
	if w.putGrantsCalls >= w.failOnPutGrantsCall {
		return errors.New("simulated interruption")
	}
	return w.C1File.PutGrants(ctx, grants...)
}

// TestSanitizeCheckpointResume interrupts a resumable run at the grant phase,
// persists the partial destination, then resumes — and asserts the resumed
// output is record-identical to an uninterrupted run with the same secret and
// anchor.
func TestSanitizeCheckpointResume(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	cleanDst := filepath.Join(tmp, "clean.c1z")
	resumeDst := filepath.Join(tmp, "resume.c1z")
	secret := bytes32("checkpoint-resume")
	buildSyncFixture(t, ctx, srcPath, 1, 40)

	// Reference: a single uninterrupted resumable run.
	sanitizeToFile(t, ctx, srcPath, cleanDst, secret, Options{Resumable: true})

	// Interrupted run: fail on the first PutGrants (resources + entitlements
	// were written and checkpointed; grants did not start).
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &interruptingWriter{C1File: mustOpen(t, ctx, resumeDst, false), failOnPutGrantsCall: 1}
		err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
		require.Error(t, err, "run must be interrupted at the grant phase")
		require.NoError(t, w.Close(ctx)) // persist the partial destination
	}()

	// Resume into the persisted partial destination.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, resumeDst, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
		require.NoError(t, dst.Close(ctx))
	}()

	cleanRO := mustOpen(t, ctx, cleanDst, true)
	defer cleanRO.Close(ctx)
	resumeRO := mustOpen(t, ctx, resumeDst, true)
	defer resumeRO.Close(ctx)
	clean := collectRecords(t, ctx, cleanRO)
	resumed := collectRecords(t, ctx, resumeRO)

	require.NotZero(t, len(clean.grants))
	require.Equal(t, len(clean.grants), len(resumed.grants), "resumed grant count must match the uninterrupted run")
	require.Equal(t, len(clean.resources), len(resumed.resources))
	require.Equal(t, len(clean.entitlements), len(resumed.entitlements))
	require.Equal(t, clean.idOccurrences, resumed.idOccurrences, "resumed output must be record-identical to the uninterrupted run")
}

// startSyncInterruptingWriter wraps a real C1File and fails the Nth
// StartNewSync call, simulating a process kill after one source sync fully
// completed (EndSync ran) but before the next source sync's destination sync is
// created. Embedding the concrete *C1File promotes every other method,
// including ListSyncRuns, so resume reads checkpoints through it.
type startSyncInterruptingWriter struct {
	*dotc1z.C1File
	failOnStartCall int
	startCalls      int
}

func (w *startSyncInterruptingWriter) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	w.startCalls++
	if w.startCalls >= w.failOnStartCall {
		return "", errors.New("simulated interruption at sync start")
	}
	return w.C1File.StartNewSync(ctx, syncType, parentSyncID)
}

// grantsPerDstSync returns the grant count of each destination sync, keyed by
// dst sync id. It is the B1 guard: a run that wrote a later source sync's
// records into an earlier (ended) sync would show one sync with a doubled count
// and a missing sibling.
func grantsPerDstSync(t *testing.T, ctx context.Context, r connectorstore.Reader) map[string]int {
	t.Helper()
	syncs, err := listAllSyncs(ctx, r)
	require.NoError(t, err)
	out := map[string]int{}
	for _, sr := range syncs {
		page := ""
		for {
			resp, err := r.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
				PageSize:    1000,
				PageToken:   page,
				Annotations: syncIDAnnotations(sr.GetId()),
			}.Build())
			require.NoError(t, err)
			out[sr.GetId()] += len(resp.GetList())
			if resp.GetNextPageToken() == "" {
				break
			}
			page = resp.GetNextPageToken()
		}
	}
	return out
}

// TestSanitizeResumeMultiSyncNoCrossWrite proves B1: when run 1 finishes the
// first source sync and dies before the second source sync's destination sync
// exists, the resume must create a fresh destination sync for the second
// source — never append its records to the already-ended first sync. Before the
// fix, loadResumeStates left the destination's current-sync pointer on the
// ended sync and StartNewSync handed it back, so the second sync's records
// landed in the first.
func TestSanitizeResumeMultiSyncNoCrossWrite(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	cleanDst := filepath.Join(tmp, "clean.c1z")
	resumeDst := filepath.Join(tmp, "resume.c1z")
	secret := bytes32("multisync-b1")
	const perSync = 8
	buildSyncFixture(t, ctx, srcPath, 2, perSync)

	// Reference: a single uninterrupted run over both syncs.
	sanitizeToFile(t, ctx, srcPath, cleanDst, secret, Options{Resumable: true})

	// Interrupted run: complete the first source sync, then fail when the second
	// source sync tries to start (2nd StartNewSync). The persisted destination
	// holds exactly one ended sync.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &startSyncInterruptingWriter{C1File: mustOpen(t, ctx, resumeDst, false), failOnStartCall: 2}
		err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
		require.Error(t, err, "run must be interrupted at the second sync's start")
		require.NoError(t, w.Close(ctx))
	}()

	// Sanity: the partial destination has exactly one (ended) sync.
	func() {
		ro := mustOpen(t, ctx, resumeDst, true)
		defer ro.Close(ctx)
		syncs, err := listAllSyncs(ctx, ro)
		require.NoError(t, err)
		require.Len(t, syncs, 1, "interrupted run persisted exactly the first sync")
	}()

	// Resume: the second source sync must get its OWN destination sync.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, resumeDst, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
		require.NoError(t, dst.Close(ctx))
	}()

	cleanRO := mustOpen(t, ctx, cleanDst, true)
	defer cleanRO.Close(ctx)
	resumeRO := mustOpen(t, ctx, resumeDst, true)
	defer resumeRO.Close(ctx)

	cleanPer := grantsPerDstSync(t, ctx, cleanRO)
	resumePer := grantsPerDstSync(t, ctx, resumeRO)
	require.Len(t, resumePer, 2, "resume must produce two destination syncs, not one overloaded sync")
	require.Len(t, cleanPer, 2)
	for id, n := range resumePer {
		require.Equal(t, perSync, n, "dst sync %s must hold exactly one source sync's grants (no cross-write)", id)
	}

	clean := collectRecords(t, ctx, cleanRO)
	resumed := collectRecords(t, ctx, resumeRO)
	require.Equal(t, len(clean.grants), len(resumed.grants))
	require.Equal(t, clean.idOccurrences, resumed.idOccurrences, "resumed multi-sync output must be record-identical to the uninterrupted run")
}

// TestSanitizeResumePreservesAssetRefs proves B2: a resume that skips the
// resource WRITE must still re-collect resource-trait asset refs so copyAssets
// reproduces them. The interrupted run dies at the grant phase before copyAssets
// ever runs, so the asset rows exist only if the resume's read-only resource
// walk re-collected their refs. Before the fix, resume skipped copyResources
// entirely and the icon assets were never written.
func TestSanitizeResumePreservesAssetRefs(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	resumeDst := filepath.Join(tmp, "resume.c1z")
	secret := bytes32("asset-refs-b2")
	buildSyncFixture(t, ctx, srcPath, 1, 6)

	// Interrupted run: resources + entitlements written, grants fail (copyAssets
	// never reached).
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &interruptingWriter{C1File: mustOpen(t, ctx, resumeDst, false), failOnPutGrantsCall: 1}
		err := Sanitize(ctx, src, w, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true})
		require.Error(t, err)
		require.NoError(t, w.Close(ctx))
	}()

	// Resume to completion.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, resumeDst, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor, Resumable: true}))
		require.NoError(t, dst.Close(ctx))
	}()

	// Every source icon asset must be present in the resumed destination under
	// its sanitized id. The sanitized id is s.id(srcIconID) under the same
	// secret.
	idHelper := &sanitizer{secret: secret, hmacPool: newHMACPool(secret)}
	resumeRO := mustOpen(t, ctx, resumeDst, true)
	defer resumeRO.Close(ctx)
	for i := 0; i < 6; i++ {
		dstAssetID := idHelper.id(fmt.Sprintf("s0-icon-%d", i))
		_, r, err := resumeRO.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
			Asset: v2.AssetRef_builder{Id: dstAssetID}.Build(),
		}.Build())
		require.NoError(t, err, "resumed output must contain the resource-trait icon asset %d (B2: asset refs survive resume)", i)
		require.NoError(t, closeIfCloser(r))
	}
}

// TestSanitizeDefaultAnchorResume proves B3: a resumable run that does NOT pass
// an explicit TimestampAnchor must still resume. Before the fix the checkpoint
// fingerprint bound the anchor, and run 2's default time.Now() anchor produced
// a different fingerprint, so resume hard-failed. Now the anchor is persisted in
// the token and adopted on resume.
func TestSanitizeDefaultAnchorResume(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.c1z")
	resumeDst := filepath.Join(tmp, "resume.c1z")
	secret := bytes32("default-anchor-b3")
	buildSyncFixture(t, ctx, srcPath, 1, 10)

	// Interrupted run with a ZERO TimestampAnchor (anchor defaults to now).
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		w := &interruptingWriter{C1File: mustOpen(t, ctx, resumeDst, false), failOnPutGrantsCall: 1}
		err := Sanitize(ctx, src, w, Options{Secret: secret, Resumable: true})
		require.Error(t, err)
		require.NoError(t, w.Close(ctx))
	}()

	// Resume, again with a ZERO TimestampAnchor. This must succeed by adopting
	// the anchor the first run persisted, not fail on a fingerprint mismatch.
	func() {
		src := mustOpen(t, ctx, srcPath, true)
		defer src.Close(ctx)
		dst := mustOpen(t, ctx, resumeDst, false)
		require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, Resumable: true}),
			"a default-anchor resumable run must resume, not fail on an anchor-bound fingerprint")
		require.NoError(t, dst.Close(ctx))
	}()

	resumeRO := mustOpen(t, ctx, resumeDst, true)
	defer resumeRO.Close(ctx)
	rec := collectRecords(t, ctx, resumeRO)
	require.NotZero(t, len(rec.grants), "resumed default-anchor run must have completed the grant phase")
	require.Equal(t, 10, len(rec.grants))
}

// BenchmarkSanitize tracks end-to-end sanitize throughput. genGrants is the
// tracked size knob; the headline number is G=1_000_000 (raise genGrants for a
// real measurement — kept modest here so the benchmark is runnable in CI).
func BenchmarkSanitize(b *testing.B) {
	const genGrants = 2000
	ctx := context.Background()
	tmp := b.TempDir()
	srcPath := filepath.Join(tmp, "bench-src.c1z")
	buildSyncFixture(b, ctx, srcPath, 1, genGrants)
	secret := bytes32("bench-sanitize")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dstPath := filepath.Join(tmp, fmt.Sprintf("bench-dst-%d.c1z", i))
		src, err := dotc1z.NewC1ZFile(ctx, srcPath, dotc1z.WithReadOnly(true))
		if err != nil {
			b.Fatal(err)
		}
		dst, err := dotc1z.NewC1ZFile(ctx, dstPath)
		if err != nil {
			b.Fatal(err)
		}
		if err := Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}); err != nil {
			b.Fatal(err)
		}
		_ = dst.Close(ctx)
		_ = src.Close(ctx)
	}
}
