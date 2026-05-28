package dotc1z

import (
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// makeCandidate builds a SyncRun at a given relative
// time offset, expressed in seconds from a fixed base. Tests use
// integer offsets to keep the relative ordering obvious — sync at
// offset 0 is oldest, offset N is newest. EndedAt is +1 second
// after StartedAt unless ended is false.
func makeCandidate(id string, syncType connectorstore.SyncType, startSec int, ended bool) SyncRun {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	startedAt := base.Add(time.Duration(startSec) * time.Second)
	cand := SyncRun{
		ID:        id,
		Type:      syncType,
		StartedAt: &startedAt,
	}
	if ended {
		endedAt := startedAt.Add(time.Second)
		cand.EndedAt = &endedAt
	}
	return cand
}

// withLink returns a copy of c with LinkedSyncID set. Keeps the
// makeCandidate signature short while letting diff-sync cases set
// up linked pairs inline.
func withLink(c SyncRun, linkedID string) SyncRun {
	c.LinkedSyncID = linkedID
	return c
}

// assertDeletes is a set-comparison helper. SelectSyncsToDelete
// guarantees order only for the full-sync bucket (oldest first);
// partials and diff syncs ride along after. Tests assert membership
// rather than exact sequence so the test stays robust to internal
// re-ordering of the policy implementation.
func assertDeletes(t *testing.T, got, want []string) {
	t.Helper()
	gotSorted := append([]string(nil), got...)
	wantSorted := append([]string(nil), want...)
	sort.Strings(gotSorted)
	sort.Strings(wantSorted)
	if !reflect.DeepEqual(gotSorted, wantSorted) {
		t.Errorf("SelectSyncsToDelete:\n  got  = %v\n  want = %v", got, want)
	}
}

// -----------------------------------------------------------------------------
// SelectSyncsToDelete: structural edge cases
// -----------------------------------------------------------------------------

func TestSelectSyncsToDelete_EmptyCandidates(t *testing.T) {
	got := SelectSyncsToDelete(nil, "", 2)
	if len(got) != 0 {
		t.Errorf("empty candidates: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_AllUnfinished(t *testing.T) {
	// Three unfinished syncs of varying type. EndedAt == nil ⇒ in
	// flight; never selected for deletion regardless of syncLimit.
	cands := []SyncRun{
		makeCandidate("s1", connectorstore.SyncTypeFull, 0, false),
		makeCandidate("s2", connectorstore.SyncTypePartial, 10, false),
		makeCandidate("s3", connectorstore.SyncTypePartialUpserts, 20, false),
	}
	got := SelectSyncsToDelete(cands, "", 1)
	if len(got) != 0 {
		t.Errorf("all unfinished: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_SkipsCurrentSync(t *testing.T) {
	// The active sync is listed in candidates with an EndedAt
	// (defensive — caller might not have stamped EndedAt yet, but
	// the policy must not prune it regardless).
	cands := []SyncRun{
		makeCandidate("old", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("mid", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("active", connectorstore.SyncTypeFull, 20, true),
	}
	got := SelectSyncsToDelete(cands, "active", 1)
	// Buckets: fullSyncs = [old, mid]; active filtered out before
	// bucketing. overflow = 2 - 1 = 1, delete oldest (old).
	assertDeletes(t, got, []string{"old"})
}

// -----------------------------------------------------------------------------
// SelectSyncsToDelete: full-sync retention math
// -----------------------------------------------------------------------------

func TestSelectSyncsToDelete_FullSyncsBelowLimit_NoOp(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
	}
	// 2 full syncs, limit 2 ⇒ none over budget.
	got := SelectSyncsToDelete(cands, "", 2)
	if len(got) != 0 {
		t.Errorf("at limit: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_FullSyncsAtExactLimit_NoOp(t *testing.T) {
	// Edge: exactly equal to syncLimit (not strictly greater).
	// The "> syncLimit" check must not delete at parity.
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
	}
	got := SelectSyncsToDelete(cands, "", 3)
	if len(got) != 0 {
		t.Errorf("at exact limit: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_FullSyncsOverflow_DropsOldest(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("f4", connectorstore.SyncTypeFull, 30, true),
	}
	// 4 full syncs, limit 2 ⇒ drop the oldest 2.
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1", "f2"})
}

func TestSelectSyncsToDelete_UnknownTypeBucketsAsFull(t *testing.T) {
	// SQLite's default branch treats unrecognized types as full
	// syncs. A SyncType("frobnitz") (made up) must end up in
	// fullSyncs and be subject to the full-sync retention math.
	cands := []SyncRun{
		makeCandidate("u1", connectorstore.SyncType("frobnitz"), 0, true),
		makeCandidate("f1", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 20, true),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"u1"})
}

// -----------------------------------------------------------------------------
// SelectSyncsToDelete: partial-cleanup cutoff
// -----------------------------------------------------------------------------

func TestSelectSyncsToDelete_DropsPartialsBeforeEarliestKept(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("p_old", connectorstore.SyncTypePartial, 5, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("p_recent", connectorstore.SyncTypePartial, 15, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
	}
	// 3 full syncs, limit 2 ⇒ drop f1.
	// Earliest kept full sync: f2 (started at t=10).
	// p_old ended at t=6 ⇒ before t=10 ⇒ deleted.
	// p_recent ended at t=16 ⇒ after t=10 ⇒ kept.
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1", "p_old"})
}

func TestSelectSyncsToDelete_ResourcesOnlyIsPartial(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("ro_old", connectorstore.SyncTypeResourcesOnly, 5, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("ro_recent", connectorstore.SyncTypeResourcesOnly, 15, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
	}
	// SyncTypeResourcesOnly is bucketed with partials; same cutoff
	// math as TestSelectSyncsToDelete_DropsPartialsBeforeEarliestKept.
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1", "ro_old"})
}

func TestSelectSyncsToDelete_PartialsNotPrunedWhenFullSyncsAtLimit(t *testing.T) {
	// If no full sync overflow, the partial cleanup branch never
	// runs — old partials linger. This matches SQLite behavior; the
	// partial cutoff hinges on a full-sync prune happening.
	cands := []SyncRun{
		makeCandidate("p1", connectorstore.SyncTypePartial, 0, true),
		makeCandidate("f1", connectorstore.SyncTypeFull, 100, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 200, true),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	if len(got) != 0 {
		t.Errorf("no full-sync overflow: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_PartialCutoffSkippedWhenCutoffStartedAtNil(t *testing.T) {
	// Defensive: if the earliest-kept full sync has no StartedAt
	// (corrupt or pre-migration record), the partial cutoff step
	// silently skips rather than panicking.
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		{ID: "f2", Type: connectorstore.SyncTypeFull, EndedAt: &time.Time{}}, // no StartedAt
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("p1", connectorstore.SyncTypePartial, 1, true),
	}
	// 3 full syncs, limit 2 ⇒ drop f1. Earliest-kept = f2 (no
	// StartedAt) ⇒ partials not evaluated. p1 survives.
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1"})
}

// -----------------------------------------------------------------------------
// SelectSyncsToDelete: current-sync + decremented-limit quirks
// -----------------------------------------------------------------------------

func TestSelectSyncsToDelete_CurrentSyncDecrementedToZero(t *testing.T) {
	// Matches TestCleanupSyncLimitCurrentSync: 10 finished full
	// syncs + an open current sync, WithSyncLimit(1) → effective
	// syncLimit=0 after decrement. Drop all 10 finished full syncs;
	// "active" was filtered out at the top because it's the current
	// sync.
	var cands []SyncRun
	for i := 0; i < 10; i++ {
		cands = append(cands, makeCandidate("f"+strconv.Itoa(i), connectorstore.SyncTypeFull, i*10, true))
	}
	got := SelectSyncsToDelete(cands, "active", 0)
	want := []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9"}
	assertDeletes(t, got, want)
}

func TestSelectSyncsToDelete_CurrentSyncCutoffBackUpQuirk(t *testing.T) {
	// Captures the quirky SQLite cutoff math at sync_runs.go:862-866.
	// With currentSyncOpen=true the partial cutoff uses
	// fullSyncs[overflow - 1].StartedAt instead of fullSyncs[overflow].
	//
	// Setup: limit=1, current sync open ⇒ effective limit=0.
	// fullSyncs (oldest→newest): f0 (t=0), f1 (t=10), f2 (t=20).
	// All three get deleted. Cutoff index = overflow(3) - 1 = 2 ⇒
	// fullSyncs[2] = f2 (started at t=20).
	// p_t5 ended at t=6 ⇒ before t=20 ⇒ deleted.
	// p_t25 ended at t=26 ⇒ after t=20 ⇒ kept.
	cands := []SyncRun{
		makeCandidate("f0", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("p_t5", connectorstore.SyncTypePartial, 5, true),
		makeCandidate("f1", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("p_t25", connectorstore.SyncTypePartial, 25, true),
	}
	got := SelectSyncsToDelete(cands, "active", 0)
	assertDeletes(t, got, []string{"f0", "f1", "f2", "p_t5"})
}

func TestSelectSyncsToDelete_NoCurrentSyncCutoffNormal(t *testing.T) {
	// Counterpart to the previous test: no current sync, so cutoff
	// index = overflow (not decremented). Same fullSyncs but
	// limit=2 (no decrement). Drop f0; earliest-kept = f1 (t=10).
	cands := []SyncRun{
		makeCandidate("f0", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("p_t5", connectorstore.SyncTypePartial, 5, true),
		makeCandidate("f1", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("p_t25", connectorstore.SyncTypePartial, 25, true),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f0", "p_t5"})
}

// -----------------------------------------------------------------------------
// SelectSyncsToDelete: diff-sync retention
// -----------------------------------------------------------------------------

func TestSelectSyncsToDelete_TwoDiffSyncs_NoOp(t *testing.T) {
	// The "> 2" guard means we don't touch diff syncs until there
	// are at least three. Exactly two is a no-op even if they're
	// not a linked pair.
	cands := []SyncRun{
		makeCandidate("d1", connectorstore.SyncTypePartialUpserts, 0, true),
		makeCandidate("d2", connectorstore.SyncTypePartialDeletions, 10, true),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	if len(got) != 0 {
		t.Errorf("two diff syncs: got %v, want []", got)
	}
}

func TestSelectSyncsToDelete_ManyDiffSyncs_KeepsLatestOnly(t *testing.T) {
	// Latest diff sync has no LinkedSyncID; keep only it.
	cands := []SyncRun{
		makeCandidate("d1", connectorstore.SyncTypePartialUpserts, 0, true),
		makeCandidate("d2", connectorstore.SyncTypePartialDeletions, 10, true),
		makeCandidate("d3", connectorstore.SyncTypePartialUpserts, 20, true),
		makeCandidate("d4", connectorstore.SyncTypePartialUpserts, 30, true),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"d1", "d2", "d3"})
}

func TestSelectSyncsToDelete_ManyDiffSyncs_KeepsLinkedPair(t *testing.T) {
	// Latest diff sync (d4) links to d3 ⇒ keep both. Drop d1 and d2.
	cands := []SyncRun{
		makeCandidate("d1", connectorstore.SyncTypePartialUpserts, 0, true),
		makeCandidate("d2", connectorstore.SyncTypePartialDeletions, 10, true),
		withLink(makeCandidate("d3", connectorstore.SyncTypePartialDeletions, 20, true), "d4"),
		withLink(makeCandidate("d4", connectorstore.SyncTypePartialUpserts, 30, true), "d3"),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"d1", "d2"})
}

func TestSelectSyncsToDelete_ManyDiffSyncs_LinkedPartnerMissing(t *testing.T) {
	// Latest diff sync (d4) claims to link to a missing sync.
	// Policy doesn't fabricate the partner ⇒ keep only d4.
	cands := []SyncRun{
		makeCandidate("d1", connectorstore.SyncTypePartialUpserts, 0, true),
		makeCandidate("d2", connectorstore.SyncTypePartialDeletions, 10, true),
		makeCandidate("d3", connectorstore.SyncTypePartialUpserts, 20, true),
		withLink(makeCandidate("d4", connectorstore.SyncTypePartialUpserts, 30, true), "ghost"),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"d1", "d2", "d3"})
}

func TestSelectSyncsToDelete_DiffSyncsIndependentOfFullSyncs(t *testing.T) {
	// Both branches active in one call: 4 full syncs (drop 2) +
	// 4 diff syncs with a linked latest pair (drop the other 2).
	cands := []SyncRun{
		makeCandidate("f1", connectorstore.SyncTypeFull, 0, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 10, true),
		makeCandidate("f3", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("f4", connectorstore.SyncTypeFull, 30, true),
		makeCandidate("d1", connectorstore.SyncTypePartialUpserts, 1, true),
		makeCandidate("d2", connectorstore.SyncTypePartialDeletions, 11, true),
		withLink(makeCandidate("d3", connectorstore.SyncTypePartialDeletions, 21, true), "d4"),
		withLink(makeCandidate("d4", connectorstore.SyncTypePartialUpserts, 31, true), "d3"),
	}
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"f1", "f2", "d1", "d2"})
}

// -----------------------------------------------------------------------------
// ResolveCleanupSyncLimit
// -----------------------------------------------------------------------------

func TestResolveCleanupSyncLimit_Default(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "")
	if got := ResolveCleanupSyncLimit(0, false); got != defaultCleanupSyncLimit {
		t.Errorf("default: got %d, want %d", got, defaultCleanupSyncLimit)
	}
}

func TestResolveCleanupSyncLimit_DefaultWithCurrentSync(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "")
	// Default 2, current sync ⇒ 1 effective.
	if got := ResolveCleanupSyncLimit(0, true); got != 1 {
		t.Errorf("default + current: got %d, want 1", got)
	}
}

func TestResolveCleanupSyncLimit_CallerLimitWins(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "99")
	// Caller-supplied limit precedes the env var.
	if got := ResolveCleanupSyncLimit(5, false); got != 5 {
		t.Errorf("caller wins over env: got %d, want 5", got)
	}
}

func TestResolveCleanupSyncLimit_EnvFallback(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "7")
	if got := ResolveCleanupSyncLimit(0, false); got != 7 {
		t.Errorf("env fallback: got %d, want 7", got)
	}
}

func TestResolveCleanupSyncLimit_EnvMalformedFallsBackToDefault(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "garbage")
	if got := ResolveCleanupSyncLimit(0, false); got != defaultCleanupSyncLimit {
		t.Errorf("malformed env: got %d, want %d", got, defaultCleanupSyncLimit)
	}
}

func TestResolveCleanupSyncLimit_EnvZeroIgnored(t *testing.T) {
	// Env value must be strictly positive to take effect — 0 falls
	// through to the default. Matches the > 0 check in
	// (*C1File).Cleanup.
	t.Setenv("BATON_KEEP_SYNC_COUNT", "0")
	if got := ResolveCleanupSyncLimit(0, false); got != defaultCleanupSyncLimit {
		t.Errorf("env 0: got %d, want %d", got, defaultCleanupSyncLimit)
	}
}

func TestResolveCleanupSyncLimit_DecrementFloorAtZero(t *testing.T) {
	t.Setenv("BATON_KEEP_SYNC_COUNT", "")
	// Caller passes 1, current sync ⇒ 0 (would underflow without
	// the floor in the decrement branch).
	if got := ResolveCleanupSyncLimit(1, true); got != 0 {
		t.Errorf("decrement to floor: got %d, want 0", got)
	}
}

// -----------------------------------------------------------------------------
// CleanupSkippedByEnv
// -----------------------------------------------------------------------------

func TestCleanupSkippedByEnv(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  string
		want bool
	}{
		{"unset", "", false},
		{"true", "true", true},
		{"1", "1", true},
		{"false", "false", false},
		{"0", "0", false},
		{"garbage", "garbage", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BATON_SKIP_CLEANUP", tc.val)
			if got := CleanupSkippedByEnv(); got != tc.want {
				t.Errorf("env=%q: got %v, want %v", tc.val, got, tc.want)
			}
		})
	}
}
