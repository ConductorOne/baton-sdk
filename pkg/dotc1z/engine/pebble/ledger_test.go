package pebble

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func ledgerTestResource(rt, id string) *v3.ResourceRecord {
	return v3.ResourceRecord_builder{ResourceTypeId: rt, ResourceId: id}.Build()
}

func ledgerTestEntitlement(rt, rid, ent string) *v3.EntitlementRecord {
	return v3.EntitlementRecord_builder{
		ExternalId: rt + ":" + rid + ":" + ent,
		Resource:   v3.ResourceRef_builder{ResourceTypeId: rt, ResourceId: rid}.Build(),
	}.Build()
}

func grantsPageIdentity(rid, token string) LedgerIdentity {
	return LedgerIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: rid, PageToken: token}
}

func TestLedgerKeyEncoding(t *testing.T) {
	base := grantsPageIdentity("github", "p1")

	require.Equal(t, encodeLedgerKey(base), encodeLedgerKey(base), "identity ⇒ key is a function")

	variants := []LedgerIdentity{
		{Op: "SyncEntitlements", ResourceTypeID: "app", ResourceID: "github", PageToken: "p1"},
		{Op: "SyncGrants", ResourceTypeID: "group", ResourceID: "github", PageToken: "p1"},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "gitlab", PageToken: "p1"},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", ParentResourceTypeID: "org", PageToken: "p1"},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", ParentResourceID: "acme", PageToken: "p1"},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: "p2"},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: "p1", TypeScoped: true},
		{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: ""},
	}
	seen := map[string]LedgerIdentity{string(encodeLedgerKey(base)): base}
	for _, v := range variants {
		k := string(encodeLedgerKey(v))
		prev, dup := seen[k]
		require.False(t, dup, "distinct identities must not share a key: %+v vs %+v", v, prev)
		seen[k] = v
	}

	// Separator-bearing components stay inside their element (tuple
	// escaping), so an id that contains 0x00 cannot forge a boundary.
	forged := LedgerIdentity{Op: "SyncGrants", ResourceTypeID: "app\x00github", ResourceID: "", PageToken: "p1"}
	require.NotEqual(t, encodeLedgerKey(forged), encodeLedgerKey(base))

	// Prefix scans: by op and by (op, resource) bound exactly their rows.
	byOp := encodeLedgerPrefixOp("SyncGrants")
	byRes := encodeLedgerPrefixResource("SyncGrants", "app", "github")
	for k, id := range seen {
		kb := []byte(k)
		require.Equal(t, id.Op == "SyncGrants", bytes.HasPrefix(kb, byOp), "%+v", id)
		require.Equal(t, id.Op == "SyncGrants" && id.ResourceTypeID == "app" && id.ResourceID == "github",
			bytes.HasPrefix(kb, byRes), "%+v", id)
	}
	// "github" must not match "github2" under the by-resource prefix.
	require.False(t, bytes.HasPrefix(encodeLedgerKey(grantsPageIdentity("github2", "p1")), byRes))

	lo, hi := LedgerLowerBound(), LedgerUpperBound()
	for k := range seen {
		require.True(t, bytes.Compare([]byte(k), lo) >= 0 && bytes.Compare([]byte(k), hi) < 0, "key within family bounds")
	}
}

func TestPageUnitCommitIsOneFact(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	id := grantsPageIdentity("github", "p1")
	child := LedgerIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: "spawn-7", TypeScoped: true}

	// Nothing before: the identity resolves to "never ran".
	_, err = e.GetLedgerRowRecord(ctx, id)
	require.ErrorIs(t, err, pebble.ErrNotFound)

	u := e.NewPageUnit()
	require.NoError(t, u.StageResourceTypes(v3.ResourceTypeRecord_builder{ExternalId: "app"}.Build()))
	require.NoError(t, u.StageResources(ledgerTestResource("app", "github"), ledgerTestResource("user", "alice")))
	require.NoError(t, u.StageEntitlements(ledgerTestEntitlement("app", "github", "ent-A")))
	require.NoError(t, u.StageGrants(testGrantRecord("ent-A", "alice")))
	require.False(t, u.Empty())

	row := v3.LedgerRow_builder{
		NextPageToken: "p2",
		Children:      []*v3.LedgerActionIdentity{ledgerIdentityToProto(child)},
		Attempt:       "attempt-1",
	}.Build()
	require.NoError(t, u.Commit(ctx, id, row))

	// Spent after commit.
	require.ErrorIs(t, u.StageGrants(testGrantRecord("ent-A", "bob")), ErrPageUnitCommitted)
	require.ErrorIs(t, u.Commit(ctx, id, nil), ErrPageUnitCommitted)

	// Records landed through the same typed paths Put*Records use.
	_, err = e.GetResourceTypeRecord(ctx, "app")
	require.NoError(t, err)
	_, err = e.GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	n := 0
	require.NoError(t, e.IterateEntitlementsByResource(ctx, "app", "github", func(*v3.EntitlementRecord) bool { n++; return true }))
	require.Equal(t, 1, n)
	n = 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(*v3.GrantRecord) bool { n++; return true }))
	require.Equal(t, 1, n, "inline by_principal index must serve the unit's grant")

	// The row landed with them, echoing identity and completion.
	got, err := e.GetLedgerRowRecord(ctx, id)
	require.NoError(t, err)
	require.Equal(t, id, ledgerIdentityFromProto(got.GetIdentity()))
	require.Equal(t, ledgerTokenHash("p1"), got.GetIdentity().GetPageTokenHash())
	require.Equal(t, "p2", got.GetNextPageToken())
	require.Equal(t, ledgerTokenHash("p2"), got.GetNextPageTokenHash())
	require.Len(t, got.GetChildren(), 1)
	require.Equal(t, child, ledgerIdentityFromProto(got.GetChildren()[0]))
	require.Equal(t, ledgerTokenHash("spawn-7"), got.GetChildren()[0].GetPageTokenHash())
	require.Equal(t, "attempt-1", got.GetAttempt())
	require.NotNil(t, got.GetCommittedAt())
	require.EqualValues(t, 1, got.GetResourceTypesWritten())
	require.EqualValues(t, 2, got.GetResourcesWritten())
	require.EqualValues(t, 1, got.GetEntitlementsWritten())
	require.EqualValues(t, 1, got.GetGrantsWritten())
	require.False(t, got.GetScrubbed())

	// The caller's row object was not aliased by the store.
	row.SetAttempt("mutated-after-commit")
	got, err = e.GetLedgerRowRecord(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "attempt-1", got.GetAttempt())

	// A neighbouring page is still "never ran".
	_, err = e.GetLedgerRowRecord(ctx, grantsPageIdentity("github", "p2"))
	require.ErrorIs(t, err, pebble.ErrNotFound)

	// An empty page still records that it ran.
	empty := e.NewPageUnit()
	require.True(t, empty.Empty())
	require.NoError(t, empty.Commit(ctx, grantsPageIdentity("github", "p2"), nil))
	got, err = e.GetLedgerRowRecord(ctx, grantsPageIdentity("github", "p2"))
	require.NoError(t, err)
	require.Zero(t, got.GetGrantsWritten())
	require.Empty(t, got.GetNextPageToken(), "bare completion: action finished")

	cnt, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, cnt)

	// Prefix iteration finds both pages of the resource.
	n = 0
	require.NoError(t, e.IterateLedgerByResource(ctx, "SyncGrants", "app", "github", func(*v3.LedgerRow) bool { n++; return true }))
	require.Equal(t, 2, n)
	n = 0
	require.NoError(t, e.IterateLedgerByOp(ctx, "SyncEntitlements", func(*v3.LedgerRow) bool { n++; return true }))
	require.Zero(t, n)
}

// A failed commit is indistinguishable from a page that never ran:
// no record, no row — and the unit survives for the retry.
func TestPageUnitFailedCommitLandsNothing(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	id := grantsPageIdentity("github", "p1")
	u := e.NewPageUnit()
	require.NoError(t, u.StageResources(ledgerTestResource("user", "alice")))
	require.NoError(t, u.StageGrants(testGrantRecord("ent-A", "alice")))

	boom := errors.New("injected commit failure")
	e.db.SetRecordCommitTestHook(func() error { return boom })
	require.ErrorIs(t, u.Commit(ctx, id, nil), boom)

	_, err = e.GetResourceRecord(ctx, "user", "alice")
	require.ErrorIs(t, err, pebble.ErrNotFound, "no record from a failed page")
	_, err = e.GetLedgerRowRecord(ctx, id)
	require.ErrorIs(t, err, pebble.ErrNotFound, "no row from a failed page")
	n := 0
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", "alice", func(*v3.GrantRecord) bool { n++; return true }))
	require.Zero(t, n, "no index entry from a failed page")

	e.db.SetRecordCommitTestHook(nil)
	require.NoError(t, u.Commit(ctx, id, nil), "the unit is reusable after a failed commit")
	_, err = e.GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	_, err = e.GetLedgerRowRecord(ctx, id)
	require.NoError(t, err)

	// Discard is the other exit: also nothing.
	d := e.NewPageUnit()
	require.NoError(t, d.StageResources(ledgerTestResource("user", "bob")))
	d.Discard()
	require.ErrorIs(t, d.Commit(ctx, grantsPageIdentity("github", "p2"), nil), ErrPageUnitCommitted)
	_, err = e.GetResourceRecord(ctx, "user", "bob")
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestPageUnitReadSeesOwnWrites(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	u := e.NewPageUnit()
	_, err = u.GetResourceRecord(ctx, "user", "alice")
	require.ErrorIs(t, err, pebble.ErrNotFound)

	first := ledgerTestResource("user", "alice")
	require.NoError(t, u.StageResources(first))
	got, err := u.GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	require.Same(t, first, got, "the page sees its own staged resource before commit")

	// Latest staged wins, matching the commit's last-occurrence dedup.
	second := ledgerTestResource("user", "alice")
	second.SetParent(v3.ResourceRef_builder{ResourceTypeId: "org", ResourceId: "acme"}.Build())
	require.NoError(t, u.StageResources(second))
	got, err = u.GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	require.Same(t, second, got)

	// Other pages' committed data still reads through to the DB.
	require.NoError(t, e.PutResourceRecords(ctx, ledgerTestResource("user", "bob")))
	got, err = u.GetResourceRecord(ctx, "user", "bob")
	require.NoError(t, err)
	require.Equal(t, "bob", got.GetResourceId())

	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	stored, err := e.GetResourceRecord(ctx, "user", "alice")
	require.NoError(t, err)
	require.Equal(t, "acme", stored.GetParent().GetResourceId(), "last staged occurrence is what lands")
	n := 0
	require.NoError(t, e.IterateResourcesByParent(ctx, "org", "acme", func(*v3.ResourceRecord) bool { n++; return true }))
	require.Equal(t, 1, n, "by_parent obligation staged for the winning occurrence")
}

// A row at the identity's key that echoes a DIFFERENT identity is a
// key-function bug (an omitted distinguishing field). The read side
// must refuse it — the page re-runs — and count it, never skip.
func TestLedgerIdentityMismatchReadsAsAbsent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	x := grantsPageIdentity("github", "p1")
	y := grantsPageIdentity("github", "p1")
	y.ParentResourceID = "acme" // a field the (hypothetically buggy) key dropped

	// Plant: Y's key holding a row that echoes X. This state is
	// unconstructible through the production API (the unit derives
	// key and echo from the same identity), which is the point.
	rowX := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(x), NextPageToken: "p2"}.Build()
	val, err := marshalRecord(rowX)
	require.NoError(t, err)
	require.NoError(t, e.db.UnsafeForTesting().Set(encodeLedgerKey(y), val, pebble.Sync))

	_, err = e.GetLedgerRowRecord(ctx, y)
	require.ErrorIs(t, err, ErrLedgerIdentityMismatch)
	require.EqualValues(t, 1, e.LedgerMismatches())

	// Same fields, different token: also a mismatch (the token is part
	// of identity; the key carries only its hash).
	rowX2 := v3.LedgerRow_builder{Identity: ledgerIdentityToProto(grantsPageIdentity("github", "other"))}.Build()
	val, err = marshalRecord(rowX2)
	require.NoError(t, err)
	require.NoError(t, e.db.UnsafeForTesting().Set(encodeLedgerKey(x), val, pebble.Sync))
	_, err = e.GetLedgerRowRecord(ctx, x)
	require.ErrorIs(t, err, ErrLedgerIdentityMismatch)
	require.EqualValues(t, 2, e.LedgerMismatches())

	// The honest row is accepted.
	require.NoError(t, e.NewPageUnit().Commit(ctx, x, nil))
	_, err = e.GetLedgerRowRecord(ctx, x)
	require.NoError(t, err)
	require.EqualValues(t, 2, e.LedgerMismatches())
}

func TestLedgerScrubAtSealForSensitiveTokens(t *testing.T) {
	ctx := context.Background()
	tokens := []string{"", "https://x/?sig=SECRET1", "https://x/?sig=SECRET2"}

	commitPages := func(t *testing.T, e *Engine) {
		for i, tok := range tokens {
			next := ""
			if i+1 < len(tokens) {
				next = tokens[i+1]
			}
			u := e.NewPageUnit()
			require.NoError(t, u.StageResources(ledgerTestResource("user", fmt.Sprintf("u%d", i))))
			row := v3.LedgerRow_builder{
				NextPageToken: next,
				Children:      []*v3.LedgerActionIdentity{ledgerIdentityToProto(grantsPageIdentity("child", "child-"+tok))},
			}.Build()
			require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", tok), row))
		}
	}

	t.Run("unflagged keeps tokens verbatim", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		commitPages(t, e)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			require.False(t, r.GetScrubbed())
			if r.GetIdentity().GetPageToken() != "" {
				require.Contains(t, r.GetIdentity().GetPageToken(), "SECRET")
			}
			return true
		}))
	})

	t.Run("flagged scrubs before seal", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e.SetLedgerTokensSensitive(true)
		commitPages(t, e)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))

		n := 0
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			n++
			require.True(t, r.GetScrubbed())
			require.Empty(t, r.GetIdentity().GetPageToken())
			require.Len(t, r.GetIdentity().GetPageTokenHash(), ledgerTokenHashLen)
			require.Empty(t, r.GetNextPageToken())
			require.Len(t, r.GetNextPageTokenHash(), ledgerTokenHashLen)
			for _, c := range r.GetChildren() {
				require.Empty(t, c.GetPageToken())
				require.Len(t, c.GetPageTokenHash(), ledgerTokenHashLen)
			}
			return true
		}))
		require.Equal(t, len(tokens), n)

		// The identity compare survives the scrub (hash path): a
		// scrubbed row still resolves for its own identity and still
		// refuses another's.
		got, err := e.GetLedgerRowRecord(ctx, grantsPageIdentity("github", tokens[1]))
		require.NoError(t, err)
		require.Equal(t, ledgerTokenHash(tokens[2]), got.GetNextPageTokenHash())
		_, err = e.GetLedgerRowRecord(ctx, grantsPageIdentity("github", "not-a-real-token"))
		require.ErrorIs(t, err, pebble.ErrNotFound)

		// Idempotent: a second pass (the resumed-EndSync shape) changes nothing.
		require.NoError(t, e.ScrubLedgerTokens(ctx))
		cnt, err := e.LedgerRowCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, len(tokens), cnt)
	})

	// The finished verdict must never be durable over a verbatim
	// token: a failed scrub fails EndSync, the sync stays unfinished
	// with its tokens (nothing half-done landed), and the retried
	// EndSync finishes the scrub.
	t.Run("failed scrub fails the seal and retries clean", func(t *testing.T) {
		e, _ := newTestEngine(t)
		syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e.SetLedgerTokensSensitive(true)
		commitPages(t, e)

		boom := errors.New("injected scrub commit failure")
		e.db.SetRecordCommitTestHook(func() error { return boom })
		require.ErrorIs(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}), boom)
		e.db.SetRecordCommitTestHook(nil)

		sr, err := e.GetSyncRunRecord(ctx, syncID)
		require.NoError(t, err)
		require.Nil(t, sr.GetEndedAt(), "no finished verdict after a failed scrub")
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			require.False(t, r.GetScrubbed(), "a failed scrub batch lands nothing")
			return true
		}))

		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		sr, err = e.GetSyncRunRecord(ctx, syncID)
		require.NoError(t, err)
		require.NotNil(t, sr.GetEndedAt())
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			require.True(t, r.GetScrubbed())
			require.Empty(t, r.GetIdentity().GetPageToken())
			return true
		}))
	})
}

func TestLedgerWipedWithItsSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.NewPageUnit().Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.NoError(t, e.NewPageUnit().Commit(ctx, grantsPageIdentity("github", "p2"), nil))
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))

	cnt, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, cnt, "the sealed sync keeps its trace")

	// A new sync starts on an empty ledger (scopedRanges wipe).
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cnt, err = e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.Zero(t, cnt)

	require.NoError(t, e.NewPageUnit().Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.NoError(t, e.DropLedger(ctx))
	cnt, err = e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.Zero(t, cnt)
}

// The crash oracle: store == f(ledger). Over a crashable in-memory
// FS, commit many NoSync pages, cut crash images that keep varying
// fractions of unsynced data, reopen each, and check both directions
// for every page — row present ⇒ every record of the page present;
// any record of the page present ⇒ its row present. A page is never
// half there. (Which pages survive is the crash's business; the
// ledger enumerates exactly the survivors, which is what resume
// relies on.)
func TestPageUnitCrashImageStoreEqualsLedger(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	const pages, perPage = 120, 5

	fs := vfs.NewCrashableMem()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	e, err := Open(ctx, "ledger-crash-db", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err)
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	pageRT := func(p int) string { return fmt.Sprintf("t%03d", p) }
	pageID := func(p int) LedgerIdentity { return grantsPageIdentity("github", fmt.Sprintf("page-%03d", p)) }
	for p := 0; p < pages; p++ {
		u := e.NewPageUnit()
		for i := 0; i < perPage; i++ {
			require.NoError(t, u.StageResources(ledgerTestResource(pageRT(p), fmt.Sprintf("r%d", i))))
		}
		require.NoError(t, u.Commit(ctx, pageID(p), v3.LedgerRow_builder{NextPageToken: fmt.Sprintf("page-%03d", p+1)}.Build()))
	}
	// No clean close: the images below are what a crash leaves.

	// checkImage reopens a crash image and returns how many pages
	// survived, failing the test if any page is half there.
	checkImage := func(image *vfs.MemFS, label string) int {
		re, err := Open(ctx, "ledger-crash-db", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
		require.NoError(t, err, label)
		defer func() { require.NoError(t, re.Close(), label) }()

		survivors := 0
		for p := 0; p < pages; p++ {
			_, rowErr := re.GetLedgerRowRecord(ctx, pageID(p))
			rowPresent := rowErr == nil
			if !rowPresent {
				require.ErrorIs(t, rowErr, pebble.ErrNotFound, label)
			}
			present := 0
			for i := 0; i < perPage; i++ {
				_, err := re.GetResourceRecord(ctx, pageRT(p), fmt.Sprintf("r%d", i))
				switch {
				case err == nil:
					present++
				case errors.Is(err, pebble.ErrNotFound):
				default:
					require.NoError(t, err, label)
				}
			}
			if rowPresent {
				survivors++
				require.Equal(t, perPage, present, "%s: page %d has a row but %d/%d records", label, p, present, perPage)
			} else {
				require.Zero(t, present, "%s: page %d has %d records but no row", label, p, present)
			}
		}
		t.Logf("%s: %d/%d pages survived, every one whole", label, survivors, pages)
		return survivors
	}

	// Images with varying fractions of unsynced data kept. Which pages
	// survive is up to the crash (pebble's WAL writer even buffers the
	// tail block in-process, so a 100% clone can still lose the last
	// pages); the oracle is only that no page is torn.
	rng := rand.New(rand.NewPCG(2026, 902)) //nolint:gosec // deterministic crash-image sampling, not security
	for _, pct := range []int{0, 20, 50, 80, 100} {
		trials := 3
		if pct == 0 {
			trials = 1 // deterministic image
		}
		for trial := 0; trial < trials; trial++ {
			cfg := vfs.CrashCloneCfg{UnsyncedDataPercent: pct}
			if pct > 0 {
				cfg.RNG = rng
			}
			checkImage(fs.CrashClone(cfg), fmt.Sprintf("unsynced=%d%% trial=%d", pct, trial))
		}
	}

	// A WAL sync point hardens every earlier NoSync page (sequential
	// WAL) — the mechanism EndSync's pebble.Sync stamp relies on to
	// carry the pages (TestEndSyncStampDurabilityCarriesPages). After
	// it, even the strictest image holds every page.
	require.NoError(t, e.db.WALSyncPoint())
	require.Equal(t, pages, checkImage(fs.CrashClone(vfs.CrashCloneCfg{}), "after WAL sync point, unsynced=0%"),
		"a sync point must make every earlier page durable")
	require.NoError(t, e.Close())
}

// withTokenOnlySDK shrinks the supported keyspace set to what an SDK that
// resumes from the checkpoint token alone accepts (v2), for the duration
// of fn. Not parallel-safe; the ledger tests do not run in parallel.
func withTokenOnlySDK(fn func()) {
	saved := supportedKeyspaceVersions
	supportedKeyspaceVersions = map[uint32]bool{keyspaceVersion: true}
	defer func() { supportedKeyspaceVersions = saved }()
	fn()
}

// The in-flight stamp (keyspaceVersionLedgerInFlight): a token-only SDK
// must refuse a file whose in-flight truth is in the ledger, and must
// accept the same file once sealed. Every crash image that holds a row
// holds the stamp.
func TestLedgerInFlightStampGatesTokenOnlyReaders(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	fs := vfs.NewCrashableMem()
	e, err := Open(ctx, "ledger-stamp-db", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	stamp := func(e *Engine) uint32 {
		v, err := e.keyspaceVersionStamp()
		require.NoError(t, err)
		return v
	}
	require.Equal(t, keyspaceVersion, stamp(e), "a sync with no rows yet is a plain v2 file")

	// Token-only SDK on the pre-row image: accepted (nothing to miss).
	withTokenOnlySDK(func() {
		pre := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 100, RNG: rand.New(rand.NewPCG(1, 1))}) //nolint:gosec // deterministic
		old, err := Open(ctx, "ledger-stamp-db", WithVFS(pre), WithReadOnly(true))
		require.NoError(t, err, "token-only SDK must open a ledgered sync that has not committed a row yet")
		require.NoError(t, old.Close())
	})

	u := e.NewPageUnit()
	require.NoError(t, u.StageResources(ledgerTestResource("t", "r1")))
	require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.Equal(t, keyspaceVersionLedgerInFlight, stamp(e), "first row flips the stamp")

	// Every image: stamp present whenever the row is (stamp is synced
	// before the row's NoSync batch, so no torn prefix has row-without-stamp).
	rng := rand.New(rand.NewPCG(7, 7)) //nolint:gosec // deterministic
	for pct := 0; pct <= 100; pct += 25 {
		img := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: pct, RNG: rng})
		re, err := Open(ctx, "ledger-stamp-db", WithVFS(img), WithReadOnly(true))
		require.NoError(t, err, "current SDK opens every image")
		n, err := re.LedgerRowCount(ctx)
		require.NoError(t, err)
		if n > 0 {
			require.Equal(t, keyspaceVersionLedgerInFlight, stamp(re), "unsynced=%d%%: a row without the in-flight stamp", pct)
			withTokenOnlySDK(func() {
				_, err := Open(ctx, "ledger-stamp-db", WithVFS(img), WithReadOnly(true))
				require.Error(t, err, "unsynced=%d%%: token-only SDK must refuse an in-flight ledgered file", pct)
				require.Contains(t, err.Error(), "unsupported keyspace layout v3")
				require.Contains(t, err.Error(), "regenerate this c1z with a current SDK")
			})
		}
		require.NoError(t, re.Close())
	}

	// Seal restores v2; the rows stay; a token-only SDK opens the sealed file.
	require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
	require.Equal(t, keyspaceVersion, stamp(e), "seal restores the v2 stamp")
	n, err := e.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, n, "seal keeps the rows")
	sealed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 100, RNG: rng})
	withTokenOnlySDK(func() {
		old, err := Open(ctx, "ledger-stamp-db", WithVFS(sealed), WithReadOnly(true))
		require.NoError(t, err, "token-only SDK must open a sealed ledgered file")
		require.NoError(t, old.Close())
	})

	// Reopen the live file: the mirror flag is derived from the stamp, so a
	// new sync's first row flips it again.
	require.NoError(t, e.Close())
	e, err = Open(ctx, "ledger-stamp-db", WithVFS(fs), withPanicOnFatalLogger())
	require.NoError(t, err)
	defer func() { _ = e.Close() }()
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.Equal(t, keyspaceVersion, stamp(e))
	require.NoError(t, e.NewPageUnit().Commit(ctx, grantsPageIdentity("github", "p1"), nil))
	require.Equal(t, keyspaceVersionLedgerInFlight, stamp(e))
}

// checkpointNeedleHits is the byte-level oracle for what a saved c1z
// ships: it takes a pebble checkpoint (the exact file set save's
// CheckpointTo hard-links into the envelope) and counts the internal
// KVs — EVERY version in every SST, shadowed ones included — whose key
// or value contains needle, plus raw byte hits in any WAL segment. A
// read through the engine cannot see this: pebble's iterators collapse
// to the newest version, so a scrubbed row reads clean while the
// pre-scrub bytes sit in an older SST.
func checkpointNeedleHits(t *testing.T, e *Engine, needle []byte) int {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "ckpt")
	require.NoError(t, e.CheckpointTo(ctx, dir))

	hits := 0
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, ent := range entries {
		path := filepath.Join(dir, ent.Name())
		switch filepath.Ext(ent.Name()) {
		case ".sst":
			f, err := vfs.Default.Open(path)
			require.NoError(t, err)
			readable, err := sstable.NewSimpleReadable(f)
			require.NoError(t, err)
			r, err := sstable.NewReader(ctx, readable, sstable.ReaderOptions{Comparer: pebble.DefaultComparer})
			require.NoError(t, err)
			it, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.TableBlobContext{})
			require.NoError(t, err)
			for kv := it.First(); kv != nil; kv = it.Next() {
				val, _, err := kv.Value(nil)
				require.NoError(t, err)
				if bytes.Contains(kv.K.UserKey, needle) || bytes.Contains(val, needle) {
					hits++
				}
			}
			require.NoError(t, it.Close())
			require.NoError(t, r.Close())
		case ".log":
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			hits += bytes.Count(raw, needle)
		}
	}
	return hits
}

// The flagged-connector scrub must hold at the byte level of the saved
// artifact, not just at the query level. Pebble never overwrites in
// place: without a compaction of the ledger range after the scrub, the
// pre-scrub rows (verbatim tokens) remain in the SSTs the checkpoint
// hard-links, query-invisible and `strings`-visible. Three arms: the
// unflagged control proves the oracle sees tokens that ARE shipped; the
// flagged arm proves zero residue; the no-compaction mutant proves the
// compaction, not the scrub, is what removes the bytes.
func TestLedgerScrubLeavesNoSSTResidue(t *testing.T) {
	ctx := context.Background()
	const needleText = "sig=SECRET-RESIDUE"
	tokens := []string{"", "https://x/?" + needleText + "-1", "https://x/?" + needleText + "-2"}

	commitPages := func(t *testing.T, e *Engine) {
		for i, tok := range tokens {
			next := ""
			if i+1 < len(tokens) {
				next = tokens[i+1]
			}
			u := e.NewPageUnit()
			require.NoError(t, u.StageResources(ledgerTestResource("user", fmt.Sprintf("u%d", i))))
			row := v3.LedgerRow_builder{
				NextPageToken: next,
				Children:      []*v3.LedgerActionIdentity{ledgerIdentityToProto(grantsPageIdentity("child", "child-"+tok))},
			}.Build()
			require.NoError(t, u.Commit(ctx, grantsPageIdentity("github", tok), row))
		}
		// Put the verbatim rows into an SST before the seal so the
		// residue, if any, is in a file the scrub cannot touch — the
		// production shape (pages flush to L0 throughout a sync).
		require.NoError(t, e.Flush(ctx))
	}

	t.Run("unflagged control: tokens ship", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		commitPages(t, e)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		require.Positive(t, checkpointNeedleHits(t, e, []byte(needleText)), "oracle must see verbatim tokens in an unflagged file")
	})

	t.Run("flagged: zero residue", func(t *testing.T) {
		e, _ := newTestEngine(t)
		_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e.SetLedgerTokensSensitive(true)
		commitPages(t, e)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		require.Zero(t, checkpointNeedleHits(t, e, []byte(needleText)), "verbatim token bytes survive in the checkpointed SSTs")
		// And the rows are still there, scrubbed.
		n, err := e.LedgerRowCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, len(tokens), n)
	})

	t.Run("mutant: scrub without residue purge leaks", func(t *testing.T) {
		e, _ := newTestEngine(t)
		e.test.skipLedgerResiduePurge = true
		_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e.SetLedgerTokensSensitive(true)
		commitPages(t, e)
		require.NoError(t, e.EndSyncWithStats(ctx, c1zstore.SyncStats{}))
		// Query level: clean.
		require.NoError(t, e.IterateLedger(ctx, func(r *v3.LedgerRow) bool {
			require.True(t, r.GetScrubbed())
			require.Empty(t, r.GetNextPageToken())
			return true
		}))
		// Byte level: leaks. This is the pre-fix behavior.
		require.Positive(t, checkpointNeedleHits(t, e, []byte(needleText)), "without the purge the scrub must leave residue, or this test proves nothing")
	})
}
