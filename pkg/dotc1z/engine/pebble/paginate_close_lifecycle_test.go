package pebble

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// callAfterClose runs fn and converts a panic into an error.
//
// The methods under test are being checked precisely because they may
// dereference a handle Close nil'd. Left unrecovered, the first one to
// do so takes the package's test binary down and every other verdict in
// this file is lost with it — the run reports one panic instead of the
// list of entry points that need the guard.
func callAfterClose(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked instead of returning an error: %v", r)
		}
	}()
	return fn()
}

// TestReadSurfaceAfterCloseReturnsClosing pins the lifecycle contract on
// the paginate and iterate surfaces.
//
// The Engine doc states it outright: "After Close, all methods return
// ErrEngineClosing." Before pinRead these families reached
// rawdb.DB.NewIter on a nil receiver instead — the paginate methods had
// no guard, and the Iterate family had neither a guard nor the nil check
// the invariant-scan surface carried
// (TestIngestScanSurfaceAfterCloseReturnsClosing covers that one).
//
// Each entry point is its own subtest so one run names every method that
// regressed rather than stopping at the first.
func TestReadSurfaceAfterCloseReturnsClosing(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()
	require.NoError(t, a.PutGrants(ctx, mkV2Grant("", "ent-A", "user", "alice")))
	require.NoError(t, e.Close())

	entID := entitlementIdentityFromParts("app", "github", canonicalTestEntID("ent-A"))

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"PaginateGrants", func() error {
			_, _, err := e.PaginateGrants(ctx, "", 10)
			return err
		}},
		{"PaginateGrantsByEntitlement", func() error {
			_, _, err := e.PaginateGrantsByEntitlement(ctx, entID, "", 10)
			return err
		}},
		{"PaginateGrantPrincipalKeysByEntitlement", func() error {
			_, _, err := e.PaginateGrantPrincipalKeysByEntitlement(ctx, entID, "", 10)
			return err
		}},
		{"PaginateGrantsByEntitlementPrincipal", func() error {
			_, _, err := e.PaginateGrantsByEntitlementPrincipal(ctx, entID, "user", "alice", "", 10)
			return err
		}},
		{"PaginateGrantsByPrincipal", func() error {
			_, _, err := e.PaginateGrantsByPrincipal(ctx, "user", "alice", "", 10)
			return err
		}},
		{"PaginateGrantsByEntitlementResource", func() error {
			_, _, err := e.PaginateGrantsByEntitlementResource(ctx, "app", "github", "", 10)
			return err
		}},
		{"PaginateGrantsByPrincipalResourceType", func() error {
			_, _, err := e.PaginateGrantsByPrincipalResourceType(ctx, "user", "", 10)
			return err
		}},
		{"PaginateGrantsByNeedsExpansion", func() error {
			_, _, err := e.PaginateGrantsByNeedsExpansion(ctx, "", 10)
			return err
		}},
		{"PaginateResources", func() error {
			_, _, err := e.PaginateResources(ctx, "", 10)
			return err
		}},
		{"PaginateResourcesByParent", func() error {
			_, _, err := e.PaginateResourcesByParent(ctx, "app", "github", "", 10)
			return err
		}},
		{"PaginateResourceTypes", func() error {
			_, _, err := e.PaginateResourceTypes(ctx, "", 10)
			return err
		}},
		{"PaginateEntitlements", func() error {
			_, _, err := e.PaginateEntitlements(ctx, "", 10)
			return err
		}},
		{"PaginateEntitlementsByResource", func() error {
			_, _, err := e.PaginateEntitlementsByResource(ctx, "app", "github", "", 10)
			return err
		}},
		{"IterateGrants", func() error {
			return e.IterateGrants(ctx, func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateGrantsByEntitlement", func() error {
			return e.IterateGrantsByEntitlement(ctx, canonicalTestEntID("ent-A"), func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateGrantsByPrincipal", func() error {
			return e.IterateGrantsByPrincipal(ctx, "user", "alice", func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateGrantsByPrincipalResourceType", func() error {
			return e.IterateGrantsByPrincipalResourceType(ctx, "user", func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateGrantsByNeedsExpansion", func() error {
			return e.IterateGrantsByNeedsExpansion(ctx, func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateGrantsByEntitlementBucket", func() error {
			return e.IterateGrantsByEntitlementBucket(ctx, entID, DigestBucket{}, func(*v3.GrantRecord) bool { return true })
		}},
		{"IterateResources", func() error {
			return e.IterateResources(ctx, func(*v3.ResourceRecord) bool { return true })
		}},
		{"IterateResourcesByParent", func() error {
			return e.IterateResourcesByParent(ctx, "app", "github", func(*v3.ResourceRecord) bool { return true })
		}},
		{"IterateResourceTypes", func() error {
			return e.IterateResourceTypes(ctx, func(*v3.ResourceTypeRecord) bool { return true })
		}},
		{"IterateEntitlements", func() error {
			return e.IterateEntitlements(ctx, func(*v3.EntitlementRecord) bool { return true })
		}},
		{"IterateEntitlementsByResource", func() error {
			return e.IterateEntitlementsByResource(ctx, "app", "github", func(*v3.EntitlementRecord) bool { return true })
		}},
		{"IterateAssets", func() error {
			return e.IterateAssets(ctx, func(*v3.AssetRecord) bool { return true })
		}},
		{"IterateAllSyncRuns", func() error {
			return e.IterateAllSyncRuns(ctx, func(*v3.SyncRunRecord) bool { return true })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, callAfterClose(tc.call), ErrEngineClosing)
		})
	}
}

// pinnedReadPrefixes are the exported read families that must go
// through pinRead. Every method whose name starts with one of these is
// a self-contained scan: it opens an iterator, drains it, and closes it
// before returning, so a pin scoped to the call covers the whole read.
//
// Surfaces that hand a live handle back to the caller — merge_surface's
// NewIter and Get return an iterator and a closer the caller uses after
// the call returns — are deliberately absent. A call-scoped pin would
// release while the caller still holds the handle, so they need the
// release tied to the returned object instead, and listing them here
// would let a useless pin satisfy the check.
var pinnedReadPrefixes = []string{"Paginate", "Iterate", "ForEach"}

// TestCloseWaitsForInFlightAdmission pins the ordering that keeps a
// joiner's closing check and its Add from straddling Close's flip.
//
// Re-checking the flag after the Add does not close that window, it just
// narrows it: a reader that read closing==false can be descheduled and
// run its Add after Close has parked in Wait with the counter at zero,
// and sync.WaitGroup answers that with "Add called concurrently with
// Wait" — a panic, not the ErrEngineClosing the re-check was reaching
// for. It surfaces as a rare crash under load, on the interleaving
// TestConcurrentCloseWithPaginatedReads spends four goroutines trying to
// hit, which is the worst way to learn about it.
//
// Holding the admission lock the way pinRead does is the whole
// interleaving, stopped in the middle: while a pin is being acquired,
// Close must not have flipped the flag yet.
func TestCloseWaitsForInFlightAdmission(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// A pin caught between its closing check and its Add.
	e.admitMu.RLock()

	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()

	require.Never(t, func() bool { return e.closing.Load() }, 200*time.Millisecond, 5*time.Millisecond,
		"Close flipped the closing flag while a pin was mid-acquisition. That pin's Add now lands on a "+
			"WaitGroup Close is already waiting on at zero, which panics instead of refusing the read")

	// Finish acquiring, then let Close proceed: it must now see the pin
	// and wait for it rather than tear the handle down underneath it.
	e.enterReadWG()
	e.admitMu.RUnlock()

	require.Never(t, func() bool { return e.db == nil }, 100*time.Millisecond, 5*time.Millisecond,
		"Close tore the handle down while a read was pinned")
	e.exitReadWG()
	require.NoError(t, <-closed)
}

// TestScanReadsArePinned keeps the read surface on the pin.
//
// pinRead is what orders a read's view of the handle against Close's
// teardown and what makes Close wait for the read to finish. A method
// that reaches for e.db directly is back to borrowing a handle the
// teardown can pull out from under it, and it fails as a panic from
// inside pebble rather than as an error.
//
// The check is mechanical because the mistake is invisible at the call
// site: a direct field read compiles, passes every functional test, and
// only surfaces when a Close lands mid-scan.
func TestScanReadsArePinned(t *testing.T) {
	_, files := parseProductionDir(t, ".")

	pinnedFamily := func(name string) bool {
		for _, prefix := range pinnedReadPrefixes {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}
		return false
	}

	// Scoped to methods on *Engine. The same names exist as free
	// functions in merge_accessor.go (ForEachGrantIndexKey and friends),
	// which encode index keys from a record and never touch the handle —
	// there is nothing there to pin.
	onEngine := func(fn *ast.FuncDecl) bool {
		if fn.Recv == nil || len(fn.Recv.List) != 1 {
			return false
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			return false
		}
		ident, ok := star.X.(*ast.Ident)
		return ok && ident.Name == "Engine"
	}

	checked := 0
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !onEngine(fn) || !pinnedFamily(fn.Name.Name) {
				continue
			}
			checked++
			t.Run(fn.Name.Name, func(t *testing.T) {
				var pinned, direct bool
				ast.Inspect(fn, func(n ast.Node) bool {
					sel, ok := n.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch sel.Sel.Name {
					case "pinRead":
						pinned = true
					case "db":
						if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "e" {
							direct = true
						}
					}
					return true
				})
				require.True(t, pinned,
					"%s reads without pinning the handle: call e.pinRead, defer its release, and read through the handle it returns", fn.Name.Name)
				require.False(t, direct,
					"%s reads e.db directly. The pinned handle is the one Close's drain accounts for; re-reading the "+
						"field inside the body is the unordered access pinRead exists to remove", fn.Name.Name)

				hasLoop, loopChecksCtx := scanLoopCancellation(fn)
				if hasLoop {
					require.True(t, loopChecksCtx,
						"%s scans without checking ctx.Err() in the loop. The pin makes Close wait for this scan, so a "+
							"full-keyspace read — or a yield callback that blocks on IO — holds the teardown open for as "+
							"long as it takes, with no way for the caller to call it off", fn.Name.Name)
				}
			})
		}
	}
	require.Positive(t, checked, "no read methods matched %v: this check has drifted off the surface it is meant to hold", pinnedReadPrefixes)
}

// scanLoopCancellation reports whether fn drives a pebble iterator
// directly, and whether any such loop consults ctx.Err().
//
// Keyed on an iterator loop — a `for` whose condition calls Valid() —
// rather than on any loop at all, because the unbounded thing is the
// keyspace, not the ranging. PaginateGrants ranges over the page it just
// read to reconcile absent fields; that loop is bounded by the page limit
// and the scan feeding it lives in a shared helper that checks per
// iteration. Requiring a check there would be noise, and noise is how a
// meta-test gets an exemption list and stops meaning anything.
//
// Deliberately "any iterator loop" rather than "every" one: these shapes
// nest, an outer index walk feeding an inner primary-key fetch, and one
// check per scan is what bounds the pin.
func scanLoopCancellation(fn *ast.FuncDecl) (bool, bool) {
	var hasLoop, checksCtx bool
	callsMethod := func(node ast.Node, name string) bool {
		if node == nil {
			return false
		}
		found := false
		ast.Inspect(node, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
				found = true
			}
			return true
		})
		return found
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		loop, ok := n.(*ast.ForStmt)
		if !ok || !callsMethod(loop.Cond, "Valid") {
			return true
		}
		hasLoop = true
		if callsMethod(loop.Body, "Err") {
			checksCtx = true
		}
		return true
	})
	return hasLoop, checksCtx
}

// TestConcurrentCloseWithPaginatedReads is the concurrent half of the
// same contract: "Concurrent Reader/Writer calls are safe."
//
// Writers are held to it by a real barrier — the closing flag, writeWG
// participation, and the re-check after Add — so Close drains them and
// they never touch a handle that is being torn down. Readers take part
// in none of that: there is no withRead, and the paginate family reads
// e.db directly. So a read in flight when Close nils the handle is
// unsynchronized against that write, and the value it read is one the
// teardown is about to invalidate.
//
// A reader here may only succeed or be refused with ErrEngineClosing.
// A panic, any other error, or a race report is a failure.
func TestConcurrentCloseWithPaginatedReads(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	const (
		readers       = 4
		grantsPerUser = 256
	)
	principals := make([]string, 0, readers)
	grants := make([]*v2.Grant, 0, readers*grantsPerUser)
	for w := range readers {
		principal := fmt.Sprintf("user%02d", w)
		principals = append(principals, principal)
		for i := range grantsPerUser {
			grants = append(grants, mkV2Grant("", fmt.Sprintf("ent-%02d-%05d", w, i), "user", principal))
		}
	}
	require.NoError(t, a.PutGrants(ctx, grants...))

	var (
		wg         sync.WaitGroup
		failuresMu sync.Mutex
		failures   []string
		pages      atomic.Int64
		refusals   atomic.Int64
	)
	failf := func(format string, args ...any) {
		failuresMu.Lock()
		defer failuresMu.Unlock()
		failures = append(failures, fmt.Sprintf(format, args...))
	}

	// Readers exit on a deadline as well as on the close boundary. One
	// that only stopped when it observed Close would hang forever
	// against a fix that drains readers before tearing the handle down,
	// turning this probe into a deadlock trap for the very change it is
	// asking for.
	deadline := time.Now().Add(5 * time.Second)

	// Both read shapes are hammered: the cursor-paged one and the
	// callback scan. They pin identically, but the scan holds its
	// iterator across the whole keyspace rather than one page, so it is
	// the one with a real chance of still being inside pebble when the
	// teardown starts.
	for i, principal := range principals {
		scan := i%2 == 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					failf("reader %s panicked mid-read: %v", principal, r)
				}
			}()
			for time.Now().Before(deadline) {
				var err error
				if scan {
					err = e.IterateGrantsByPrincipal(ctx, "user", principal, func(*v3.GrantRecord) bool { return true })
				} else {
					_, _, err = e.PaginateGrantsByPrincipal(ctx, "user", principal, "", 32)
				}
				if err != nil {
					if !errors.Is(err, ErrEngineClosing) {
						failf("reader %s: read across Close returned %v, want ErrEngineClosing", principal, err)
					}
					refusals.Add(1)
					return
				}
				pages.Add(1)
			}
		}()
	}

	// Let the readers build pressure, then close under them.
	time.Sleep(50 * time.Millisecond)
	closeErr := e.Close()
	wg.Wait()

	if len(failures) > 0 {
		for _, f := range failures {
			t.Error(f)
		}
		t.FailNow()
	}
	require.NoError(t, closeErr, "Close failed with reads in flight")
	require.Positive(t, pages.Load(),
		"no reader completed a page before Close; the race window was never exercised")
	require.Positive(t, refusals.Load(),
		"no reader was refused at the close boundary; every reader exited on its deadline instead, so the window was never exercised")
}
