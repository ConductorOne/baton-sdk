package c1zstore

import (
	"context"
	"errors"
	"fmt"
)

// The write seam (docs/tasks/sound-syncs-solutions-brief.md §3.10 fact 1):
// under atomic pages every store write a page makes goes through its
// PageWriter and lands in the page's unit. A direct store write from
// inside a page is a bypass — the write is durable on its own, ahead of
// the page's ledger row. The seam makes fact 1 checkable instead of
// audited: the syncer marks a page's context open, the store checks the
// mark on every direct write, and a test-time hook turns a bypass into a
// failure.
//
// Bypasses are not all bugs. A write that PRECEDES the row and is
// idempotent on re-run (an asset blob; a source-cache replay copy whose
// scope is cleared first) is safe: a row can never claim work that is
// not there. Such a site registers itself with WithPageWriteBypass and a
// reason, so the seam sees an accounted-for write rather than an
// unknown one. The unsafe direction — a write landing AFTER the row —
// is not reachable through this seam at all: the row commits in
// PageWriter.Commit, the last thing a page does.

type openPageKey struct{}
type pageBypassKey struct{}

// WithOpenPage marks ctx as running inside an atomic page. Direct store
// writes carrying this ctx are bypasses of the page's unit.
func WithOpenPage(ctx context.Context) context.Context {
	return context.WithValue(ctx, openPageKey{}, true)
}

// PageOpen reports whether ctx is inside an atomic page.
func PageOpen(ctx context.Context) bool {
	v, _ := ctx.Value(openPageKey{}).(bool)
	return v
}

// WithPageWriteBypass registers that direct writes carrying ctx are a
// known, reasoned bypass of the page's unit. reason is recorded on the
// seam event and should say why the order is safe.
func WithPageWriteBypass(ctx context.Context, reason string) context.Context {
	return context.WithValue(ctx, pageBypassKey{}, reason)
}

// PageWriteBypass returns the registered bypass reason for ctx, if any.
func PageWriteBypass(ctx context.Context) (string, bool) {
	r, ok := ctx.Value(pageBypassKey{}).(string)
	return r, ok && r != ""
}

// WriteSeamEvent is one direct store write observed while a page was
// open. Bypass is the registered reason, "" for an unregistered write.
type WriteSeamEvent struct {
	Method string
	Bypass string
}

// Registered reports whether the write carried a bypass registration.
func (e WriteSeamEvent) Registered() bool { return e.Bypass != "" }

// ErrUnregisteredPageWrite is returned by the strict seam for an
// unregistered direct write inside a page.
var ErrUnregisteredPageWrite = errors.New("atomic pages: direct store write inside an open page bypasses the page's unit")

// WriteSeamHook receives every direct write observed inside an open
// page. A non-nil error fails the write.
type WriteSeamHook func(ctx context.Context, ev WriteSeamEvent) error

// WriteSeamStore is implemented by stores that expose the seam.
type WriteSeamStore interface {
	// SetWriteSeam installs hook; nil removes it. With no hook installed
	// the seam costs one nil check per write.
	SetWriteSeam(hook WriteSeamHook)
}

// StrictWriteSeam is the test-time hook: unregistered writes fail with
// ErrUnregisteredPageWrite; registered bypasses pass and are recorded
// through record (may be nil).
func StrictWriteSeam(record func(WriteSeamEvent)) WriteSeamHook {
	return func(_ context.Context, ev WriteSeamEvent) error {
		if record != nil {
			record(ev)
		}
		if !ev.Registered() {
			return fmt.Errorf("%w: %s", ErrUnregisteredPageWrite, ev.Method)
		}
		return nil
	}
}
