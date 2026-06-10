package connectorstore

import (
	"context"
	"iter"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// StreamingReader is an optional capability for stores that can
// yield records via Go iterators (iter.Seq2 — Go 1.23+) without
// the page-by-page accumulation that the unary Reader RPCs
// require. RFC §B3.
//
// Why iter.Seq2 instead of gRPC server-streaming: every in-process
// consumer of connectorstore (the syncer, expander, c1z-as-file
// wrapper) already runs the store as a Go object, not over the
// wire. gRPC streaming for an in-process callee just routes
// through a fake-stream wrapper — and the codebase already punts
// on that path for GetAsset (see connectorstore.Reader). A
// future PR can layer a gRPC streaming handler over these
// iterators trivially; the bounded-memory work lives here.
//
// Iteration contract:
//   - The iterator yields (record, error) pairs. A non-nil error
//     terminates iteration; the caller must check it before using
//     the record.
//   - Caller can stop early by returning false from the range body.
//   - Context cancellation surfaces as the yielded error on the
//     next iteration boundary.
//   - syncID == "" uses the store's currently active sync (matches
//     unary RPC behavior).
type StreamingReader interface {
	StreamGrants(ctx context.Context, syncID string, opts StreamGrantsOptions) iter.Seq2[*v2.Grant, error]
	StreamResources(ctx context.Context, syncID string, opts StreamResourcesOptions) iter.Seq2[*v2.Resource, error]
	StreamEntitlements(ctx context.Context, syncID string) iter.Seq2[*v2.Entitlement, error]
}

// StreamGrantsOptions narrows the stream to a single entitlement
// and/or principal. Empty fields mean no filter.
//
// IncludeExpansion re-attaches each grant's GrantExpandable annotation when
// set. The SQLite engine strips GrantExpandable into a side column on write,
// so without this the SQLite stream yields grants with their expansion
// topology missing — a faithful round-trip (cross-engine copy, rollback /
// replay, or an external grant consumer) must set it. Default false preserves
// the existing data-only stream. The Pebble engine reconstructs the
// annotation from its record either way, so the option is a no-op there.
type StreamGrantsOptions struct {
	EntitlementID         string
	PrincipalResourceType string
	PrincipalResourceID   string
	IncludeExpansion      bool
}

// StreamResourcesOptions narrows the stream by resource_type.
type StreamResourcesOptions struct {
	ResourceTypeID string
}
