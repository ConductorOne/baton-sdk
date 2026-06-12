package pebble

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// adaptNotFound must produce an error that satisfies all three
// not-found contracts at once: the gRPC NotFound status code (for
// gRPC consumers), sql.ErrNoRows (the engine-agnostic sentinel
// pkg/sync relies on), and pebble.ErrNotFound (the engine-internal
// sentinel, preserved via Unwrap).
func TestAdaptNotFound(t *testing.T) {
	err := adaptNotFound(pebble.ErrNotFound)
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("status code: got %v, want NotFound", got)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("errors.Is(err, sql.ErrNoRows) = false, want true")
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Errorf("errors.Is(err, pebble.ErrNotFound) = false, want true")
	}

	// Wrapped not-found errors translate too.
	wrapped := adaptNotFound(fmt.Errorf("lookup: %w", pebble.ErrNotFound))
	if got := status.Code(wrapped); got != codes.NotFound {
		t.Errorf("wrapped status code: got %v, want NotFound", got)
	}

	// Other errors pass through unchanged.
	other := errors.New("boom")
	if got := adaptNotFound(other); !errors.Is(got, other) || status.Code(got) != codes.Unknown {
		t.Errorf("non-not-found error: got %v, want passthrough", got)
	}
	if adaptNotFound(nil) != nil {
		t.Error("adaptNotFound(nil) should be nil")
	}
}

// The sentinel errors carry gRPC status codes while keeping
// errors.Is identity comparisons working.
func TestSentinelErrorStatusCodes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{ErrNoCurrentSync, codes.FailedPrecondition},
		{ErrInvalidPageToken, codes.InvalidArgument},
		{ErrDiskFull, codes.ResourceExhausted},
		{ErrEngineClosing, codes.Unavailable},
		{ErrSaveDestExists, codes.AlreadyExists},
	} {
		if got := status.Code(tc.err); got != tc.code {
			t.Errorf("%v: status code got %v, want %v", tc.err, got, tc.code)
		}
		if !errors.Is(fmt.Errorf("wrap: %w", tc.err), tc.err) {
			t.Errorf("%v: errors.Is identity through wrapping broken", tc.err)
		}
	}
}

// End-to-end: a missing record surfaced through the reader API
// carries codes.NotFound.
func TestReaderNotFoundStatus(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatal(err)
	}

	_, err := a.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: "does-not-exist",
	}.Build())
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("GetGrant missing: status code got %v, want NotFound", got)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("GetGrant missing: errors.Is(err, sql.ErrNoRows) = false, want true")
	}
}
