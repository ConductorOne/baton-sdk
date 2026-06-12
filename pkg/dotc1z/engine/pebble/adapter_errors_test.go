package pebble

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

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
}
