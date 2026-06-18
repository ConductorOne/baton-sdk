package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
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
		require.Equal(t, tc.code, status.Code(tc.err), "%v: status code", tc.err)
		require.ErrorIs(t, fmt.Errorf("wrap: %w", tc.err), tc.err, "%v: errors.Is identity through wrapping broken", tc.err)
	}
}

// End-to-end: a missing record surfaced through the reader API
// carries codes.NotFound.
func TestReaderNotFoundStatus(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	_, err = a.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: "does-not-exist",
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "GetGrant missing: status code")
}
