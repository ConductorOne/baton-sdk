package c1zstore

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errEngineNotFound = errors.New("engine not found")

func TestAdaptNotFound(t *testing.T) {
	require.Nil(t, AdaptNotFound(nil))

	err := AdaptNotFound(sql.ErrNoRows)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorIs(t, err, sql.ErrNoRows)

	err = AdaptNotFound(errEngineNotFound, errEngineNotFound)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.ErrorIs(t, err, errEngineNotFound)

	wrapped := AdaptNotFound(fmt.Errorf("lookup: %w", errEngineNotFound), errEngineNotFound)
	require.Equal(t, codes.NotFound, status.Code(wrapped))

	other := errors.New("boom")
	got := AdaptNotFound(other)
	require.ErrorIs(t, got, other)
	require.Equal(t, codes.Unknown, status.Code(got))
}
