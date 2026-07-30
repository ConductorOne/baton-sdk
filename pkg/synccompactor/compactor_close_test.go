package synccompactor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJoinCompactorCloseError(t *testing.T) {
	operationErr := errors.New("operation failed")
	closeErr := errors.New("resource leak at close")

	err := joinCompactorCloseError(operationErr, closeErr, "candidate.c1z")
	require.ErrorIs(t, err, operationErr)
	require.ErrorIs(t, err, closeErr)
	require.ErrorContains(t, err, "candidate.c1z")
	require.Equal(t, operationErr, joinCompactorCloseError(operationErr, nil, "candidate.c1z"))
}
