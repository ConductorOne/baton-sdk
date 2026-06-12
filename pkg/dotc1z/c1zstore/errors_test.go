package c1zstore

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errEngineNotFound = errors.New("engine not found")

func TestAdaptNotFound(t *testing.T) {
	if AdaptNotFound(nil) != nil {
		t.Error("AdaptNotFound(nil) should be nil")
	}

	err := AdaptNotFound(sql.ErrNoRows)
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("sql.ErrNoRows status code: got %v, want NotFound", got)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		t.Error("AdaptNotFound(sql.ErrNoRows) should preserve ErrNoRows via Unwrap")
	}

	err = AdaptNotFound(errEngineNotFound, errEngineNotFound)
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("extra sentinel status code: got %v, want NotFound", got)
	}
	if !errors.Is(err, errEngineNotFound) {
		t.Error("AdaptNotFound should preserve extra sentinel via Unwrap")
	}

	wrapped := AdaptNotFound(fmt.Errorf("lookup: %w", errEngineNotFound), errEngineNotFound)
	if got := status.Code(wrapped); got != codes.NotFound {
		t.Errorf("wrapped status code: got %v, want NotFound", got)
	}

	other := errors.New("boom")
	if got := AdaptNotFound(other); !errors.Is(got, other) || status.Code(got) != codes.Unknown {
		t.Errorf("non-not-found error: got %v, want passthrough", got)
	}
}
