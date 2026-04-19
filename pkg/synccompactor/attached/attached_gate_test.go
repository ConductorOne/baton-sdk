package attached

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// nonSQLiteStore is a minimal C1ZStore implementation that does NOT derive
// from *dotc1z.C1File. It exists only to verify the AsSQLiteStore gate in
// NewAttachedCompactor. Embedding dotc1z.C1ZStore satisfies the interface
// with nil-method stubs — sufficient because the test never calls anything
// on the store; it only passes through the type-assertion gate.
type nonSQLiteStore struct {
	dotc1z.C1ZStore
}

func TestNewAttachedCompactor_RejectsNonSQLiteBase(t *testing.T) {
	_, err := NewAttachedCompactor(&nonSQLiteStore{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SQLite-backed base store")
}

func TestNewAttachedCompactor_RejectsNonSQLiteApplied(t *testing.T) {
	// base is a real *C1File; applied is the fake.
	ctx := context.Background()
	baseDB := makeTestC1File(ctx, t)
	defer func() { _ = baseDB.Close(ctx) }()

	_, err := NewAttachedCompactor(baseDB, &nonSQLiteStore{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SQLite-backed applied store")
}

func TestNewAttachedCompactor_RejectsNilTypedC1File(t *testing.T) {
	// A typed-nil *C1File wrapped in the interface must not pass the gate.
	var typedNil *dotc1z.C1File
	_, err := NewAttachedCompactor(typedNil, typedNil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SQLite-backed base store")
}

// makeTestC1File is defined in the existing test file(s) but repeated here for
// isolation. Uses t.TempDir() so cleanup is automatic.
func makeTestC1File(ctx context.Context, t *testing.T) *dotc1z.C1File {
	t.Helper()
	path := t.TempDir() + "/gate-test.c1z"
	f, err := dotc1z.NewC1ZFile(ctx, path)
	require.NoError(t, err)
	return f
}
