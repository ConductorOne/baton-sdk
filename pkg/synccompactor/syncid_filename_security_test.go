package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

// Fold path: separator-bearing base SyncID escapes the compactor's private
// MkdirTemp dir AND the caller's WithTmpDir root; copyFileForFold O_TRUNC-writes
// the full hostile base .c1z at the escaped path BEFORE any other step, and the
// file persists after the documented cleanup() (which removes only c.tmpDir).
//
// Note the "compacted-" prefix is its own path element: the first "../" in the
// SyncID cancels it, so N "../" yield N-1 effective hops. Four "../" here:
// cancel prefix, cancel baton-N, cancel outer -> lands in outerParent.
func TestSecurityCompactSyncIDFilenameEscapeFold(t *testing.T) {
	ctx := context.Background()
	inputDir := t.TempDir()
	outerParent := t.TempDir()
	outer := filepath.Join(outerParent, "v7-outer")
	require.NoError(t, os.Mkdir(outer, 0o700))
	outputDir := t.TempDir()

	basePath := filepath.Join(inputDir, "base.c1z")
	w, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, w.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, w.PutResources(ctx, user))
	member := v2.Entitlement_builder{Id: "e1", Resource: user, Purpose: v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT}.Build()
	require.NoError(t, w.PutEntitlements(ctx, member))
	require.NoError(t, w.PutGrants(ctx, v2.Grant_builder{Id: "g1", Principal: user, Entitlement: member}.Build()))
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))

	hostileSyncID := "../../../../v7-verify-escape"
	base := &CompactableSync{FilePath: basePath, SyncID: hostileSyncID}

	badPath := filepath.Join(inputDir, "bad.c1z")
	require.NoError(t, os.WriteFile(badPath, append([]byte("C1Z3\x00"), []byte("garbage")...), 0o600))
	bad := &CompactableSync{FilePath: badPath, SyncID: "whatever"}

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")
	_, _, err = NewCompactor(ctx, outputDir, []*CompactableSync{base, bad},
		WithTmpDir(outer), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion())
	require.Error(t, err,
		"containment violation: NewCompactor accepted a separator-bearing SyncID that escapes the temp dir")
	require.Contains(t, err.Error(), "invalid sync id")

	// Before the fix, the fold path O_TRUNC-copied the hostile base .c1z to
	// the escaped path, where it outlived cleanup (which removes only
	// c.tmpDir).
	found := filepath.Join(outerParent, "v7-verify-escape.c1z")
	_, statErr := os.Stat(found)
	require.True(t, os.IsNotExist(statErr),
		"hostile SyncID wrote a file outside the WithTmpDir root")
}

// Non-fold (overlay) path: before the fix, dotc1z.NewStore created the
// working store at the escaped path. The hostile SyncID must be rejected at
// NewCompactor, with nothing created.
func TestSecurityCompactSyncIDFilenameEscapeOverlay(t *testing.T) {
	ctx := context.Background()
	inputDir := t.TempDir()
	outerParent := t.TempDir()
	outer := filepath.Join(outerParent, "v7-outer")
	require.NoError(t, os.Mkdir(outer, 0o700))
	outputDir := t.TempDir()

	basePath := filepath.Join(inputDir, "base2.c1z")
	w, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	_, err = w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, w.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	require.NoError(t, w.EndSync(ctx))
	require.NoError(t, w.Close(ctx))

	base := &CompactableSync{FilePath: basePath, SyncID: "../../../../v7-verify-escape-overlay"}
	badPath := filepath.Join(inputDir, "bad2.c1z")
	require.NoError(t, os.WriteFile(badPath, append([]byte("C1Z3\x00"), []byte("garbage")...), 0o600))
	bad := &CompactableSync{FilePath: badPath, SyncID: "whatever"}

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "overlay")
	_, _, err = NewCompactor(ctx, outputDir, []*CompactableSync{base, bad},
		WithTmpDir(outer), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion())
	require.Error(t, err,
		"containment violation: NewCompactor accepted a separator-bearing SyncID (overlay path)")
	require.Contains(t, err.Error(), "invalid sync id")

	found := filepath.Join(outerParent, "v7-verify-escape-overlay.c1z")
	_, statErr := os.Stat(found)
	require.True(t, os.IsNotExist(statErr),
		"working store created at escaped path via NewStore")
}

// Every hostile SyncID shape must be rejected at NewCompactor, before any
// working-artifact filename is built from it.
func TestSecurityCompactSyncIDRejectionCases(t *testing.T) {
	validKSUID := "2YpKj9CDvFqPHLhMZWB4h0X1YyL"
	cases := []struct {
		name    string
		syncID  string
		wantErr bool
	}{
		{"forward slash traversal", "compacted-../../pwned.c1z", true},
		{"backslash traversal", "..\\..\\pwned.c1z", true},
		{"drive-style prefix", "C:whatever", true},
		{"absolute unix path", "/x", true},
		{"absolute windows path", "\\x", true},
		{"empty sentinel accepted", "", false},
		{"valid ksuid accepted", validKSUID, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inputDir := t.TempDir()
			outputDir := t.TempDir()

			basePath := filepath.Join(inputDir, "base.c1z")
			w, err := dotc1z.NewStore(ctx, basePath, dotc1z.WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			_, err = w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, w.EndSync(ctx))
			require.NoError(t, w.Close(ctx))
			badPath := filepath.Join(inputDir, "bad.c1z")
			require.NoError(t, os.WriteFile(badPath, append([]byte("C1Z3\x00"), []byte("garbage")...), 0o600))

			entries := []*CompactableSync{
				{FilePath: basePath, SyncID: tc.syncID},
				{FilePath: badPath, SyncID: "whatever"},
			}
			_, cleanup, err := NewCompactor(ctx, outputDir, entries, WithTmpDir(t.TempDir()))
			if tc.wantErr {
				require.Error(t, err, "NewCompactor accepted hostile SyncID %q", tc.syncID)
				require.Contains(t, err.Error(), "invalid sync id")
				return
			}
			require.NoError(t, err, "NewCompactor rejected benign SyncID %q", tc.syncID)
			t.Cleanup(func() { _ = cleanup() })
		})
	}
}
