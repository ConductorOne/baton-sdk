package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestC1FileCloseSurvivesCanceledCtx exercises the finalize-detach path.
// A caller cancellation (Temporal activity deadline, pod drain) must NOT
// stop Close from checkpointing the WAL, writing the c1z, and producing
// a durable, readable artifact on disk.
func TestC1FileCloseSurvivesCanceledCtx(t *testing.T) {
	cases := []struct {
		name      string
		slug      string
		cancelCtx func(t *testing.T) context.Context
	}{
		{
			name: "ctx canceled before close",
			slug: "canceled",
			cancelCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
		},
		{
			name: "ctx deadline already exceeded",
			slug: "deadline",
			cancelCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-1*time.Second))
				t.Cleanup(cancel)
				return ctx
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testFilePath := filepath.Join(c1zTests.workingDir, "close-cancel-"+tc.slug+".c1z")

			openCtx := t.Context()
			f, err := NewC1ZFile(openCtx, testFilePath)
			require.NoError(t, err)

			_, err = f.StartNewSync(openCtx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)

			err = f.PutResourceTypes(openCtx, v2.ResourceType_builder{Id: testResourceType}.Build())
			require.NoError(t, err)

			err = f.EndSync(openCtx)
			require.NoError(t, err)

			// Now close with a cancelled context. Finalize must still
			// complete because the detached context inside Close has
			// its own bound.
			err = f.Close(tc.cancelCtx(t))
			require.NoError(t, err)

			// The c1z must exist on disk and be readable.
			info, err := os.Stat(testFilePath)
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(0))

			// Reopen and verify the resource type was persisted.
			f2, err := NewC1ZFile(openCtx, testFilePath)
			require.NoError(t, err)
			t.Cleanup(func() { _ = f2.Close(openCtx) })

			resp, err := f2.GetResourceType(openCtx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
				ResourceTypeId: testResourceType,
			}.Build())
			require.NoError(t, err)
			require.Equal(t, testResourceType, resp.GetResourceType().GetId())
		})
	}
}

// TestC1FileCloseReadOnlyIgnoresCancel ensures the cheap (no-finalize)
// branch of Close still respects the caller's context — there is no
// finalize work to protect, so detaching would only add span noise.
func TestC1FileCloseReadOnlyIgnoresCancel(t *testing.T) {
	openCtx := t.Context()
	testFilePath := filepath.Join(c1zTests.workingDir, "close-readonly.c1z")

	// Seed a c1z so we can reopen it read-only.
	f, err := NewC1ZFile(openCtx, testFilePath)
	require.NoError(t, err)
	_, err = f.StartNewSync(openCtx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.EndSync(openCtx))
	require.NoError(t, f.Close(openCtx))

	f2, err := NewC1ZFile(openCtx, testFilePath, WithReadOnly(true))
	require.NoError(t, err)

	cancelCtx, cancel := context.WithCancel(openCtx)
	cancel()
	require.NoError(t, f2.Close(cancelCtx))
}
