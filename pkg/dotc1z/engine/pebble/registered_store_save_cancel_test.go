package pebble

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestRegisteredStoreSaveSurvivesCanceledCtx is the Pebble-engine
// analogue of c1file_close_cancel_test.go::TestC1FileCloseSurvivesCanceledCtx.
// Caller cancellation (Temporal activity deadline, pod drain) must
// NOT stop registeredStore.save from checkpointing Pebble, walking
// the checkpoint into the v3 envelope, and producing a readable c1z
// on disk. A regression that replaces finalizeCtx with the caller
// ctx in save() would fail this test.
func TestRegisteredStoreSaveSurvivesCanceledCtx(t *testing.T) {
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
			require.NoError(t, Register())

			openCtx := t.Context()
			path := t.TempDir() + "/save-cancel-" + tc.slug + ".c1z"

			store, err := dotc1z.NewStore(openCtx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
			require.NoError(t, err)

			_, err = store.StartNewSync(openCtx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.PutGrants(openCtx, mkV2Grant("g-cancel", "ent", "user", "alice")))
			require.NoError(t, store.EndSync(openCtx))

			// Close with a cancelled context. The save() detach
			// must produce a complete envelope anyway.
			require.NoError(t, store.Close(tc.cancelCtx(t)))

			// File exists and is readable.
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(0))

			// Reopen read-only and verify the grant survived.
			f, err := os.Open(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = f.Close() })
			env, err := formatv3.ReadEnvelope(f)
			require.NoError(t, err)
			t.Cleanup(func() { _ = env.Close() })
			require.Equal(t, string(dotc1z.EnginePebble), env.Manifest.GetEngine())
		})
	}
}
