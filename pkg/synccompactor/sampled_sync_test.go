package synccompactor

import (
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestCompactorRejectsSampledInputs(t *testing.T) {
	for _, mode := range []PebbleCompactorMode{PebbleCompactorModeKWay, PebbleCompactorModeOverlay, PebbleCompactorModeFold} {
		for _, sampledInput := range []string{"base", "partial"} {
			t.Run(string(mode)+"/"+sampledInput, func(t *testing.T) {
				ctx, dir := t.Context(), t.TempDir()
				basePath, partialPath := filepath.Join(dir, "base.c1z"), filepath.Join(dir, "partial.c1z")
				baseCheckpoint, partialCheckpoint := "", ""
				const sampledCheckpoint = `{"counters":{"list_page_window.sampled":1}}`
				if sampledInput == "base" {
					baseCheckpoint = sampledCheckpoint
				} else {
					partialCheckpoint = sampledCheckpoint
				}
				baseID := buildPebbleInputWithQuality(t, ctx, basePath, connectorstore.SyncTypeFull, baseCheckpoint, "base")
				partialID := buildPebbleInputWithQuality(t, ctx, partialPath, connectorstore.SyncTypePartial, partialCheckpoint, "partial")
				compactor, cleanup, err := NewCompactor(ctx, t.TempDir(), []*CompactableSync{
					{FilePath: basePath, SyncID: baseID}, {FilePath: partialPath, SyncID: partialID},
				}, WithTmpDir(t.TempDir()), WithEngine(c1zstore.EnginePebble), WithSkipGrantExpansion(), WithPebbleCompactorMode(mode))
				require.NoError(t, err)
				defer func() { require.NoError(t, cleanup()) }()
				output, err := compactor.Compact(ctx)
				require.ErrorContains(t, err, "sampled")
				require.Nil(t, output)
			})
		}
	}
}
