package sync //nolint:revive,nolintlint // existing package name preserved for backwards compatibility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithMinCheckpointInterval(t *testing.T) {
	t.Run("sets positive duration", func(t *testing.T) {
		s := &syncer{}
		WithMinCheckpointInterval(5 * time.Second)(s)
		require.Equal(t, 5*time.Second, s.minCheckpointInterval)
	})

	t.Run("zero is rejected (field stays at default-zero so Checkpoint falls back)", func(t *testing.T) {
		s := &syncer{minCheckpointInterval: 7 * time.Second}
		WithMinCheckpointInterval(0)(s)
		require.Equal(t, 7*time.Second, s.minCheckpointInterval,
			"zero must not overwrite a previously-set value")
	})

	t.Run("negative is rejected", func(t *testing.T) {
		s := &syncer{minCheckpointInterval: 7 * time.Second}
		WithMinCheckpointInterval(-1 * time.Second)(s)
		require.Equal(t, 7*time.Second, s.minCheckpointInterval,
			"negative must not overwrite a previously-set value")
	})

	t.Run("starting from zero stays zero for non-positive input", func(t *testing.T) {
		s := &syncer{}
		WithMinCheckpointInterval(0)(s)
		require.Equal(t, time.Duration(0), s.minCheckpointInterval)
		WithMinCheckpointInterval(-1)(s)
		require.Equal(t, time.Duration(0), s.minCheckpointInterval)
	})
}

// TestSyncerCheckpoint_IntervalGate exercises the interval gate on Checkpoint
// without touching state.Marshal or store.CheckpointSync. We force-set
// lastCheckPointTime so the gate is reached, then assert the early-return.
// A configured interval of 1 hour should short-circuit; the default (10s)
// should also short-circuit when lastCheckPointTime is "now".
func TestSyncerCheckpoint_IntervalGate(t *testing.T) {
	t.Run("configured interval gates non-forced checkpoint", func(t *testing.T) {
		s := &syncer{
			minCheckpointInterval: 1 * time.Hour,
			lastCheckPointTime:    time.Now(),
		}
		err := s.Checkpoint(t.Context(), false)
		require.NoError(t, err, "interval gate must short-circuit before reaching store")
	})

	t.Run("default interval gates non-forced checkpoint right after a previous one", func(t *testing.T) {
		s := &syncer{
			// minCheckpointInterval=0 → Checkpoint falls back to defaultMinCheckpointInterval (10s)
			lastCheckPointTime: time.Now(),
		}
		err := s.Checkpoint(t.Context(), false)
		require.NoError(t, err)
	})

	t.Run("zero lastCheckPointTime bypasses the gate", func(t *testing.T) {
		// lastCheckPointTime.IsZero() → gate does not apply → Checkpoint proceeds
		// to state.Marshal which will panic on a nil state. We use a deferred
		// recover to assert we got past the early-return.
		s := &syncer{
			minCheckpointInterval: 1 * time.Hour,
		}
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected Checkpoint to proceed past the gate (and panic on nil state)")
			}
		}()
		_ = s.Checkpoint(t.Context(), false)
	})
}
