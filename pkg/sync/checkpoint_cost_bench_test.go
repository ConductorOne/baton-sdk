package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Cost contract for the per-checkpoint loop under spawned-cursor fan-out
// (docs/BUG_CATCHING.md §2 "cost contracts": the per-checkpoint loop is a
// declared hot path — performance there is a correctness property, and the
// reviewable object is the cost CURVE, stated and benchmarked, not "fine at
// fixture scale").
//
// What this branch added to that loop, and the curves these benchmarks pin:
//
//   - Every in-flight spawned cursor is an Action in state.actions, and
//     Marshal re-serializes the whole map on EVERY checkpoint (default
//     every ~10s for the sync's lifetime). Cost is O(width) in time and
//     token bytes, where width is bounded only by maxSpawnedCursorsPerBatch
//     (100k) — and each cursor's page token may itself be up to ~1 MiB, so
//     the constant is connector-controlled. The benchmark reports
//     token-bytes alongside ns/op so a regression in either is visible.
//   - Marshal also version-stamps by scanning every action for
//     type-scoped/spawned markers: a second O(width) pass that exists only
//     on this branch. It is measured by the same benchmark.
//   - Each admission permanently retains a spawnedAdmitted entry (32-byte
//     identity digest + action-ID string) for the process lifetime BY
//     DESIGN (re-mention termination guard; see state.go). Admission cost,
//     including that never-pruned entry, is pinned per-op below.
//
// Ratchet these against main when touching the checkpoint loop, state
// serialization, or the scheduler's admission path:
//
//	go test ./pkg/sync/ -run '^$' -bench BenchmarkCheckpoint -benchmem

import (
	"context"
	"fmt"
	"testing"
)

// buildFanoutState models the checkpoint-visible state of a sync holding
// `width` in-flight spawned type-scoped cursors (the worst case the batch
// cap admits), plus the parent action that spawned them.
func buildFanoutState(b *testing.B, width int) *state {
	b.Helper()
	ctx := context.Background()
	st := newState()
	st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", TypeScoped: true})
	for i := 0; i < width; i++ {
		st.pushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			PageToken:      fmt.Sprintf("spawned-cursor-page-token-%08d", i),
			Spawned:        true,
			TypeScoped:     true,
		})
	}
	return st
}

var checkpointFanoutWidths = []int{100, 1_000, 10_000, maxSpawnedCursorsPerBatch}

// BenchmarkCheckpointTokenMarshalFanout is the per-checkpoint serialization
// cost at fan-out width: O(width) time and token bytes, paid every ~10s.
func BenchmarkCheckpointTokenMarshalFanout(b *testing.B) {
	for _, width := range checkpointFanoutWidths {
		b.Run(fmt.Sprintf("width-%d", width), func(b *testing.B) {
			st := buildFanoutState(b, width)
			token, err := st.Marshal()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(token)), "token-bytes")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := st.Marshal(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCheckpointTokenUnmarshalFanout is the resume-side cost of the
// same token: parse plus the spawnedInFlight/spawnedAdmitted evidence
// rebuild, paid once per process start.
func BenchmarkCheckpointTokenUnmarshalFanout(b *testing.B) {
	for _, width := range checkpointFanoutWidths {
		b.Run(fmt.Sprintf("width-%d", width), func(b *testing.B) {
			token, err := buildFanoutState(b, width).Marshal()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := newState().Unmarshal(token); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSpawnedCursorAdmission is the steady-state cost of admitting and
// draining one spawned cursor, INCLUDING the process-lifetime spawnedAdmitted
// entry the drain deliberately does not free: B/op here is the memory the
// process permanently retains per admission (the map grows monotonically
// across the loop, so amortized growth is in the number).
func BenchmarkSpawnedCursorAdmission(b *testing.B) {
	ctx := context.Background()
	st := newState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		admitted := st.pushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			PageToken:      fmt.Sprintf("admission-cursor-%012d", i),
			Spawned:        true,
			TypeScoped:     true,
		})
		st.FinishAction(ctx, admitted)
	}
}
