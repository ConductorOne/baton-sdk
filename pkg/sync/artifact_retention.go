package sync

import (
	"errors"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// ErrArtifactUnusable is the storage verdict — the local c1z may not reflect
// a clean commit of this run's progress — and the only signal that permits a
// runner to discard a partial sync artifact (RFC 0009). It reaches a runner
// through Close() alone; Sync() errors and connector errors never carry it.
// Defined in pkg/dotc1z (storage owns the verdict) and re-exported here so
// runners depend on pkg/sync alone. Test with errors.Is.
var ErrArtifactUnusable = dotc1z.ErrArtifactUnusable

// ShouldDiscardSyncArtifact reports whether err carries the storage verdict.
// Everything else — cancellation, timeouts, all connector-side failures —
// preserves the artifact (RFC 0009 invariants I1/I2). Pass the join of the
// Sync() and Close() errors; today the verdict comes from Close() alone.
func ShouldDiscardSyncArtifact(err error) bool {
	return errors.Is(err, ErrArtifactUnusable)
}
