package dotc1z

import (
	"os"
	"strconv"
	"time"
)

// DefaultFinalizeTimeout bounds the detached context used for c1z
// finalization (WAL checkpoint, save-to-disk, upload). Large tenants take
// 5-15 min to upload a 4 GB compressed c1z; one hour gives plenty of
// headroom while still being a hard ceiling so a wedged upload cannot
// hold a worker indefinitely.
const DefaultFinalizeTimeout = 1 * time.Hour

// finalizeTimeout holds the resolved value. Set once at package init from
// BATON_C1Z_FINALIZE_TIMEOUT (seconds); falls back to DefaultFinalizeTimeout
// when unset or invalid.
var finalizeTimeout = parseFinalizeTimeout(os.Getenv("BATON_C1Z_FINALIZE_TIMEOUT"))

func parseFinalizeTimeout(v string) time.Duration {
	return parseTimeoutSeconds(v, DefaultFinalizeTimeout)
}

// FinalizeTimeout returns the bound for the detached context that wraps
// c1z finalize-and-upload tails.
func FinalizeTimeout() time.Duration {
	return finalizeTimeout
}

// DefaultBulkLoadIndexTimeout bounds the detached context for the bulk-load
// deferred-index rebuild at Close. Building several secondary indexes over a
// 50M+-row grants table is tens of minutes — much longer than the
// checkpoint+save FinalizeTimeout covers — so this gets its own generous
// ceiling. Six hours is a backstop against a wedged build, not an expected
// duration.
const DefaultBulkLoadIndexTimeout = 6 * time.Hour

// bulkLoadIndexTimeout is resolved once from BATON_C1Z_BULKLOAD_INDEX_TIMEOUT
// (seconds), falling back to the default when unset or invalid.
var bulkLoadIndexTimeout = parseTimeoutSeconds(
	os.Getenv("BATON_C1Z_BULKLOAD_INDEX_TIMEOUT"), DefaultBulkLoadIndexTimeout)

// parseTimeoutSeconds parses a whole-seconds duration string, returning def
// when the value is empty, non-numeric, or non-positive. Shared by the
// finalize and bulk-load-index timeout knobs.
func parseTimeoutSeconds(v string, def time.Duration) time.Duration {
	if v == "" {
		return def
	}
	secs, err := strconv.ParseInt(v, 10, 64)
	if err != nil || secs <= 0 {
		return def
	}
	return time.Duration(secs) * time.Second
}

// BulkLoadIndexTimeout returns the bound for the detached context that wraps
// the bulk-load deferred-index rebuild.
func BulkLoadIndexTimeout() time.Duration {
	return bulkLoadIndexTimeout
}
