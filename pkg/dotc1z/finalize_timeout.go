package dotc1z

import (
	"math"
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
// finalize, bulk-load-index, and sqlite-init timeout knobs. The seconds
// value is clamped before the multiply so a huge accepted int64 cannot
// overflow time.Duration to negative.
func parseTimeoutSeconds(v string, def time.Duration) time.Duration {
	if v == "" {
		return def
	}
	secs, err := strconv.ParseInt(v, 10, 64)
	if err != nil || secs <= 0 {
		return def
	}
	if secs > math.MaxInt64/int64(time.Second) {
		secs = math.MaxInt64 / int64(time.Second)
	}
	return time.Duration(secs) * time.Second
}

// DefaultSQLiteInitTimeout bounds the SQLite initialization phase (open,
// ping, schema guard, schema DDL + migrations, checkpoint, optimize, caller
// pragma setup) of NewC1File. One hour is the starting policy: it is a hard
// ceiling against a wedged init, not an expected duration. Raise via
// BATON_C1Z_SQLITE_INIT_TIMEOUT (positive seconds) if representative
// large-legacy-file verification shows it is too tight. The clone-path
// deferred-index rebuild keeps its own BulkLoadIndexTimeout budget.
const DefaultSQLiteInitTimeout = 1 * time.Hour

// sqliteInitTimeout holds the resolved value. Set once at package init
// from BATON_C1Z_SQLITE_INIT_TIMEOUT (seconds); falls back to
// DefaultSQLiteInitTimeout when unset or invalid.
var sqliteInitTimeout = parseTimeoutSeconds(
	os.Getenv("BATON_C1Z_SQLITE_INIT_TIMEOUT"), DefaultSQLiteInitTimeout)

// SQLiteInitTimeout returns the bound for the NewC1File init phase. The
// clone path overrides it with BulkLoadIndexTimeout via the unexported
// withInitBudget option.
func SQLiteInitTimeout() time.Duration {
	return sqliteInitTimeout
}

// BulkLoadIndexTimeout returns the bound for the detached context that wraps
// the bulk-load deferred-index rebuild.
func BulkLoadIndexTimeout() time.Duration {
	return bulkLoadIndexTimeout
}
