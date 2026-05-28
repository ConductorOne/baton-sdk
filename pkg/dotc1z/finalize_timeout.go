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
	if v == "" {
		return DefaultFinalizeTimeout
	}
	secs, err := strconv.ParseInt(v, 10, 64)
	if err != nil || secs <= 0 {
		return DefaultFinalizeTimeout
	}
	return time.Duration(secs) * time.Second
}

// FinalizeTimeout returns the bound for the detached context that wraps
// c1z finalize-and-upload tails.
func FinalizeTimeout() time.Duration {
	return finalizeTimeout
}
