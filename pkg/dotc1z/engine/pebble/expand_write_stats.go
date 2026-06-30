package pebble

import (
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// expandWriteStats accumulates a breakdown of the PutExpandedGrantRecords
// write path so a diagnostic run can attribute the expansion write cost to
// its parts: the per-record read-before-write Get, the proto marshal, the
// stale-index delete, the primary-batch commit, the index-batch commit, and
// the per-index-family key/byte volume.
//
// All fields are atomic so accumulation is safe regardless of how the engine
// serializes writes. It is allocated only when EnableExpandWriteStats is
// called; the hot path guards on a nil pointer, so a production run pays one
// nil check per record and nothing else.
type expandWriteStats struct {
	calls   atomic.Int64
	records atomic.Int64

	getNs       atomic.Int64
	getCount    atomic.Int64
	getNotFound atomic.Int64
	marshalNs   atomic.Int64
	deleteNs    atomic.Int64
	priCommitNs atomic.Int64
	idxCommitNs atomic.Int64

	// readGet* track the primary Get issued per by_entitlement index row when
	// the read path (projection-source scan + destination base stream) hydrates
	// full grant records from the index. This is the "materialization Get" the
	// streaming-merge design flags as the main remaining read cost — distinct
	// from the read-before-write get* fields above, which live on the write path.
	readGetNs       atomic.Int64
	readGetCount    atomic.Int64
	readGetNotFound atomic.Int64

	priKeyBytes atomic.Int64
	priValBytes atomic.Int64

	entKeys  atomic.Int64
	entBytes atomic.Int64

	entResKeys  atomic.Int64
	entResBytes atomic.Int64

	prinKeys  atomic.Int64
	prinBytes atomic.Int64

	prinRTKeys  atomic.Int64
	prinRTBytes atomic.Int64

	needsExpKeys  atomic.Int64
	needsExpBytes atomic.Int64
}

// ExpandWriteStats is a snapshot of the expansion write-path breakdown.
// Durations are wall time summed across every PutExpandedGrantRecords call;
// because commits run NoSync, the commit durations also absorb any write-stall
// back-pressure the LSM applies when L0/compaction falls behind — i.e. a large
// IdxCommit relative to PriCommit is the signature of out-of-order index
// writes overwhelming compaction.
type ExpandWriteStats struct {
	Calls   int64
	Records int64

	GetDur       time.Duration
	GetCount     int64
	GetNotFound  int64
	MarshalDur   time.Duration
	DeleteIdxDur time.Duration
	PriCommitDur time.Duration
	IdxCommitDur time.Duration

	// Read-path primary materialization Get (by_entitlement scans).
	ReadGetDur      time.Duration
	ReadGetCount    int64
	ReadGetNotFound int64

	PriKeyBytes int64
	PriValBytes int64

	ByEntitlementKeys  int64
	ByEntitlementBytes int64

	ByEntitlementResourceKeys  int64
	ByEntitlementResourceBytes int64

	ByPrincipalKeys  int64
	ByPrincipalBytes int64

	ByPrincipalResourceTypeKeys  int64
	ByPrincipalResourceTypeBytes int64

	NeedsExpansionKeys  int64
	NeedsExpansionBytes int64
}

// getGrantPrimaryTimed wraps the per-index-row primary Get used by the grant
// pagination read paths (PaginateGrantsBy*). When expansion write stats are
// enabled it records read-materialization Get timing/count; otherwise it is a
// straight pass-through to e.db.Get with identical (value, closer, error)
// semantics. Diagnostic only.
func (e *Engine) getGrantPrimaryTimed(key []byte) ([]byte, io.Closer, error) {
	stats := e.expandStats
	if stats == nil {
		return e.db.Get(key)
	}
	t0 := time.Now()
	val, closer, err := e.db.Get(key)
	stats.readGetNs.Add(int64(time.Since(t0)))
	stats.readGetCount.Add(1)
	if errors.Is(err, pebble.ErrNotFound) {
		stats.readGetNotFound.Add(1)
	}
	return val, closer, err
}

// EnableExpandWriteStats turns on PutExpandedGrantRecords instrumentation.
// Call before an expansion run; read the result with ExpandWriteStats.
// Diagnostic only — not used by production code paths.
func (e *Engine) EnableExpandWriteStats() {
	e.expandStats = &expandWriteStats{}
}

// ExpandWriteStats returns a snapshot of accumulated expansion write-path
// stats and whether instrumentation was enabled.
func (e *Engine) ExpandWriteStats() (ExpandWriteStats, bool) {
	s := e.expandStats
	if s == nil {
		return ExpandWriteStats{}, false
	}
	return ExpandWriteStats{
		Calls:                        s.calls.Load(),
		Records:                      s.records.Load(),
		GetDur:                       time.Duration(s.getNs.Load()),
		GetCount:                     s.getCount.Load(),
		GetNotFound:                  s.getNotFound.Load(),
		MarshalDur:                   time.Duration(s.marshalNs.Load()),
		DeleteIdxDur:                 time.Duration(s.deleteNs.Load()),
		PriCommitDur:                 time.Duration(s.priCommitNs.Load()),
		IdxCommitDur:                 time.Duration(s.idxCommitNs.Load()),
		ReadGetDur:                   time.Duration(s.readGetNs.Load()),
		ReadGetCount:                 s.readGetCount.Load(),
		ReadGetNotFound:              s.readGetNotFound.Load(),
		PriKeyBytes:                  s.priKeyBytes.Load(),
		PriValBytes:                  s.priValBytes.Load(),
		ByEntitlementKeys:            s.entKeys.Load(),
		ByEntitlementBytes:           s.entBytes.Load(),
		ByEntitlementResourceKeys:    s.entResKeys.Load(),
		ByEntitlementResourceBytes:   s.entResBytes.Load(),
		ByPrincipalKeys:              s.prinKeys.Load(),
		ByPrincipalBytes:             s.prinBytes.Load(),
		ByPrincipalResourceTypeKeys:  s.prinRTKeys.Load(),
		ByPrincipalResourceTypeBytes: s.prinRTBytes.Load(),
		NeedsExpansionKeys:           s.needsExpKeys.Load(),
		NeedsExpansionBytes:          s.needsExpBytes.Load(),
	}, true
}
